package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

type Column struct {
	Name, Typ         string
	IsNull, IsPrimary bool
}

type Uploader interface {
	Upload() error
	Start()
	Stop()
}

type DBManager struct {
	db         *sql.DB
	dbDir      string
	table      string
	createdAT  time.Time
	cols       []Column
	uploadMngr Uploader
}

func NewDBManager(
	ctx context.Context,
	dbDir string,
	table string,
	cols []Column,
	uploadInterval time.Duration,
) (*DBManager, error) {
	dbPath := path.Join(dbDir, fmt.Sprintf("%d.db", time.Now().Unix()))
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	umngr := NewUploadManager(ctx, dbDir, table, uploadInterval)
	return &DBManager{
		db:         db,
		dbDir:      dbDir,
		table:      table,
		createdAT:  time.Now(),
		cols:       cols,
		uploadMngr: umngr,
	}, nil
}

func (dbm *DBManager) Close() {
	dbm.db.Close()
}

// queryFromWAL creates a query for a WAL TX records.
func (dbm *DBManager) queryFromWAL(tx *pgrepl.Tx) string {
	var queries []string
	for _, r := range tx.Records {
		columnNames := []string{}
		vals := []string{}
		for _, c := range r.Columns {
			columnNames = append(columnNames, c.Name)
			vals = append(vals, strings.ReplaceAll(string(c.Value), "\"", ""))
		}

		cols := strings.Join(columnNames, ", ")
		valsStr := strings.Join(vals, ", ")
		query := fmt.Sprintf(
			"insert into %s (%s) values (%s) \n",
			r.Table, cols, valsStr,
		)
		queries = append(queries, query)
	}

	// batch all insert queries into one
	return strings.Join(queries, "\n")
}

func (dbm *DBManager) Swap() error {
	now := time.Now()
	dbPath := path.Join(dbm.dbDir, fmt.Sprintf("%d.db", now.Unix()))
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return err
	}

	fmt.Println("swapping db to ", dbPath)
	dbm.db.Close()
	dbm.db = db
	dbm.createdAT = now

	// setup the new db
	if err := dbm.Setup(); err != nil {
		return err
	}

	return nil
}

func (dbm *DBManager) Setup() error {
	// initialize parquet extension
	_, err := dbm.db.Exec("INSTALL parquet; LOAD parquet;")
	if err != nil {
		return err
	}

	createQuery, err := dbm.generateCreateQuery()
	if err != nil {
		return fmt.Errorf("cannot create table in duckdb: %s", err)
	}

	// create table if it does not exist
	fmt.Println("create query: ", createQuery)
	_, err = dbm.db.Exec(createQuery)
	if err != nil {
		return err
	}

	return nil
}

func (dbm *DBManager) Replay(tx *pgrepl.Tx) error {
	query := dbm.queryFromWAL(tx) // (todo): error handling
	fmt.Println("replay query: ", query)
	res, err := dbm.db.Exec(query)
	if err != nil {
		return fmt.Errorf("cannot replay WAL record %s", err)
	}

	fmt.Println("Replayed the WAL log", res, query)
	return nil
}

func (dbm *DBManager) generateCreateQuery() (string, error) {
	var cols, pks string
	for i, column := range dbm.cols {
		col := fmt.Sprintf("%s %s", column.Name, column.Typ)
		if !column.IsNull {
			col = fmt.Sprintf("%s NOT NULL", col)
		}
		if i == 0 {
			cols = col
			if column.IsPrimary {
				pks = column.Name
			}
		} else {
			cols = fmt.Sprintf("%s,%s", cols, col)
			if column.IsPrimary {
				pks = fmt.Sprintf("%s,%s", pks, column.Name)
			}
		}
	}

	if pks != "" {
		cols = fmt.Sprintf("%s,PRIMARY KEY (%s)", cols, pks)
	}

	if cols == "" {
		return "", errors.New("schema must have at least one column")
	}

	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		dbm.table, cols)
	return stmt, nil
}

func NewUploadManager(ctx context.Context, dbDir, table string, interval time.Duration) *UploadManager {
	ctx, cancel := context.WithCancel(ctx)
	return &UploadManager{
		dbDir:    dbDir,
		table:    table,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

type UploadManager struct {
	dbDir    string
	table    string
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func (u *UploadManager) Start() {
	go func(ctx context.Context) {
		ticker := time.NewTicker(u.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				fmt.Println("Uploader is stopping")
				return
			case <-ticker.C:
				fmt.Println("Time to scan, export, upload and cleanup")
				if err := u.Upload(); err != nil {
					fmt.Println("upload error: ", err)
					continue
				}
			}
		}
	}(u.ctx)
}

func (u *UploadManager) Stop() {
	u.cancel()
}

func (u *UploadManager) Upload() error {
	re := regexp.MustCompile(`^\d+\.db$`)
	files, err := os.ReadDir(u.dbDir)
	if err != nil {
		return fmt.Errorf("cannot read dir: %s", err)
	}

	for _, f := range files {
		if re.MatchString(f.Name()) {
			dbPath := path.Join(u.dbDir, f.Name())
			fmt.Printf("db dump found, backing up %s\n", f.Name())
			exportFilePath := path.Join(u.dbDir, fmt.Sprintf("%s.parquet", f.Name()))
			exportQuery := fmt.Sprintf(
				"COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)", u.table, exportFilePath)

			db, err := sql.Open("duckdb", dbPath)
			if err != nil {
				return err
			}

			_, err = db.Exec("INSTALL parquet; LOAD parquet;")
			if err != nil {
				return err
			}

			_, err = db.Exec(exportQuery)
			if err != nil {
				return fmt.Errorf("cannot exprort to parquet file: %s", err)
			}

			// Read exported parquet file and send to provider
			f, err := os.Open(exportFilePath)
			if err != nil {
				return fmt.Errorf("open file: %s", err)
			}
			defer func() {
				_ = f.Close()
			}()

			// --- UPLOAD TO PROVIDER ---
			/*
				fi, err := f.Stat()
				if err != nil {
					return fmt.Errorf("fstat: %s", err)
				}

				progress := progressbar.DefaultBytes(
						fi.Size(),
						"Uploading file...",
				)
				if err := b.provider.Upload(
						ctx, b.namespace, table, uint64(fi.Size()),
						f, NewSigner(b.privateKey), progress,
				); err != nil {
						return fmt.Errorf("upload: %s", err)
				}
			*/
			// --- END UPLOAD TO PROVIDER ---

			fmt.Printf("deleting db dump %s\n", f.Name())
			if err := os.Remove(dbPath); err != nil {
				return fmt.Errorf("cannot delete file: %s", err)
			}

			fmt.Printf("deleting db WAL file %s\n", f.Name())
			if err := os.Remove(fmt.Sprintf("%s.wal", dbPath)); err != nil {
				fmt.Println("cannot delete file: ", err)
			}
		}
	}
	return nil
}
