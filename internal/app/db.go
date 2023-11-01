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

	"github.com/schollz/progressbar/v3"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"
)

// Column represents a column in a table being replicated.
type Column struct {
	Name, Typ         string
	IsNull, IsPrimary bool
}

// DBManager manages a duckdb database.
type DBManager struct {
	db        *sql.DB
	dbDir     string
	table     string
	createdAT time.Time
	cols      []Column
	winSize   time.Duration
	uploader  *BasinUploader
}

// NewDBManager creates a new DBManager.
func NewDBManager(dbDir, table string, cols []Column, winSize time.Duration, uploader *BasinUploader) *DBManager {
	return &DBManager{
		dbDir:     dbDir,
		table:     table,
		createdAT: time.Now(),
		cols:      cols,
		winSize:   winSize,
		uploader:  uploader,
	}
}

// Close closes the current db.
func (dbm *DBManager) Close() error {
	return dbm.db.Close()
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

func (dbm *DBManager) replace(ctx context.Context) error {
	// Export current db to a parquet file
	now := time.Now()
	exportPath := path.Join(dbm.dbDir, fmt.Sprintf("%d.db", now.UnixNano()))
	if err := dbm.Export(ctx, exportPath); err != nil {
		return err
	}

	// Close current db
	slog.Info("closing current db")
	if err := dbm.Close(); err != nil {
		return err
	}

	// Upload the exported parquet file
	if err := dbm.UploadAt(ctx, exportPath); err != nil {
		fmt.Println("upload error, skipping", "err", err)
	}

	// Create a new db
	db, err := dbm.NewDB()
	if err != nil {
		return fmt.Errorf("new db: %v", err)
	}

	// Setup the new db
	dbm.db = db
	dbm.createdAT = now
	return dbm.Setup(ctx)
}

// NewDB creates a new duckdb database at the <ts>.db path.
func (dbm *DBManager) NewDB() (*sql.DB, error) {
	now := time.Now()
	dbPath := path.Join(dbm.dbDir, fmt.Sprintf("%d.db", now.UnixNano()))
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open db: %s", err)
	}

	return db, nil
}

// Setup creates a local table in the local db.
func (dbm *DBManager) Setup(ctx context.Context) error {
	query, err := dbm.genCreateQuery()
	if err != nil {
		return fmt.Errorf("cannot create table in duckdb: %s", err)
	}

	// create table if it does not exist
	slog.Info("applying create", "query", query)
	_, err = dbm.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (dbm *DBManager) windowPassed() bool {
	if dbm.createdAT.IsZero() {
		return false
	}
	delta := time.Since(dbm.createdAT).Seconds()
	threshold := dbm.winSize.Seconds()
	return delta > threshold
}

// Replay replays a WAL record onto the current db.
// If the window has passed, it replaces the current db with
// a new one. The current db is exported and uploaded before
// new db is ready to be used.
func (dbm *DBManager) Replay(ctx context.Context, tx *pgrepl.Tx) error {
	if dbm.windowPassed() {
		slog.Info("replacing current db before replaying further txs")
		if err := dbm.replace(ctx); err != nil {
			return fmt.Errorf("cannot replace db: %v", err)
		}
	}

	query := dbm.queryFromWAL(tx) // (todo): error handling
	slog.Info("replaying", "query", query)

	_, err := dbm.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("cannot replay WAL record %s", err)
	}

	return nil
}

func (dbm *DBManager) genCreateQuery() (string, error) {
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

// Export exports the current db to a parquet file at the given path.
func (dbm *DBManager) Export(ctx context.Context, fname string) error {
	slog.Info("backing up db dump", "at", fname)
	var err error
	db := dbm.db
	// db is nil before replication starts.
	// In that case, we open all existing db files
	// and upload them.
	if db == nil {
		db, err = sql.Open("duckdb", fname)
		if err != nil {
			return err
		}
		defer func() {
			if err := db.Close(); err != nil {
				fmt.Println("cannot close db", "err", err)
			}
		}()
	}
	_, err = db.ExecContext(ctx,
		fmt.Sprintf(
			`INSTALL parquet;
			 LOAD parquet;
			 COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)`,
			dbm.table, fname+".parquet"))
	if err != nil {
		return fmt.Errorf("cannot export to parquet file: %s", err)
	}

	return nil
}

func (dbm *DBManager) cleanup(fname string) error {
	dbPath := path.Join(dbm.dbDir, fname)
	slog.Info("deleting db dump", "at", dbPath)
	if err := os.Remove(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
	}

	slog.Info("deleting db wal", "at", dbPath+".wal")
	if err := os.Remove(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
	}

	slog.Info("deleting db export", "at", dbPath+".parquet")
	if err := os.Remove(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
	}

	return nil
}

// UploadAt uploads a db dump at the given path.
// It returns an error if a dumps cannot be uploaded.
// It cleans up the db dumps and export files after uploading.
func (dbm *DBManager) UploadAt(ctx context.Context, dbPath string) error {
	exportPath := dbPath + ".parquet"
	f, err := os.Open(exportPath)
	if err != nil {
		return fmt.Errorf("cannot open file: %s", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %s", err)
	}

	progress := progressbar.DefaultBytes(
		fi.Size(),
		"Uploading file...",
	)

	if err := dbm.uploader.Upload(ctx, exportPath, progress); err != nil {
		return fmt.Errorf("upload: %s", err)
	}

	if err := dbm.cleanup(dbPath); err != nil {
		return fmt.Errorf("cleanup: %s", err)
	}

	return nil
}

// UploadAll uploads all db dumps in the db dir.
func (dbm *DBManager) UploadAll(ctx context.Context) error {
	files, err := os.ReadDir(dbm.dbDir)
	if err != nil {
		return fmt.Errorf("read dir: %s", err)
	}

	// find all db dumps: <timestamp>.db
	re := regexp.MustCompile(`^\d+\.db$`)
	for _, file := range files {
		fname := file.Name()
		if re.MatchString(fname) {
			dbPath := path.Join(dbm.dbDir, fname)
			if err = dbm.Export(ctx, dbPath); err != nil {
				return fmt.Errorf("export: %s", err)
			}
			if err := dbm.UploadAt(ctx, dbPath); err != nil {
				return fmt.Errorf("upload: %s", err)
			}
		}
	}

	return nil
}
