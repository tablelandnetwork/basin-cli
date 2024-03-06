package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb" // register duckdb driver
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
	// deps
	uploader *VaultsUploader

	// db attrs
	db      *sql.DB
	dbDir   string
	dbFname string
	schemas []TableSchema

	// configs
	windowInterval time.Duration

	// lock
	mu sync.Mutex

	// control attributes
	close chan struct{}
}

// TableSchema represents a table and its schema.
type TableSchema struct {
	Table   string
	Columns []Column
}

// NewDBManager creates a new DBManager.
func NewDBManager(
	dbDir string, schemas []TableSchema, windowInterval time.Duration, uploader *VaultsUploader,
) *DBManager {
	return &DBManager{
		dbDir:          dbDir,
		schemas:        schemas,
		windowInterval: windowInterval,
		uploader:       uploader,
	}
}

// NewDB creates a new duckdb database at the <ts>.db path.
func (dbm *DBManager) NewDB(ctx context.Context) error {
	now := time.Now()
	dbm.dbFname = fmt.Sprintf("%d.db", now.UnixNano())
	dbPath := path.Join(dbm.dbDir, dbm.dbFname)
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("cannot open db: %s", err)
	}

	slog.Info("created new db", "at", dbPath)
	dbm.db = db

	if err := dbm.setup(ctx); err != nil {
		return fmt.Errorf("cannot setup db: %s", err)
	}

	ticker := time.NewTicker(dbm.windowInterval)
	dbm.close = make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				dbm.mu.Lock()
				slog.Info("window interval passed")
				if err := dbm.replace(ctx); err != nil {
					slog.Error("replacing current db before replaying further txs", "error", err)
				}
				dbm.mu.Unlock()
			case <-dbm.close:
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

// Replay replays a WAL record onto the current db.
// If the window has passed, it replaces the current db with
// a new one. The current db is exported and uploaded before
// new db is ready to be used.
func (dbm *DBManager) Replay(ctx context.Context, tx *pgrepl.Tx) error {
	dbm.mu.Lock()
	defer dbm.mu.Unlock()

	query, err := dbm.queryFromWAL(tx)
	if err != nil {
		return err
	}

	slog.Info("replaying", "query", query)
	_, err = dbm.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("cannot replay WAL record: %v", err)
	}

	return nil
}

// Export exports the current db to a parquet file at the given path.
func (dbm *DBManager) Export(ctx context.Context, exportPath string) ([]string, error) {
	var err error
	db := dbm.db
	// db is nil before replication starts.
	// In that case, we open all existing db files
	// and upload them.
	if db == nil {
		dbm.dbFname = path.Base(exportPath)
		// convert the export path to a db path:
		// <ts>.db.parquet -> <ts>.db
		dbPath := strings.ReplaceAll(exportPath, ".parquet", "")
		db, err = sql.Open("duckdb", dbPath)
		if err != nil {
			return []string{}, err
		}
		defer func() {
			if err := db.Close(); err != nil {
				slog.Error("cannot close db", "error", err)
			}
		}()
		slog.Info("backing up db", "at", dbPath)
	} else {
		slog.Info("backing up current db")
	}

	exportedFiles := []string{}
	for _, schema := range dbm.schemas {
		var n int
		if err := db.QueryRowContext(
			ctx,
			fmt.Sprintf("select count(1) from %s LIMIT 1", schema.Table),
		).Scan(&n); err != nil {
			return []string{}, fmt.Errorf("querying row count: %s", err)
		}

		if n == 0 {
			continue
		}

		exportedFileName := strings.Replace(exportPath, dbm.dbFname, fmt.Sprintf("%s-%s", schema.Table, dbm.dbFname), -1)
		exportedFiles = append(exportedFiles, exportedFileName)
		_, err = db.ExecContext(ctx,
			fmt.Sprintf(
				`INSTALL parquet;
				 LOAD parquet;
				 COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)`,
				schema.Table, exportedFileName))
		if err != nil {
			return []string{}, fmt.Errorf("cannot export to parquet file: %s", err)
		}
	}

	return exportedFiles, nil
}

// UploadAt uploads a db dump at the given path.
// It returns an error if a dumps cannot be uploaded.
// It cleans up the db dumps and export files after uploading.
func (dbm *DBManager) UploadAt(ctx context.Context, exportPath string) error {
	f, err := os.Open(exportPath)
	if err != nil {
		return fmt.Errorf("cannot open file: %s", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %s", err)
	}

	ts := NewTimestamp(time.Now().UTC())
	if err := dbm.uploader.Upload(ctx, exportPath, io.Discard, ts, fi.Size()); err != nil {
		return fmt.Errorf("upload: %s", err)
	}

	// cleanup the exported parquet file
	slog.Info("deleting db parquet export", "at", exportPath)
	if err := os.Remove(exportPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
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
			exportAt := dbPath + ".parquet"
			files, err := dbm.Export(ctx, exportAt)
			if err != nil {
				return fmt.Errorf("export: %s", err)
			}

			for _, file := range files {
				if err := dbm.UploadAt(ctx, file); err != nil {
					return fmt.Errorf("upload: %s", err)
				}
			}

			if err := dbm.cleanup(dbPath); err != nil {
				return fmt.Errorf("cleanup: %s", err)
			}
		}
	}

	return nil
}

// Close closes the current db.
func (dbm *DBManager) Close() {
	close(dbm.close)
	_ = dbm.db.Close()
}

// queryFromWAL creates a query for a WAL TX records.
func (dbm *DBManager) queryFromWAL(tx *pgrepl.Tx) (string, error) {
	var columnValsStr string

	// build an insert stmt for each record inside tx
	stmts := []string{}
	for _, r := range tx.Records {
		cols := []string{}
		for _, c := range r.Columns {
			cols = append(cols, c.Name)
		}

		columnVals := []string{}
		for _, c := range r.Columns {
			ddbType, err := dbm.pgToDDBType(c.Type)
			if err != nil {
				return "", err
			}
			columnVal := ddbType.transformFn(string(c.Value))
			columnVals = append(columnVals, columnVal)
		}
		columnValsStr = strings.Join(columnVals, ", ")
		recordVals := fmt.Sprintf("(%s)", columnValsStr)

		stmt := fmt.Sprintf(
			"insert into %s (%s) values %s",
			r.Table,
			strings.Join(cols, ", "),
			recordVals,
		)

		stmts = append(stmts, stmt)
	}

	return strings.Join(stmts, ";"), nil
}

func (dbm *DBManager) replace(ctx context.Context) error {
	// Export current db to a parquet file at a given path
	exportAt := path.Join(dbm.dbDir, dbm.dbFname) + ".parquet"
	files, err := dbm.Export(ctx, exportAt)
	if err != nil {
		return err
	}

	// Close current db
	slog.Info("closing current db")
	dbm.Close()

	for _, file := range files {
		// Upload the exported parquet file
		if err := dbm.UploadAt(ctx, file); err != nil {
			slog.Error("upload error, skipping", "err", err)
		}
	}

	// Cleanup the previous db and wal files
	oldDBPath := path.Join(dbm.dbDir, dbm.dbFname)
	if err := dbm.cleanup(oldDBPath); err != nil {
		return fmt.Errorf("cleanup: %s", err)
	}

	// Create a new db
	if err := dbm.NewDB(ctx); err != nil {
		return fmt.Errorf("new db: %v", err)
	}

	return nil
}

// setup creates a local table in the local db.
func (dbm *DBManager) setup(ctx context.Context) error {
	query, err := dbm.genCreateQuery()
	if err != nil {
		return err
	}

	// create table if it does not exist
	slog.Info("applying create", "query", query)
	_, err = dbm.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

// pgToDDBType maps a PG type to a duckdb type.
func (dbm *DBManager) pgToDDBType(typ string) (duckdbType, error) {
	// handle character(N), character varying(N), numeric(N, M)
	if strings.HasSuffix(typ, ")") {
		typ = strings.Split(typ, "(")[0]
	}

	// handle character(N)[], character varying(N)[], numeric(N, M)[]
	if strings.HasSuffix(typ, ")[]") {
		typ = strings.Split(typ, "(")[0] + "[]"
	}

	ddbType, ok := typeConversionMap[typ]
	if !ok {
		// custom enum, stucts and n-d array types are not supported
		return duckdbType{}, fmt.Errorf("unsupported type: %s", typ)
	}
	return ddbType, nil
}

func (dbm *DBManager) genCreateQuery() (string, error) {
	stmts := []string{}
	for _, schema := range dbm.schemas {
		var cols, pks string
		for i, column := range schema.Columns {
			ddbType, err := dbm.pgToDDBType(column.Typ)
			if err != nil {
				return "", err
			}
			col := fmt.Sprintf("%s %s", column.Name, ddbType.typeName)
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
			schema.Table, cols)
		stmts = append(stmts, stmt)
	}

	return strings.Join(stmts, ";"), nil
}

func (dbm *DBManager) cleanup(dbPath string) error {
	slog.Info("deleting db dump", "at", dbPath)
	if err := os.Remove(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
	}

	walPath := dbPath + ".wal"
	slog.Info("deleting db wal", "at", walPath)
	if err := os.Remove(walPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file: %s", err)
		}
	}

	return nil
}
