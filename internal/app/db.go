package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"
)

type Column struct {
	Name, Typ         string
	IsNull, IsPrimary bool
}

type DBManager struct {
	db               *sql.DB
	dbDir            string
	table            string
	createdAT        time.Time
	cols             []Column
	replaceThreshold time.Duration
}

func NewDBManager(
	dbDir string,
	table string,
	cols []Column,
	threshold time.Duration,
) *DBManager {
	return &DBManager{
		dbDir:            dbDir,
		table:            table,
		createdAT:        time.Now(),
		cols:             cols,
		replaceThreshold: threshold,
	}
}

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
	now := time.Now()
	dbPath := path.Join(dbm.dbDir, fmt.Sprintf("%d.db", now.UnixNano()))

	// 1. close current db
	slog.Info("closing current db")
	if err := dbm.db.Close(); err != nil {
		return err
	}

	// 2. rename current.db to <timestamp>.db
	slog.Info("renaming current db", "to", dbPath)
	if err := os.Rename(path.Join(dbm.dbDir, "current.db"), dbPath); err != nil {
		return err
	}

	// 3. create a new current.db
	db, err := dbm.NewDB()
	if err != nil {
		return fmt.Errorf("swap: %v", err)
	}

	// 4. setup the new current db
	dbm.db = db
	dbm.createdAT = now
	return dbm.Setup(ctx)
}

// NewDB creates a new duckdb database at the current.db path.
func (dbm *DBManager) NewDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", path.Join(dbm.dbDir, "current.db"))
	if err != nil {
		return nil, fmt.Errorf("cannot open db: %s", err)
	}

	return db, nil
}

func (dbm *DBManager) Setup(ctx context.Context) error {
	createQuery, err := dbm.genCreateQuery()
	if err != nil {
		return fmt.Errorf("cannot create table in duckdb: %s", err)
	}

	// create table if it does not exist
	slog.Info("applying create", "query", createQuery)
	query := fmt.Sprintf("INSTALL parquet; LOAD parquet; %s", createQuery)
	_, err = dbm.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (dbm *DBManager) replaceThresholdExceeded() bool {
	delta := time.Since(dbm.createdAT).Seconds()
	threshold := dbm.replaceThreshold.Seconds()
	return delta > threshold
}

// Replay replays a WAL record to the current.db after materializing it.
// If the replace threshold is exceeded, it replaces the current.db with
// a new one.
func (dbm *DBManager) Replay(ctx context.Context, tx *pgrepl.Tx) error {
	if dbm.replaceThresholdExceeded() {
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
