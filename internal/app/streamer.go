package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pglogrepl"
	"github.com/schollz/progressbar/v3"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"

	"database/sql"

	_ "github.com/marcboeker/go-duckdb"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, string, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// BasinProvider ...
type BasinProvider interface {
	Create(context.Context, string, string, basincapnp.Schema, common.Address) (bool, error)
	Push(context.Context, string, string, basincapnp.Tx, []byte) error
	Reconnect() error
}

// BasinStreamer contains logic of streaming Postgres changes to Basin Provider.
type BasinStreamer struct {
	namespace  string
	replicator Replicator
	privateKey *ecdsa.PrivateKey
	provider   BasinProviderUploader
}

// NewBasinStreamer creates new streamer.
func NewBasinStreamer(ns string, r Replicator, bp BasinProviderUploader, pk *ecdsa.PrivateKey) *BasinStreamer {
	return &BasinStreamer{
		namespace:  ns,
		replicator: r,
		provider:   bp,
		privateKey: pk,
	}
}

// queryGen creates a query for a WAL TX records.
func (b *BasinStreamer) queryGen(tx *pgrepl.Tx) string {
	var query string
	for _, r := range tx.Records {
		columnNames := []string{}
		vals := []string{}
		for _, c := range r.Columns {
			columnNames = append(columnNames, c.Name)
			vals = append(vals, string(c.Value))
		}

		cols := strings.Join(columnNames, ", ")
		valsStr := strings.Join(vals, ", ")
		query = fmt.Sprintf(
			"insert into %s (%s) values (%s) \n",
			r.Table, cols, valsStr,
		)
	}
	return query
}

type DBManager struct {
	db          *sql.DB
	dbFilename  string
	dbDir       string
	tableSchema string
	createdAT   time.Time
}

func NewDBManager(dbDir, tableSchema string) (*DBManager, error) {
	nowEpoch := time.Now().Unix()
	dbFilename := fmt.Sprintf("%d.db", nowEpoch)
	dbPath := path.Join(dbDir, dbFilename)
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	return &DBManager{
		dbFilename:  dbFilename,
		dbDir:       dbDir,
		tableSchema: tableSchema,
		createdAT:   time.Now(),
		db:          db,
	}, nil
}

func (dbm *DBManager) close() {
	dbm.db.Close()
}

func (dbm *DBManager) swap() error {
	now := time.Now()
	dbPath := path.Join(dbm.dbDir, fmt.Sprintf("%d.db", now.Unix()))
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return err
	}

	fmt.Println("swapping db to ", dbPath)
	dbm.db = db
	dbm.createdAT = now

	// setup the new db
	if err := dbm.setup(); err != nil {
		return err
	}

	return nil
}

func (dbm *DBManager) setup() error {
	// initialize parquet extension
	_, err := dbm.db.Exec("INSTALL parquet; LOAD parquet;")
	if err != nil {
		return err
	}

	// create table if it does not exist
	fmt.Println("source schema: ", dbm.tableSchema)
	_, err = dbm.db.Exec(dbm.tableSchema)
	if err != nil {
		return err
	}

	return nil
}

func (b *BasinStreamer) upload(ctx context.Context, db *sql.DB, table string, lastExportedTS uint64) error {
	exportFilePath := fmt.Sprintf("export.%d.parquet", lastExportedTS)
	obsoleteDBPath := fmt.Sprintf("%s_%d.db", table, lastExportedTS)

	// Export old db
	exportQuery := fmt.Sprintf(
		"COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)", table, exportFilePath)
	_, err := db.Exec(exportQuery)
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

	// cleanup (delete the table and export file)

	// delete old db file
	if err := os.Remove(obsoleteDBPath); err != nil {
		return fmt.Errorf("cannot remove db file: %s", err)
	}

	// delete the exported parquet file
	if err := os.Remove(exportFilePath); err != nil {
		return fmt.Errorf("cannot remove file: %s", err)
	}

	return nil
}

// Run runs the BasinStreamer logic.
func (b *BasinStreamer) Run(ctx context.Context, tableSchema, dbDir string) error {
	txs, table, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}
	fmt.Println("table: ", table)

	// Creates a new db manager when replication starts
	dbMngr, err := NewDBManager(dbDir, tableSchema)
	if err != nil {
		return fmt.Errorf("cannot create db manager: %s", err)
	}

	if err := dbMngr.setup(); err != nil {
		return fmt.Errorf("cannot setup db: %s", err)
	}

	go func() {
		ticker := time.NewTicker(55 * time.Second)
		defer ticker.Stop()

		pattern := `^\d+\.db$`
		re := regexp.MustCompile(pattern)

		for range ticker.C {
			fmt.Println("Time to scan, export, upload and cleanup")
			files, err := os.ReadDir(dbDir)
			if err != nil {
				fmt.Println("cannot read dir: ", err)
				continue
			}

			for _, f := range files {
				if re.MatchString(f.Name()) {
					fmt.Printf("db dump found, backing up %s\n", f.Name())
					fmt.Printf("deleting db dump %s\n", f.Name())
					if err := os.Remove(path.Join(dbDir, f.Name())); err != nil {
						fmt.Println("cannot delete file: ", err)
					}
					/* (todo) implement uplaod
					err = b.upload(ctx, oldDB, ddbTable, lastExportedTS) // should be last export ts
					if err != nil {
						return fmt.Errorf("cannot upload: %s", err)
					}
					*/
					fmt.Printf("deleting db WAL file %s\n", f.Name())
					if err := os.Remove(fmt.Sprintf("%s.wal", path.Join(dbDir, f.Name()))); err != nil {
						fmt.Println("cannot delete file: ", err)
					}
				}
			}
		}
	}()
	
	for tx := range txs {
		delta := time.Since(dbMngr.createdAT)
		if delta.Seconds() > 60 { // todo: change window size
			// swap the db in the db manager
			dbMngr.swap()
		}

		slog.Info("new transaction received")
		query := b.queryGen(tx)
		res, err := dbMngr.db.Exec(query)
		if err != nil {
			return fmt.Errorf("cannot replay WAL record %s", err)
		}

		fmt.Println("Replayed the WAL log", res, query)

		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}
		slog.Info("transaction acked")

	}

	return nil
}
