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

	"github.com/jackc/pglogrepl"

	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"

	_ "github.com/marcboeker/go-duckdb"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, string, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// BasinStreamer contains logic of streaming Postgres changes to Basin Provider.
type BasinStreamer struct {
	namespace  string
	replicator Replicator
	privateKey *ecdsa.PrivateKey
	provider   BasinProviderUploader
	dbMngr     *DBManager // (todo): change it to interface for testing?
}

// NewBasinStreamer creates new streamer.
func NewBasinStreamer(ns string, r Replicator, bp BasinProviderUploader, dbm *DBManager, pk *ecdsa.PrivateKey) *BasinStreamer {
	return &BasinStreamer{
		namespace:  ns,
		replicator: r,
		provider:   bp,
		dbMngr:     dbm,
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

// Run runs the BasinStreamer logic.
func (b *BasinStreamer) Run(ctx context.Context) error {
	txs, table, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}
	fmt.Println("table: ", table)

	// Setup local DB for replaying txs
	if err := b.dbMngr.Setup(); err != nil {
		return fmt.Errorf("cannot setup db: %s", err)
	}
	// defer b.dbMngr.Close()
	// start "exports" uploader in the background
	b.dbMngr.uploadMngr.Start()

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
		// todo: change window size to read from config
		if time.Since(b.dbMngr.createdAT).Seconds() > 60 {
			// swap the db in the db manager
			b.dbMngr.Swap()
		}

		slog.Info("new transaction received")

		if err := b.dbMngr.Replay(tx); err != nil {
			return fmt.Errorf("replay: %s", err)
		}

		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}
		slog.Info("transaction acked")
	}

	return nil
}
