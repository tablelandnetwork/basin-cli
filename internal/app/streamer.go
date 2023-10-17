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
