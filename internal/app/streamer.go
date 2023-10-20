package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pglogrepl"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
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
