package app

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"

	// Register duckdb driver.
	_ "github.com/marcboeker/go-duckdb"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, string, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// VaultsStreamer contains logic of streaming Postgres changes to Vaults Provider.
type VaultsStreamer struct {
	namespace  string
	replicator Replicator
	dbMngr     *DBManager
}

// NewVaultsStreamer creates new streamer.
func NewVaultsStreamer(ns string, r Replicator, dbm *DBManager) *VaultsStreamer {
	return &VaultsStreamer{
		namespace:  ns,
		replicator: r,
		dbMngr:     dbm,
	}
}

// Run runs the VaultsStreamer logic.
func (b *VaultsStreamer) Run(ctx context.Context) error {
	// Open a local DB for replaying txs
	if err := b.dbMngr.NewDB(ctx); err != nil {
		return err
	}

	defer func() {
		_ = b.dbMngr.Close()
	}()

	// Start replication
	txs, _, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}

	for tx := range txs {
		slog.Info("new transaction received")
		if err := b.dbMngr.Replay(ctx, tx); err != nil {
			return fmt.Errorf("replay: %s", err)
		}
		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}
		slog.Info("transaction acked")
	}

	return nil
}
