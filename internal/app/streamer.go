package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	"capnproto.org/go/capnp/v3"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pglogrepl"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// BasinProvider ...
type BasinProvider interface {
	Push(context.Context, basincapnp.Tx, []byte) (uint64, error)
}

// BasinStreamer contains logic of streaming Postgres changes to Basin Provider.
type BasinStreamer struct {
	replicator Replicator
	privateKey *ecdsa.PrivateKey
	provider   BasinProvider
}

// NewBasinStreamer creates new app.
func NewBasinStreamer(r Replicator, bp BasinProvider, pk *ecdsa.PrivateKey) *BasinStreamer {
	return &BasinStreamer{
		replicator: r,
		provider:   bp,
		privateKey: pk,
	}
}

// Run runs the BasinStreamer logic.
func (b *BasinStreamer) Run(ctx context.Context) error {
	txs, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}

	for tx := range txs {
		slog.Info("new transaction receive")

		capnpTx, err := basincapnp.FromPgReplTx(tx)
		if err != nil {
			return fmt.Errorf("to capnproto: %s", err)
		}

		signature, err := b.sign(capnpTx)
		if err != nil {
			return fmt.Errorf("sign: %s", err)
		}

		_, err = b.provider.Push(ctx, capnpTx, signature)
		if err != nil {
			return fmt.Errorf("push: %s", err)
		}

		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}

		slog.Info("transaction acked")
	}

	return nil
}

func (b *BasinStreamer) sign(tx basincapnp.Tx) ([]byte, error) {
	bytes, err := capnp.Canonicalize(tx.ToPtr().Struct())
	if err != nil {
		return []byte{}, fmt.Errorf("canonicalize: %s", err)
	}

	hash := sha256.Sum256(bytes)
	signature, err := crypto.Sign(hash[:], b.privateKey)
	if err != nil {
		return []byte{}, fmt.Errorf("sign: %s", err)
	}

	return signature, nil
}
