package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/exc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pglogrepl"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"golang.org/x/exp/slog"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, string, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// BasinProvider ...
type BasinProvider interface {
	Create(context.Context, string, string, basincapnp.Schema, common.Address) error
	Push(context.Context, string, string, basincapnp.Tx, []byte) error
	Reconnect() error
}

// BasinStreamer contains logic of streaming Postgres changes to Basin Provider.
type BasinStreamer struct {
	namespace  string
	replicator Replicator
	privateKey *ecdsa.PrivateKey
	provider   BasinProvider
}

// NewBasinStreamer creates new app.
func NewBasinStreamer(ns string, r Replicator, bp BasinProvider, pk *ecdsa.PrivateKey) *BasinStreamer {
	return &BasinStreamer{
		namespace:  ns,
		replicator: r,
		provider:   bp,
		privateKey: pk,
	}
}

// Run runs the BasinStreamer logic.
func (b *BasinStreamer) Run(ctx context.Context) error {
	txs, table, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}

	for tx := range txs {
		slog.Info("new transaction received")

		capnpTx, err := basincapnp.FromPgReplTx(tx)
		if err != nil {
			return fmt.Errorf("to capnproto: %s", err)
		}

		signature, err := b.sign(capnpTx)
		if err != nil {
			return fmt.Errorf("sign: %s", err)
		}

		if err := b.push(ctx, table, capnpTx, signature); err != nil {
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

	hash := crypto.Keccak256Hash(bytes)
	signature, err := crypto.Sign(hash.Bytes(), b.privateKey)
	if err != nil {
		return []byte{}, fmt.Errorf("sign: %s", err)
	}

	return signature, nil
}

func (b *BasinStreamer) push(ctx context.Context, table string, tx basincapnp.Tx, signature []byte) error {
	if err := b.provider.Push(ctx, b.namespace, table, tx, signature); exc.IsType(err, exc.Disconnected) {
		slog.Info("reconnecting")
		if err := b.provider.Reconnect(); err != nil {
			return fmt.Errorf("reconnect: %s", err)
		}
		return b.push(ctx, table, tx, signature)
	} else if err != nil {
		return err
	}
	return nil
}
