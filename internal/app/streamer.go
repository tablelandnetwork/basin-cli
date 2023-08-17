package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

// BasinProvider ...
type BasinProvider interface {
	Push([]byte) error
}

// BasinStreamer contains logic of streaming Postgres changes to Basin Provider.
type BasinStreamer struct {
	replicator Replicator
	privateKey interface{} // nolint
	provider   BasinProvider
}

// NewBasinStreamer creates new app.
func NewBasinStreamer(r Replicator, bp BasinProvider) *BasinStreamer {
	return &BasinStreamer{
		replicator: r,
		provider:   bp,
	}
}

// Run runs the BasinStreamer logic.
func (b *BasinStreamer) Run(ctx context.Context) error {
	txs, err := b.replicator.StartReplication(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %s", err)
	}

	for tx := range txs {
		v, err := json.MarshalIndent(tx, "", " ")
		fmt.Println(string(v), err)

		payload, err := b.encode()
		if err != nil {
			return fmt.Errorf("encode: %s", err)
		}

		signedPayload, err := b.sign(payload)
		if err != nil {
			return fmt.Errorf("sign: %s", err)
		}

		if err := b.provider.Push(signedPayload); err != nil {
			return fmt.Errorf("push: %s", err)
		}

		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}
	}

	return nil
}

func (b *BasinStreamer) encode() ([]byte, error) {
	return []byte{}, nil
}

func (b *BasinStreamer) sign([]byte) ([]byte, error) {
	return []byte{}, nil
}
