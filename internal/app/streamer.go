package app

import (
	"context"
	"encoding/json"
	"fmt"

	"capnproto.org/go/capnp/v3"
	"github.com/jackc/pglogrepl"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// Replicator replicates Postgres txs into a channel.
type Replicator interface {
	StartReplication(ctx context.Context) (chan *pgrepl.Tx, error)
	Commit(ctx context.Context, lsn pglogrepl.LSN) error
	Shutdown()
}

type (
	// TxData is the tx encoded data.
	TxData []byte

	// Signature is the signature of TxData.
	Signature []byte
)

// BasinProvider ...
type BasinProvider interface {
	Push(context.Context, TxData, Signature) (uint64, error)
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
		data, _ := json.MarshalIndent(tx, "", "    ")
		fmt.Println(string(data))

		arena := capnp.SingleSegment(nil)
		msg, seg, err := capnp.NewMessage(arena)
		if err != nil {
			return fmt.Errorf("capnp new message: %s", err)
		}

		capnpTx, err := basincapnp.NewRootTx(seg)
		if err != nil {
			return fmt.Errorf("capnp new tx: %s", err)
		}

		// right now i'm just setting this field for testing
		capnpTx.SetCommitLSN(uint64(tx.CommitLSN))

		// txData, err := capnp.Canonicalize(capnpTx.ToPtr().Struct())
		// if err != nil {
		// 	return fmt.Errorf("canonicalize: %s", err)
		// }

		txData, err := msg.Marshal()
		if err != nil {
			return fmt.Errorf("marshal: %s", err)
		}

		signature, err := b.sign(txData)
		if err != nil {
			return fmt.Errorf("sign: %s", err)
		}

		response, err := b.provider.Push(ctx, txData, signature)
		if err != nil {
			return fmt.Errorf("push: %s", err)
		}

		fmt.Println(response)

		if err := b.replicator.Commit(ctx, tx.CommitLSN); err != nil {
			return fmt.Errorf("commit: %s", err)
		}
	}

	return nil
}

func (b *BasinStreamer) sign([]byte) ([]byte, error) {
	// TODO: implement this
	return []byte{}, nil
}
