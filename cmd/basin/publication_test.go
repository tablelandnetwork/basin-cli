package main

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/test"
	"github.com/urfave/cli/v2"
)

func TestPublication(t *testing.T) {
	t.Skip()

	t.Parallel()

	pool := test.GetDockerPool()

	db, resource, dburi := pool.RunPostgres()

	cliApp := &cli.App{
		Name:  "basin",
		Usage: "basin replicates your database as logs and store them in Filecoin",
		Commands: []*cli.Command{
			newPublicationCommand(),
		},
	}

	h, err := newHelper(cliApp, db, dburi, t.TempDir())
	require.NoError(t, err)

	h.CreateTable(t)
	h.CreatePublication(t)

	go func() {
		h.StartReplication(t)
	}()

	time.Sleep(1 * time.Second)

	h.AddNewRecord(t)
	h.AddNewRecord(t)

	pool.Purge(resource)
}

type helper struct {
	app       *cli.App
	db        *sql.DB
	dburi     string
	pk        *ecdsa.PrivateKey
	tableName string
	dir       string
}

func newHelper(app *cli.App, db *sql.DB, dburi string, dir string) (*helper, error) {
	pk, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}

	return &helper{
		pk:        pk,
		tableName: "t",
		dir:       dir,

		app:   app,
		db:    db,
		dburi: dburi,
	}, nil
}

func (h *helper) CreateTable(t *testing.T) {
	_, err := h.db.ExecContext(
		context.Background(),
		fmt.Sprintf("create table %s(id serial primary key, name text);", h.tableName),
	)
	require.NoError(t, err)
}

func (h *helper) CreatePublication(t *testing.T) {
	err := h.app.Run([]string{
		"basin",
		"publication", "--dir", h.dir,
		"create", "--dburi", h.dburi, "--address", crypto.PubkeyToAddress(h.pk.PublicKey).Hex(), "--provider", "mock",
		h.tableName,
	})
	require.NoError(t, err)
}

func (h *helper) StartReplication(t *testing.T) {
	ctx := context.Background()
	err := h.app.RunContext(ctx, []string{
		"basin",
		"publication", "--dir", h.dir,
		"start", "--name", h.tableName, "--private-key", hex.EncodeToString(crypto.FromECDSA(h.pk)),
		h.tableName,
	})
	require.NoError(t, err)
}

func (h *helper) AddNewRecord(t *testing.T) {
	_, err := h.db.ExecContext(context.Background(), fmt.Sprintf("insert into %s (name) values ('foo');", h.tableName))
	require.NoError(t, err)
}
