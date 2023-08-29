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
	// This is supposed to be the closest to an e2E test we get
	// skipping this for now until I think of a way to assert the data that went to Basin Provider
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
	app   *cli.App
	db    *sql.DB
	dburi string
	pk    *ecdsa.PrivateKey
	ns    string
	rel   string
	dir   string
}

func newHelper(app *cli.App, db *sql.DB, dburi string, dir string) (*helper, error) {
	pk, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}

	return &helper{
		pk:  pk,
		ns:  "n",
		rel: "t",
		dir: dir,

		app:   app,
		db:    db,
		dburi: dburi,
	}, nil
}

func (h *helper) CreateTable(t *testing.T) {
	_, err := h.db.ExecContext(
		context.Background(),
		fmt.Sprintf("create table %s(id serial primary key, name text);", h.rel),
	)
	require.NoError(t, err)
}

func (h *helper) CreatePublication(t *testing.T) {
	name := fmt.Sprintf("%s.%s", h.ns, h.rel)
	err := h.app.Run([]string{
		"basin",
		"publication",
		"--dir", h.dir,
		"create",
		"--dburi", h.dburi,
		"--address", crypto.PubkeyToAddress(h.pk.PublicKey).Hex(),
		"--provider", "mock",
		name,
	})
	require.NoError(t, err)
}

func (h *helper) StartReplication(t *testing.T) {
	ctx := context.Background()
	name := fmt.Sprintf("%s.%s", h.ns, h.rel)
	err := h.app.RunContext(ctx, []string{
		"basin",
		"publication",
		"--dir", h.dir,
		"start",
		"--private-key", hex.EncodeToString(crypto.FromECDSA(h.pk)),
		name,
	})
	require.NoError(t, err)
}

func (h *helper) AddNewRecord(t *testing.T) {
	_, err := h.db.ExecContext(context.Background(), fmt.Sprintf("insert into %s (name) values ('foo');", h.rel))
	require.NoError(t, err)
}
