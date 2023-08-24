package pgrepl

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/tablelandnetwork/basin-cli/test"

	"github.com/stretchr/testify/require"
)

var (
	db  *sql.DB
	uri string
)

func TestMain(m *testing.M) {
	pool := test.GetDockerPool()

	var resource *dockertest.Resource
	db, resource, uri = pool.RunPostgres()

	// Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	pool.Purge(resource)

	os.Exit(code)
}

func TestReplication(t *testing.T) {
	t.Parallel()

	_, err := db.ExecContext(context.Background(), `
		create table t(id int primary key, name text);
		create publication pub_basin_t for table t;
	`)
	require.NoError(t, err)
	replicator, err := New(uri, "t")
	require.NoError(t, err)

	feed, pubName, err := replicator.StartReplication(context.Background())
	require.NoError(t, err)
	require.Equal(t, "public.t", pubName)

	_, err = db.ExecContext(context.Background(), `
			insert into t values (1, 'foo');
			insert into t values (2, 'bar');
			insert into t values (3, 'baz');
			update t set name='quz' where id=3;
			delete from t where id=2;
		`)
	require.NoError(t, err)

	tx := <-feed
	require.Equal(t, 5, len(tx.Records))
	require.Equal(t, tx.Records[0].Table, "t")
	require.Equal(t, tx.Records[0].Columns, []Column{
		{
			Name:  "id",
			Type:  "integer",
			Value: toJSON(t, 1),
		},
		{
			Name:  "name",
			Type:  "text",
			Value: toJSON(t, "foo"),
		},
	})

	// TODO: add more assertions

	replicator.Shutdown()
}

func toJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	bytes, err := json.Marshal(v)
	require.NoError(t, err)

	return bytes
}
