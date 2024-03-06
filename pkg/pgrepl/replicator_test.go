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
	_, err := db.ExecContext(context.Background(), `
		create table t(id int primary key, name text);
		create table t2(id int primary key, name text);
		create publication pub_basin_t for table t, t2;
	`)
	require.NoError(t, err)
	replicator, err := New(uri, "t")
	require.NoError(t, err)

	feed, tables, err := replicator.StartReplication(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"public.t", "public.t2"}, tables)

	_, err = db.ExecContext(context.Background(), `
			insert into t values (1, 'foo');
			insert into t values (2, 'bar');
			insert into t2 values (4, 'foo2');
			insert into t values (3, 'baz');
			update t set name='quz' where id=3;
			delete from t where id=2;
		`)
	require.NoError(t, err)

	tx := <-feed
	require.Equal(t, 6, len(tx.Records))
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

	require.Equal(t, tx.Records[2].Table, "t2")
	require.Equal(t, tx.Records[2].Columns, []Column{
		{
			Name:  "id",
			Type:  "integer",
			Value: toJSON(t, 4),
		},
		{
			Name:  "name",
			Type:  "text",
			Value: toJSON(t, "foo2"),
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
