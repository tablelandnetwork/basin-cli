package pgrepl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

var (
	db          *sql.DB
	databaseURL string
)

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "debezium/postgres",
		Tag:        "14-alpine",
		Cmd:        []string{"postgres", "-c", "wal_level=logical"},
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=admin",
			"POSTGRES_DB=basin",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	_ = resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	databaseURL = fmt.Sprintf("postgres://admin:secret@%s/basin?sslmode=disable", resource.GetHostPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		db, err = sql.Open("postgres", databaseURL)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestReplication(t *testing.T) {
	_, err := db.ExecContext(context.Background(), `
		CREATE TABLE hba (lines text); 
		COPY hba FROM '/var/lib/postgresql/data/pg_hba.conf';
		INSERT INTO hba (lines) VALUES ('host  replication admin  172.17.0.1/32                 md5');
		COPY hba TO '/var/lib/postgresql/data/pg_hba.conf';
		SELECT pg_reload_conf();
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(), `
		create table t(id int primary key, name text);
		create publication pub_basin_t for table t;
	`)
	require.NoError(t, err)
	replicator, err := New(databaseURL, "t")
	require.NoError(t, err)

	feed, err := replicator.StartReplication(context.Background())
	require.NoError(t, err)

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
