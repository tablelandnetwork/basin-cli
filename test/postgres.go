package test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var (
	p    *DockerPool
	once sync.Once
)

// DockerPool lets you manage docker containers.
type DockerPool struct {
	pool *dockertest.Pool
}

// GetDockerPool get a docker pool.
func GetDockerPool() *DockerPool {
	once.Do(func() {
		pool, err := dockertest.NewPool("")
		if err != nil {
			log.Fatalf("Could not construct pool: %s", err)
		}

		err = pool.Client.Ping()
		if err != nil {
			log.Fatalf("Could not connect to Docker: %s", err)
		}

		pool.MaxWait = 120 * time.Second

		p = &DockerPool{
			pool: pool,
		}
	})

	return p
}

// RunPostgres creates a Postgres container.
func (dp *DockerPool) RunPostgres() (db *sql.DB, resource *dockertest.Resource, uri string) {
	// pulls an image, creates a container based on it and runs it
	resource, err := dp.pool.RunWithOptions(&dockertest.RunOptions{
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
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	_ = resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	uri = fmt.Sprintf("postgres://admin:secret@%s/basin?sslmode=disable", resource.GetHostPort("5432/tcp"))
	db, err = sql.Open("postgres", uri)
	if err != nil {
		log.Fatalf("Could not open the database: %s", err)
	}

	if err = dp.pool.Retry(func() error {
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE hba (lines text); 
		COPY hba FROM '/var/lib/postgresql/data/pg_hba.conf';
		INSERT INTO hba (lines) VALUES ('host  replication admin  172.17.0.1/32                 md5');
		COPY hba TO '/var/lib/postgresql/data/pg_hba.conf';
		SELECT pg_reload_conf();
	`); err != nil {
		log.Fatalf("Could not setup replication to docker: %s", err)
	}

	return
}

// Purge removes Postgres container.
func (dp *DockerPool) Purge(resource *dockertest.Resource) {
	if err := dp.pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
}
