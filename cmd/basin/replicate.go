package main

import (
	"encoding/json"
	"fmt"
	"log"
	"path"

	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/urfave/cli/v2"
)

func newReplicatecommand() *cli.Command {
	return &cli.Command{
		Name:  "replicate",
		Usage: "start a daemon process that replicates Postgres changes to Basin server",
		Flags: []cli.Flag{},
		Action: func(cCtx *cli.Context) error {
			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfig(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("setup config: %s", err)
			}

			connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
				cfg.DBS.Postgres.User,
				cfg.DBS.Postgres.Password,
				cfg.DBS.Postgres.Host,
				cfg.DBS.Postgres.Port,
				cfg.DBS.Postgres.Database,
			)

			r, err := pgrepl.New(connString, "basin")
			if err != nil {
				log.Fatal(err)
			}

			feed, _ := r.StartReplication(cCtx.Context)
			for tx := range feed {
				processTx(tx)
				_ = r.Commit(cCtx.Context, tx.CommitLSN)
			}

			return nil
		},
	}
}

func processTx(tx *pgrepl.Tx) {
	v, err := json.MarshalIndent(tx, "", " ")
	fmt.Println(string(v), err)
}
