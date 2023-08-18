package main

import (
	"fmt"
	"log"
	"path"

	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
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

			bp := basinprovider.New(basinprovider.BasinProviderClient_ServerToClient(&basinprovider.BasinServerMock{}))
			basinStreamer := app.NewBasinStreamer(r, bp)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				log.Fatal(err)
			}

			return nil
		},
	}
}
