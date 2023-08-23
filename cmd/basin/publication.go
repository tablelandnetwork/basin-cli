package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

func newPublication() *cli.Command {
	return &cli.Command{
		Name:  "publication",
		Usage: "publication commands",
		Subcommands: []*cli.Command{
			newPublicationCreate(),
			newPublicationStartCommand(),
		},
	}
}

func newPublicationCreate() *cli.Command {
	var address, dburi string

	return &cli.Command{
		Name:  "create",
		Usage: "create a new publication",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "address",
				Usage:       "wallet address",
				Destination: &address,
			},
			&cli.StringFlag{
				Name:        "dburi",
				Usage:       "PostgreSQL connection string",
				Destination: &dburi,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("one argument should be provided")
			}
			name := cCtx.Args().First()
			if name == "" {
				return errors.New("name is empty")
			}

			if dburi == "" {
				return errors.New("dburi is empty")
			}

			if address == "" {
				return errors.New("address is empty")
			}

			pgConfig, err := pgconn.ParseConfig(dburi)
			if err != nil {
				return fmt.Errorf("parse config: %s", err)
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg := config{}
			f, err := os.Create(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("os create: %s", err)
			}

			cfg.DBS.Postgres.Host = pgConfig.Host
			cfg.DBS.Postgres.Port = int(pgConfig.Port)
			cfg.DBS.Postgres.User = pgConfig.User
			cfg.DBS.Postgres.Password = pgConfig.Password
			cfg.DBS.Postgres.Database = pgConfig.Database

			// TODO: figure out what to do with address
			cfg.Address = address

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			// Connect to the database
			pgxConn, err := pgx.Connect(cCtx.Context, dburi)
			if err != nil {
				return fmt.Errorf("connect: %s", err)
			}
			defer func() {
				_ = pgxConn.Close(cCtx.Context)
			}()

			if _, err := pgxConn.Exec(
				cCtx.Context, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgrepl.Publication(name).FullName(), name),
			); err != nil {
				if strings.Contains(err.Error(), "already exists") {
					fmt.Printf("Publication %s already exists.\n\n", name)
					return nil
				}
				return fmt.Errorf("failed to create publication: %s", err)
			}

			fmt.Printf("\033[32mPublication %s created.\033[0m\n\n", name)
			return nil
		},
	}
}

func newPublicationStartCommand() *cli.Command {
	var privateKey, publicationName string

	return &cli.Command{
		Name:  "start",
		Usage: "start a daemon process that replicates Postgres changes to Basin server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Usage:       "wallet private key",
				Destination: &privateKey,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "publication name",
				Destination: &publicationName,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if privateKey == "" {
				return errors.New("private key is empty")
			}

			if publicationName == "" {
				return errors.New("publication name is empty")
			}

			dir, err := defaultConfigLocation("")
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfig(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
				cfg.DBS.Postgres.User,
				cfg.DBS.Postgres.Password,
				cfg.DBS.Postgres.Host,
				cfg.DBS.Postgres.Port,
				cfg.DBS.Postgres.Database,
			)

			r, err := pgrepl.New(connString, pgrepl.Publication(publicationName))
			if err != nil {
				return fmt.Errorf("failed to create replicator: %s", err)
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			bp := basinprovider.New(basinprovider.BasinProviderClient_ServerToClient(&basinprovider.BasinServerMock{}))
			basinStreamer := app.NewBasinStreamer(r, bp, privateKey)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}
