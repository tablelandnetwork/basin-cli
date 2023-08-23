package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strings"

	"capnproto.org/go/capnp/v3/rpc"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"gopkg.in/yaml.v3"
)

func newPublicationCommand() *cli.Command {
	return &cli.Command{
		Name:  "publication",
		Usage: "publication commands",
		Subcommands: []*cli.Command{
			newPublicationCreateCommand(),
			newPublicationStartCommand(),
		},
	}
}

func newPublicationCreateCommand() *cli.Command {
	var address, dburi, provider string

	return &cli.Command{
		Name:  "create",
		Usage: "create a new publication",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "address",
				Usage:       "wallet address",
				Destination: &address,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "dburi",
				Usage:       "PostgreSQL connection string",
				Destination: &dburi,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider",
				Usage:       "The provider's address and port (e.g. localhost:8080)",
				Destination: &provider,
				Value:       DefaultProviderHost,
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

			pgConfig, err := pgconn.ParseConfig(dburi)
			if err != nil {
				return fmt.Errorf("parse config: %s", err)
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			f, err := os.Create(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("os create: %s", err)
			}

			cfg := config{}
			cfg.DBS.Postgres.Host = pgConfig.Host
			cfg.DBS.Postgres.Port = int(pgConfig.Port)
			cfg.DBS.Postgres.User = pgConfig.User
			cfg.DBS.Postgres.Password = pgConfig.Password
			cfg.DBS.Postgres.Database = pgConfig.Database
			cfg.ProviderHost = provider

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
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "publication name",
				Destination: &publicationName,
				Required:    true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			dir, err := defaultConfigLocation(cCtx.String("dir"))
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
			conn, err := net.Dial("tcp", cfg.ProviderHost)
			if err != nil {
				return fmt.Errorf("failed to connect to provider: %s", err)
			}

			rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
			defer func() {
				if err := rpcConn.Close(); err != nil {
					slog.Error(err.Error())
				}
			}()

			bp := basinprovider.New(basinprovider.BasinProviderClient(rpcConn.Bootstrap(cCtx.Context)))
			basinStreamer := app.NewBasinStreamer(r, bp, privateKey)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}
