package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strings"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"gopkg.in/yaml.v3"
)

func newPublicationCommand() *cli.Command {
	return &cli.Command{
		Name:  "publication",
		Usage: "publication commands",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dir",
				Usage: "The directory where config will be stored (default: $HOME)",
			},
		},
		Subcommands: []*cli.Command{
			newPublicationCreateCommand(),
			newPublicationStartCommand(),
		},
	}
}

func newPublicationCreateCommand() *cli.Command {
	var owner, dburi, provider string

	return &cli.Command{
		Name:  "create",
		Usage: "create a new publication",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "address",
				Usage:       "wallet address",
				Destination: &owner,
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

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			exists, err := createPublication(cCtx.Context, dburi, name, provider, owner)
			if err != nil {
				return fmt.Errorf("failed to create publication: %s", err)
			}

			if exists {
				fmt.Printf("Publication %s already exists.\n\n", name)
				return nil
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

			client, err := getBasinClient(cCtx.Context, cfg.ProviderHost)
			if err != nil {
				return err
			}

			bp := basinprovider.New(client)
			basinStreamer := app.NewBasinStreamer(r, bp, privateKey)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}

func getBasinClient(ctx context.Context, provider string) (basinprovider.BasinProviderClient, error) {
	var client basinprovider.BasinProviderClient
	if provider == "mock" {
		client = basinprovider.BasinProviderClient_ServerToClient(basinprovider.NewBasinServerMock())
	} else {
		conn, err := net.Dial("tcp", provider)
		if err != nil {
			return basinprovider.BasinProviderClient{}, fmt.Errorf("failed to connect to provider: %s", err)
		}

		rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
		defer func() {
			if err := rpcConn.Close(); err != nil {
				slog.Error(err.Error())
			}
		}()

		client = basinprovider.BasinProviderClient(rpcConn.Bootstrap(ctx))
	}

	return client, nil
}

func createPublication(
	ctx context.Context, dburi string, name string, provider string, owner string,
) (exists bool, err error) {
	pgxConn, err := pgx.Connect(ctx, dburi)
	if err != nil {
		return false, fmt.Errorf("connect: %s", err)
	}
	defer func() {
		_ = pgxConn.Close(ctx)
	}()

	tx, err := pgxConn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction")
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	rows, err := tx.Query(ctx,
		`
		WITH primary_key_info AS
			(SELECT tc.constraint_schema,
					tc.table_name,
					ccu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.constraint_column_usage AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
			WHERE constraint_type = 'PRIMARY KEY' )
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable = 'YES' AS is_nullable,
			pki.column_name IS NOT NULL AS is_primary
		FROM information_schema.columns AS c
		LEFT JOIN primary_key_info pki ON c.table_schema = pki.constraint_schema
			AND pki.table_name = c.table_name
			AND pki.column_name = c.column_name
		WHERE c.table_name = $1; 
		`, name,
	)
	if err != nil {
		return false, fmt.Errorf("failed to fetch schema")
	}
	defer rows.Close()

	type column struct {
		name, typ         string
		isNull, isPrimary bool
	}

	var colName, typ string
	var isNull, isPrimary bool
	columns := []column{}
	for rows.Next() {
		if err := rows.Scan(&colName, &typ, &isNull, &isPrimary); err != nil {
			return false, fmt.Errorf("scan: %s", err)
		}

		columns = append(columns, column{
			name:      name,
			typ:       typ,
			isNull:    isNull,
			isPrimary: isPrimary,
		})
	}

	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return false, fmt.Errorf("capnp new message: %s", err)
	}

	capnpSchema, err := basincapnp.NewRootSchema(seg)
	if err != nil {
		return false, fmt.Errorf("capnp new tx: %s", err)
	}

	columnsList, err := basincapnp.NewSchema_Column_List(seg, int32(len(columns)))
	if err != nil {
		return false, fmt.Errorf("capnp new columns list: %s", err)
	}

	for i, col := range columns {
		column := columnsList.At(i)

		_ = column.SetName(col.name)
		_ = column.SetType(col.typ)
		column.SetIsNullable(col.isNull)
		column.SetIsPartOfPrimaryKey(col.isPrimary)
	}
	_ = capnpSchema.SetColumns(columnsList)

	if _, err := tx.Exec(
		ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgrepl.Publication(name).FullName(), name),
	); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return true, nil
		}
		return false, fmt.Errorf("failed to create publication: %s", err)
	}

	client, err := getBasinClient(ctx, provider)
	if err != nil {
		return false, err
	}

	bp := basinprovider.New(client)
	if err := bp.Create(ctx, name, common.HexToAddress(owner), capnpSchema); err != nil {
		return false, fmt.Errorf("create call: %s", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %s", err)
	}

	return false, nil
}
