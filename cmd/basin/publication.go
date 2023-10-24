package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"capnproto.org/go/capnp/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/schollz/progressbar/v3"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var pubNameRx = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)[.]([a-zA-Z_][a-zA-Z0-9_]*$)`)

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
			newPublicationUploadCommand(),
		},
	}
}

func newPublicationCreateCommand() *cli.Command {
	var owner, dburi, provider, winSize string

	return &cli.Command{
		Name:  "create",
		Usage: "create a new publication",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "address",
				Usage:       "Ethereum wallet address",
				Destination: &owner,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "dburi",
				Usage:       "PostgreSQL connection string",
				Destination: &dburi,
			},
			&cli.StringFlag{
				Name:        "provider",
				Usage:       "The provider's address and port (e.g. localhost:8080)",
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.StringFlag{
				Name:        "window-size",
				Usage:       "The size of the window for the WAL records",
				Destination: &winSize,
				Value:       fmt.Sprintf("%d", DefaultWindowSize),
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("one argument should be provided")
			}

			pub := cCtx.Args().First()
			ns, rel, err := parsePublicationName(pub)
			if err != nil {
				return err
			}

			if !common.IsHexAddress(owner) {
				return fmt.Errorf("%s is not a valid Ethereum wallet address", owner)
			}

			pgConfig, err := pgconn.ParseConfig(dburi)
			if err != nil {
				return fmt.Errorf("parse config: %s", err)
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			f, err := os.OpenFile(path.Join(dir, "config.yaml"), os.O_RDWR|os.O_CREATE, 0o666)
			if err != nil {
				return fmt.Errorf("os create: %s", err)
			}
			defer func() {
				_ = f.Close()
			}()

			cfg, err := loadConfig(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			winSizeInt, err := strconv.ParseInt(winSize, 10, 64)
			if err != nil {
				return fmt.Errorf("cannot parse window size: %s", err)
			}

			cfg.Publications[pub] = publication{
				Host:         pgConfig.Host,
				Port:         int(pgConfig.Port),
				User:         pgConfig.User,
				Password:     pgConfig.Password,
				Database:     pgConfig.Database,
				ProviderHost: provider,
				WindowSize:   int(winSizeInt),
			}

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			// This directory will contain the db files for the pub
			if err := os.Mkdir(path.Join(dir, pub), 0o755); err != nil {
				if os.IsExist(err) {
					fmt.Println("db directory already exists")
				} else {
					fmt.Println("Error:", err)
					return err
				}
			}

			exists, err := createPublication(cCtx.Context, dburi, ns, rel, provider, owner)
			if err != nil {
				return fmt.Errorf("failed to create publication: %s", err)
			}

			if exists {
				fmt.Printf("Publication %s.%s already exists.\n\n", ns, rel)
				return nil
			}

			fmt.Printf("\033[32mPublication %s.%s created.\033[0m\n\n", ns, rel)
			return nil
		},
	}
}

func newPublicationStartCommand() *cli.Command {
	var privateKey string

	return &cli.Command{
		Name:  "start",
		Usage: "start a daemon process that replicates Postgres changes to Basin server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Usage:       "Ethereum wallet private key",
				Destination: &privateKey,
				Required:    true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("one argument should be provided")
			}

			publication := cCtx.Args().First()
			ns, rel, err := parsePublicationName(publication)
			if err != nil {
				return err
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfig(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
				cfg.Publications[publication].User,
				cfg.Publications[publication].Password,
				cfg.Publications[publication].Host,
				cfg.Publications[publication].Port,
				cfg.Publications[publication].Database,
			)

			r, err := pgrepl.New(connString, pgrepl.Publication(rel))
			if err != nil {
				return fmt.Errorf("failed to create replicator: %s", err)
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			bp, err := basinprovider.New(cCtx.Context, cfg.Publications[publication].ProviderHost)
			if err != nil {
				return err
			}
			defer bp.Close()

			pgxConn, err := pgx.Connect(cCtx.Context, connString)
			if err != nil {
				return fmt.Errorf("connect: %s", err)
			}
			defer func() {
				_ = pgxConn.Close(cCtx.Context)
			}()

			tx, err := pgxConn.Begin(cCtx.Context)
			if err != nil {
				return fmt.Errorf("failed to begin transaction")
			}
			defer func() {
				if err != nil {
					_ = tx.Rollback(cCtx.Context)
				}
			}()

			cols, err := insepectSourceTable(cCtx.Context, tx, rel)
			if err != nil {
				return fmt.Errorf("failed to inspect source table: %s", err)
			}

			// Creates a new db manager when replication starts
			dbDir := path.Join(dir, publication)

			// (todo): read from config
			// (todo): read from config
			uploadInterval := 5 * time.Second
			replaceThreshold := 2 * time.Second

			dbm := app.NewDBManager(dbDir, rel, cols, replaceThreshold)
			uploader := app.NewBasinUploader(ns, rel, bp, privateKey)
			upm := app.NewUploadManager(
				cCtx.Context, dbDir, rel, uploader, uploadInterval,
			)
			// Before starting the UploadManager, upload the current.db
			// if it exists in dbDir.
			if err := upm.Upload(`^current.db$`); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			// start uploader to run the background.
			upm.Start()
			defer upm.Stop()

			basinStreamer := app.NewBasinStreamer(ns, r, dbm)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}

func newPublicationUploadCommand() *cli.Command {
	var privateKey, publicationName string

	return &cli.Command{
		Name:  "upload",
		Usage: "upload a Parquet file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Usage:       "Ethereum wallet private key",
				Destination: &privateKey,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "Publication name",
				Destination: &publicationName,
				Required:    true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("one argument should be provided")
			}
			ns, rel, err := parsePublicationName(publicationName)
			if err != nil {
				return err
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			dir, err := defaultConfigLocation(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfig(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			bp, err := basinprovider.New(cCtx.Context, cfg.Publications[publicationName].ProviderHost)
			if err != nil {
				return err
			}
			defer bp.Close()

			filepath := cCtx.Args().First()

			f, err := os.Open(filepath)
			if err != nil {
				return fmt.Errorf("open file: %s", err)
			}
			defer func() {
				_ = f.Close()
			}()

			fi, err := f.Stat()
			if err != nil {
				return fmt.Errorf("fstat: %s", err)
			}

			bar := progressbar.DefaultBytes(
				fi.Size(),
				"Uploading file...",
			)

			basinStreamer := app.NewBasinUploader(ns, rel, bp, privateKey)
			if err := basinStreamer.Upload(cCtx.Context, filepath, bar); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			return nil
		},
	}
}

func parsePublicationName(name string) (ns string, rel string, err error) {
	match := pubNameRx.FindStringSubmatch(name)
	if len(match) != 3 {
		return "", "", errors.New("publication name must be of the form `namespace.relation_name` using only letters, numbers, and underscores (_), where `namespace` and `relation` do not start with a number") // nolint
	}
	ns = match[1]
	rel = match[2]
	return
}

func insepectSourceTable(ctx context.Context, tx pgx.Tx, rel string) ([]app.Column, error) {
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
		`, rel,
	)
	if err != nil {
		return []app.Column{}, fmt.Errorf("failed to fetch schema")
	}
	defer rows.Close()

	var colName, typ string
	var isNull, isPrimary bool
	var columns []app.Column
	for rows.Next() {
		if err := rows.Scan(&colName, &typ, &isNull, &isPrimary); err != nil {
			return []app.Column{}, fmt.Errorf("scan: %s", err)
		}

		columns = append(columns, app.Column{
			Name:      colName,
			Typ:       typ,
			IsNull:    isNull,
			IsPrimary: isPrimary,
		})
	}
	return columns, nil
}

func createPublication(
	ctx context.Context, dburi string, ns string, rel string, provider string, owner string,
) (exists bool, err error) {
	bp, err := basinprovider.New(ctx, provider)
	if err != nil {
		return false, err
	}
	defer bp.Close()

	if dburi == "" {
		exists, err := bp.Create(ctx, ns, rel, basincapnp.Schema{}, common.HexToAddress(owner))
		if err != nil {
			return false, fmt.Errorf("create call: %s", err)
		}

		return exists, nil
	}

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

	columns, err := insepectSourceTable(ctx, tx, rel)

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

		_ = column.SetName(col.Name)
		_ = column.SetType(col.Typ)
		column.SetIsNullable(col.IsNull)
		column.SetIsPartOfPrimaryKey(col.IsPrimary)
	}
	_ = capnpSchema.SetColumns(columnsList)

	if _, err := tx.Exec(
		ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgrepl.Publication(rel).FullName(), rel),
	); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return true, nil
		}
		return false, fmt.Errorf("failed to create publication: %s", err)
	}

	if _, err := bp.Create(ctx, ns, rel, capnpSchema, common.HexToAddress(owner)); err != nil {
		return false, fmt.Errorf("create call: %s", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %s", err)
	}

	return false, nil
}
