package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/olekukonko/tablewriter"
	"github.com/schollz/progressbar/v3"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/basinprovider"
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
			newPublicationListCommand(),
			newPublicationDealsCommand(),
			newPublicationRetrieveCommand(),
		},
	}
}

func newPublicationCreateCommand() *cli.Command {
	var owner, dburi, provider string
	var winSize, cache int64

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
			&cli.Int64Flag{
				Name:        "window-size",
				Usage:       "Number of seconds for which WAL updates are buffered before being sent to the provider",
				Destination: &winSize,
				Value:       DefaultWindowSize,
			},
			&cli.Int64Flag{
				Name:        "cache",
				Usage:       "Time duration (in minutes) that the data will be available in the cache",
				Destination: &cache,
				Value:       0,
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

			cfg.Publications[pub] = publication{
				Host:         pgConfig.Host,
				Port:         int(pgConfig.Port),
				User:         pgConfig.User,
				Password:     pgConfig.Password,
				Database:     pgConfig.Database,
				ProviderHost: provider,
				WindowSize:   winSize,
			}

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			exists, err := createPublication(cCtx.Context, dburi, ns, rel, provider, owner, cache)
			if err != nil {
				return fmt.Errorf("failed to create publication: %s", err)
			}

			if exists {
				fmt.Printf("Publication %s.%s already exists.\n\n", ns, rel)
				return nil
			}

			if err := os.MkdirAll(path.Join(dir, pub), 0o755); err != nil {
				return fmt.Errorf("mk db dir: %s", err)
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

			bp := basinprovider.New(cfg.Publications[publication].ProviderHost)

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

			cols, err := inspectTable(cCtx.Context, tx, rel)
			if err != nil {
				return fmt.Errorf("failed to inspect source table: %s", err)
			}

			// Creates a new db manager when replication starts
			dbDir := path.Join(dir, publication)
			winSize := time.Duration(cfg.Publications[publication].WindowSize) * time.Second
			uploader := app.NewBasinUploader(ns, rel, bp, privateKey)
			dbm := app.NewDBManager(dbDir, rel, cols, winSize, uploader)

			// Before starting replication, upload the remaining data
			if err := dbm.UploadAll(cCtx.Context); err != nil {
				return fmt.Errorf("upload all: %s", err)
			}

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
	var timestamp string

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
			&cli.StringFlag{
				Name:        "timestamp",
				Usage:       "The time the file was created (default: current epoch in UTC)",
				Destination: &timestamp,
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

			bp := basinprovider.New(cfg.Publications[publicationName].ProviderHost)

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

			if timestamp == "" {
				timestamp = fmt.Sprint(time.Now().UTC().Unix())
			}

			ts, err := app.ParseTimestamp(timestamp)
			if err != nil {
				return err
			}

			basinStreamer := app.NewBasinUploader(ns, rel, bp, privateKey)
			if err := basinStreamer.Upload(cCtx.Context, filepath, bar, ts, fi.Size()); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			return nil
		},
	}
}

func newPublicationListCommand() *cli.Command {
	var owner, provider string

	return &cli.Command{
		Name:  "list",
		Usage: "list publications of a given address",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "address",
				Usage:       "Ethereum wallet address",
				Destination: &owner,
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
			account, err := app.NewAccount(owner)
			if err != nil {
				return fmt.Errorf("%s is not a valid Ethereum wallet address", owner)
			}

			bp := basinprovider.New(provider)
			vaults, err := bp.ListVaults(cCtx.Context, app.ListVaultsParams{Account: account})
			if err != nil {
				return fmt.Errorf("failed to list publications: %s", err)
			}

			for _, vault := range vaults {
				fmt.Printf("%s\n", vault)
			}

			return nil
		},
	}
}

func newPublicationDealsCommand() *cli.Command {
	var publication, provider, before, after, at, format string
	var limit, offset, latest int

	return &cli.Command{
		Name:  "deals",
		Usage: "list deals of a given publications",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "publication",
				Usage:       "Publication name",
				Destination: &publication,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider",
				Usage:       "The provider's address and port (e.g. localhost:8080)",
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.IntFlag{
				Name:        "limit",
				Usage:       "The number of deals to fetch",
				Destination: &limit,
				Value:       10,
			},
			&cli.IntFlag{
				Name:        "latest",
				Usage:       "The latest N deals to fetch",
				Destination: &latest,
			},
			&cli.IntFlag{
				Name:        "offset",
				Usage:       "The epoch to start from",
				Destination: &offset,
				Value:       0,
			},
			&cli.StringFlag{
				Name:        "before",
				Usage:       "Filter deals created before this timestamp",
				Destination: &before,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "after",
				Usage:       "Filter deals created after this timestamp",
				Destination: &after,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "at",
				Usage:       "Filter deals created at this timestamp",
				Destination: &at,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "format",
				Usage:       "The output format (table or json)",
				Destination: &format,
				Value:       "table",
			},
		},
		Action: func(cCtx *cli.Context) error {
			ns, rel, err := parsePublicationName(publication)
			if err != nil {
				return err
			}

			bp := basinprovider.New(provider)

			b, a, err := validateBeforeAndAfter(before, after, at)
			if err != nil {
				return err
			}

			var req app.ListVaultEventsParams
			if latest > 0 {
				req = app.ListVaultEventsParams{
					Vault:  app.Vault(fmt.Sprintf("%s.%s", ns, rel)),
					Limit:  uint32(latest),
					Offset: 0,
					Before: b,
					After:  a,
				}
			} else {
				if offset < 0 {
					return errors.New("offset has to be greater than 0")
				}

				if limit < 0 {
					return errors.New("limit has to be greater than 0")
				}

				req = app.ListVaultEventsParams{
					Vault:  app.Vault(fmt.Sprintf("%s.%s", ns, rel)),
					Limit:  uint32(limit),
					Offset: uint32(offset),
					Before: b,
					After:  a,
				}
			}

			events, err := bp.ListVaultEvents(cCtx.Context, req)
			if err != nil {
				return fmt.Errorf("failed to fetch deals: %s", err)
			}

			if format == "table" {
				table := tablewriter.NewWriter(os.Stdout)
				table.SetHeader([]string{"CID", "Size", "Timestamp", "Archived", "Cache Expiry"})

				for _, event := range events {
					isArchived := "N"
					if event.IsArchived {
						isArchived = "Y"
					}
					timestamp := "(null)"
					if event.Timestamp > 0 {
						timestamp = time.Unix(event.Timestamp, 0).Format(time.RFC3339)
					}
					table.Append([]string{
						event.CID, fmt.Sprintf("%d", event.Size), timestamp, isArchived, event.CacheExpiry,
					})
				}
				table.Render()
			} else if format == "json" {
				jsonData, err := json.Marshal(events)
				if err != nil {
					return fmt.Errorf("error serializing events to JSON")
				}
				fmt.Println(string(jsonData))
			} else {
				return fmt.Errorf("invalid format: %s", format)
			}
			return nil
		},
	}
}

func newPublicationRetrieveCommand() *cli.Command {
	return &cli.Command{
		Name:  "retrieve",
		Usage: "Retrieve files by CID",
		Action: func(cCtx *cli.Context) error {
			arg := cCtx.Args().Get(0)
			if arg == "" {
				return errors.New("argument is empty")
			}

			rootCid, err := cid.Parse(arg)
			if err != nil {
				return errors.New("cid is invalid")
			}

			lassie, err := lassie.NewLassie(cCtx.Context)
			if err != nil {
				return fmt.Errorf("failed to create lassie instance: %s", err)
			}

			carOpts := []car.Option{
				car.WriteAsCarV1(true),
				car.StoreIdentityCIDs(false),
				car.UseWholeCIDs(false),
			}
			carWriter := deferred.NewDeferredCarWriterForPath(fmt.Sprintf("./%s.car", arg), []cid.Cid{rootCid}, carOpts...)
			defer func() {
				_ = carWriter.Close()
			}()
			carStore := storage.NewCachingTempStore(
				carWriter.BlockWriteOpener(), storage.NewDeferredStorageCar(os.TempDir(), rootCid),
			)
			defer func() {
				_ = carStore.Close()
			}()

			request, err := types.NewRequestForPath(carStore, rootCid, "", trustlessutils.DagScopeAll, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %s", err)
			}

			if _, err := lassie.Fetch(cCtx.Context, request, []types.FetchOption{}...); err != nil {
				return fmt.Errorf("failed to fetch: %s", err)
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

func inspectTable(ctx context.Context, tx pgx.Tx, rel string) ([]app.Column, error) {
	rows, err := tx.Query(ctx,
		`
		WITH primary_key_info AS
			(SELECT tc.constraint_schema,
					tc.table_name,
					ccu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.constraint_column_usage AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
			WHERE constraint_type = 'PRIMARY KEY' ),
			array_type_info AS
				(SELECT c.table_name,
						c.column_name,
						pg_catalog.format_type(t.oid, NULL) AS full_data_type
				FROM information_schema.columns AS c
				JOIN pg_catalog.pg_type AS t ON c.udt_name = t.typname
				WHERE c.data_type = 'ARRAY')
		SELECT
			c.column_name,
			CASE
			WHEN c.data_type = 'ARRAY' THEN ati.full_data_type
			ELSE c.data_type
			END AS data_type,
			c.is_nullable = 'YES' AS is_nullable,
			pki.column_name IS NOT NULL AS is_primary
		FROM information_schema.columns AS c
		LEFT JOIN primary_key_info pki ON c.table_schema = pki.constraint_schema
			AND pki.table_name = c.table_name
			AND pki.column_name = c.column_name
		LEFT JOIN array_type_info ati ON c.table_name = ati.table_name
    		AND c.column_name = ati.column_name
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
	ctx context.Context,
	dburi string,
	ns string,
	rel string,
	provider string,
	owner string,
	cacheDuration int64,
) (exists bool, err error) {
	account, err := app.NewAccount(owner)
	if err != nil {
		return false, fmt.Errorf("not a valid account: %s", err)
	}

	bp := basinprovider.New(provider)
	req := app.CreateVaultParams{
		Account:       account,
		Vault:         app.Vault(fmt.Sprintf("%s.%s", ns, rel)),
		CacheDuration: app.CacheDuration(cacheDuration),
	}

	if dburi == "" {
		if err := bp.CreateVault(ctx, req); err != nil {
			return false, fmt.Errorf("create vault: %s", err)
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

	if _, err := tx.Exec(
		ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgrepl.Publication(rel).FullName(), rel),
	); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return true, nil
		}
		return false, fmt.Errorf("failed to create publication: %s", err)
	}

	if err := bp.CreateVault(ctx, req); err != nil {
		return false, fmt.Errorf("create call: %s", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %s", err)
	}

	return false, nil
}

func validateBeforeAndAfter(before, after, at string) (app.Timestamp, app.Timestamp, error) {
	if !strings.EqualFold(at, "") {
		before, after = at, at
	}

	b, err := app.ParseTimestamp(before)
	if err != nil {
		return app.Timestamp{}, app.Timestamp{}, err
	}

	a, err := app.ParseTimestamp(after)
	if err != nil {
		return app.Timestamp{}, app.Timestamp{}, err
	}

	return b, a, nil
}
