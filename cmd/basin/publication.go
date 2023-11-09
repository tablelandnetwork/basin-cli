package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"capnproto.org/go/capnp/v3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/olekukonko/tablewriter"
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
			newPublicationListCommand(),
			newPublicationDealsCommand(),
		},
	}
}

func newPublicationCreateCommand() *cli.Command {
	var owner, dburi, provider string
	var secure bool

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
			&cli.BoolFlag{
				Name:        "secure",
				Usage:       "Uses TLS connection",
				Destination: &secure,
				Value:       true,
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
			}

			if err := yaml.NewEncoder(f).Encode(cfg); err != nil {
				return fmt.Errorf("encode: %s", err)
			}

			exists, err := createPublication(cCtx.Context, dburi, ns, rel, provider, owner, secure)
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
	var secure bool

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
			&cli.BoolFlag{
				Name:        "secure",
				Usage:       "Uses TLS connection",
				Destination: &secure,
				Value:       true,
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

			bp, err := basinprovider.New(cCtx.Context, cfg.Publications[publication].ProviderHost, secure)
			if err != nil {
				return err
			}
			defer bp.Close()

			basinStreamer := app.NewBasinStreamer(ns, r, bp, privateKey)
			if err := basinStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}

func newPublicationUploadCommand() *cli.Command {
	var privateKey, publicationName string
	var secure bool
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
			&cli.BoolFlag{
				Name:        "secure",
				Usage:       "Uses TLS connection",
				Destination: &secure,
				Value:       true,
			},
			&cli.StringFlag{
				Name:        "timestamp",
				Usage:       "The time the file was created",
				Destination: &timestamp,
				Value:       fmt.Sprint(time.Now().UTC().Unix()),
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

			bp, err := basinprovider.New(cCtx.Context, cfg.Publications[publicationName].ProviderHost, secure)
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

			ts, err := app.ParseTimestamp(timestamp)
			if err != nil {
				return err
			}

			basinStreamer := app.NewBasinUploader(ns, rel, bp, privateKey, ts)
			if err := basinStreamer.Upload(cCtx.Context, filepath, bar); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			return nil
		},
	}
}

func newPublicationListCommand() *cli.Command {
	var owner, provider string
	var secure bool

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
			&cli.BoolFlag{
				Name:        "secure",
				Usage:       "Uses TLS connection",
				Destination: &secure,
				Value:       true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if !common.IsHexAddress(owner) {
				return fmt.Errorf("%s is not a valid Ethereum wallet address", owner)
			}

			bp, err := basinprovider.New(cCtx.Context, provider, secure)
			if err != nil {
				return fmt.Errorf("new basin provider: %s", err)
			}
			defer bp.Close()

			publications, err := bp.List(cCtx.Context, common.HexToAddress(owner))
			if err != nil {
				return fmt.Errorf("failed to list publications: %s", err)
			}

			for _, pub := range publications {
				fmt.Printf("%s\n", pub)
			}

			return nil
		},
	}
}

func newPublicationDealsCommand() *cli.Command {
	var publication, provider, before, after, at string
	var limit, latest int
	var offset int64
	var secure bool

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
			&cli.Int64Flag{
				Name:        "offset",
				Usage:       "The epoch to start from",
				Destination: &offset,
				Value:       0,
			},
			&cli.BoolFlag{
				Name:        "secure",
				Usage:       "Uses TLS connection",
				Destination: &secure,
				Value:       true,
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
		},
		Action: func(cCtx *cli.Context) error {
			ns, rel, err := parsePublicationName(publication)
			if err != nil {
				return err
			}

			bp, err := basinprovider.New(cCtx.Context, provider, secure)
			if err != nil {
				return fmt.Errorf("new basin provider: %s", err)
			}
			defer bp.Close()

			b, a, err := validateBeforeAndAfter(before, after, at)
			if err != nil {
				return err
			}

			var deals []app.DealInfo
			if latest > 0 {
				deals, err = bp.LatestDeals(cCtx.Context, ns, rel, uint32(latest), b, a)
				if err != nil {
					return fmt.Errorf("failed to fetch deals: %s", err)
				}
			} else {
				if offset < 0 {
					return errors.New("offset has to be greater than 0")
				}

				if limit < 0 {
					return errors.New("limit has to be greater than 0")
				}

				deals, err = bp.Deals(cCtx.Context, ns, rel, uint32(limit), uint64(offset), b, a)
				if err != nil {
					return fmt.Errorf("failed to fetch deals: %s", err)
				}
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"CID", "Size", "Timestamp", "Archived"})

			for _, deal := range deals {
				isArchived := "N"
				if deal.IsArchived {
					isArchived = "Y"
				}
				table.Append([]string{
					deal.CID, fmt.Sprintf("%d", deal.Size), time.Unix(deal.Timestamp, 0).Format(time.RFC3339), isArchived,
				})
			}
			table.Render()

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

func createPublication(
	ctx context.Context, dburi string, ns string, rel string, provider string, owner string, secure bool,
) (exists bool, err error) {
	bp, err := basinprovider.New(ctx, provider, secure)
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
		return false, fmt.Errorf("failed to fetch schema")
	}
	defer rows.Close()

	type column struct {
		name, typ         string
		isNull, isPrimary bool
	}

	var colName, typ string
	var isNull, isPrimary bool
	var columns []column
	for rows.Next() {
		if err := rows.Scan(&colName, &typ, &isNull, &isPrimary); err != nil {
			return false, fmt.Errorf("scan: %s", err)
		}

		columns = append(columns, column{
			name:      colName,
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
