package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/olekukonko/tablewriter"
	"github.com/schollz/progressbar/v3"
	"github.com/tablelandnetwork/basin-cli/internal/app"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
	"github.com/tablelandnetwork/basin-cli/pkg/signing"
	"github.com/tablelandnetwork/basin-cli/pkg/vaultsprovider"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var vaultNameRx = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)[.]([a-zA-Z_][a-zA-Z0-9_]*$)`)

func newVaultCreateCommand() *cli.Command {
	var address, dburi, provider string
	var winSize, cache int64

	return &cli.Command{
		Name:      "create",
		Usage:     "Create a new vault",
		ArgsUsage: "<vault_name>",
		Description: "Create a vault for a given account's address as either database streaming \n" +
			"or file uploading. Optionally, also set a cache duration for the data.\n\nEXAMPLE:\n\n" +
			"vaults create --account 0x1234abcd --cache 10 my.vault",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "account",
				Aliases:     []string{"a"},
				Category:    "REQUIRED:",
				Usage:       "Ethereum wallet address",
				Destination: &address,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider",
				Aliases:     []string{"p"},
				Category:    "OPTIONAL:",
				Usage:       "The provider's address and port (e.g., localhost:8080)",
				DefaultText: DefaultProviderHost,
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.Int64Flag{
				Name:        "cache",
				Category:    "OPTIONAL:",
				Usage:       "Time duration (in minutes) that the data will be available in the cache",
				DefaultText: "0",
				Destination: &cache,
				Value:       0,
			},
			&cli.StringFlag{
				Name:        "dburi",
				Category:    "OPTIONAL:",
				Usage:       "PostgreSQL connection string (e.g., postgresql://postgres:[PASSWORD]@[HOST]:[PORT]/postgres)",
				Destination: &dburi,
			},
			&cli.Int64Flag{
				Name:        "window-size",
				Category:    "OPTIONAL:",
				Usage:       "Number of seconds for which WAL updates are buffered before being sent to the provider",
				DefaultText: fmt.Sprintf("%d", DefaultWindowSize),
				Destination: &winSize,
				Value:       DefaultWindowSize,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("must provide a vault name")
			}

			pub := cCtx.Args().First()
			ns, rel, err := parseVaultName(pub)
			if err != nil {
				return err
			}

			account, err := app.NewAccount(address)
			if err != nil {
				return fmt.Errorf("not a valid account: %s", err)
			}
			pgConfig, err := pgconn.ParseConfig(dburi)
			if err != nil {
				return fmt.Errorf("parse config: %s", err)
			}

			dir, _, err := defaultConfigLocationV2(cCtx.String("dir"))
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

			cfg, err := loadConfigV2(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			cfg.Vaults[pub] = vault{
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

			exists, err := createVault(cCtx.Context, dburi, ns, rel, provider, account, cache)
			if err != nil {
				return fmt.Errorf("failed to create vault: %s", err)
			}

			if exists {
				fmt.Printf("Vault %s.%s already exists.\n\n", ns, rel)
				return nil
			}

			if err := os.MkdirAll(path.Join(dir, pub), 0o755); err != nil {
				return fmt.Errorf("mk db dir: %s", err)
			}

			fmt.Printf("\033[32mVault %s.%s created.\033[0m\n\n", ns, rel)
			return nil
		},
	}
}

func newStreamCommand() *cli.Command {
	var privateKey string

	return &cli.Command{
		Name:      "stream",
		Usage:     "Starts a daemon process that streams Postgres changes to a vault",
		ArgsUsage: "<vault_name>",
		Description: "The daemon will continuously stream database changes (except deletions) \n" +
			"to the vault, as long as the daemon is actively running.\n\n" +
			"EXAMPLE:\n\nvaults stream --private-key 0x1234abcd my.vault",

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Aliases:     []string{"k"},
				Category:    "REQUIRED:",
				Usage:       "Ethereum wallet private key",
				Destination: &privateKey,
				Required:    true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("must provide a vault name")
			}

			vault := cCtx.Args().First()
			ns, rel, err := parseVaultName(vault)
			if err != nil {
				return err
			}

			dir, _, err := defaultConfigLocationV2(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfigV2(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
				cfg.Vaults[vault].User,
				cfg.Vaults[vault].Password,
				cfg.Vaults[vault].Host,
				cfg.Vaults[vault].Port,
				cfg.Vaults[vault].Database,
			)

			r, err := pgrepl.New(connString, pgrepl.Publication(rel))
			if err != nil {
				return fmt.Errorf("failed to create replicator: %s", err)
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			bp := vaultsprovider.New(cfg.Vaults[vault].ProviderHost)

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
			dbDir := path.Join(dir, vault)
			winSize := time.Duration(cfg.Vaults[vault].WindowSize) * time.Second
			uploader := app.NewVaultsUploader(ns, rel, bp, privateKey)
			dbm := app.NewDBManager(dbDir, rel, cols, winSize, uploader)

			// Before starting replication, upload the remaining data
			if err := dbm.UploadAll(cCtx.Context); err != nil {
				return fmt.Errorf("upload all: %s", err)
			}

			vaultsStreamer := app.NewVaultsStreamer(ns, r, dbm)
			if err := vaultsStreamer.Run(cCtx.Context); err != nil {
				return fmt.Errorf("run: %s", err)
			}

			return nil
		},
	}
}

func newWriteCommand() *cli.Command {
	var privateKey, vaultName string
	var timestamp string

	return &cli.Command{
		Name:      "write",
		Usage:     "Write a Parquet file",
		ArgsUsage: "<file_path>",
		Description: "A Parquet file can be pushed directly to the vault, as an \n" +
			"alternative to continuous Postgres data streaming.\n\n" +
			"EXAMPLE:\n\nvaults write --vault my.vault --private-key 0x1234abcd /path/to/file.parquet",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Aliases:     []string{"k"},
				Category:    "REQUIRED:",
				Usage:       "Ethereum wallet private key",
				Destination: &privateKey,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "vault",
				Aliases:     []string{"v"},
				Category:    "REQUIRED:",
				Usage:       "Vault name",
				Destination: &vaultName,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "timestamp",
				Category:    "OPTIONAL:",
				Usage:       "The time the file was created",
				DefaultText: "current epoch in UTC",
				Destination: &timestamp,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("must provide a file path")
			}
			ns, rel, err := parseVaultName(vaultName)
			if err != nil {
				return err
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			dir, _, err := defaultConfigLocationV2(cCtx.String("dir"))
			if err != nil {
				return fmt.Errorf("default config location: %s", err)
			}

			cfg, err := loadConfigV2(path.Join(dir, "config.yaml"))
			if err != nil {
				return fmt.Errorf("load config: %s", err)
			}

			bp := vaultsprovider.New(cfg.Vaults[vaultName].ProviderHost)

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
				"Writing...",
			)

			if timestamp == "" {
				timestamp = fmt.Sprint(time.Now().UTC().Unix())
			}

			ts, err := app.ParseTimestamp(timestamp)
			if err != nil {
				return err
			}

			vaultsStreamer := app.NewVaultsUploader(ns, rel, bp, privateKey)
			if err := vaultsStreamer.Upload(cCtx.Context, filepath, bar, ts, fi.Size()); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			return nil
		},
	}
}

func newListCommand() *cli.Command {
	var address, provider, format string

	return &cli.Command{
		Name:  "list",
		Usage: "List vaults of a given account",
		Description: "Listing vaults will show all vaults that have been created by the provided \n" +
			"account's address and logged as either line delimited text or a json array.\n\n" +
			"EXAMPLE:\n\nvaults list --account 0x1234abcd --format json",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "account",
				Aliases:     []string{"a"},
				Category:    "REQUIRED:",
				Usage:       "Ethereum wallet address",
				Destination: &address,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider",
				Aliases:     []string{"p"},
				Category:    "OPTIONAL:",
				Usage:       "The provider's address and port (e.g., localhost:8080)",
				DefaultText: DefaultProviderHost,
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.StringFlag{
				Name:        "format",
				Category:    "OPTIONAL:",
				Usage:       "The output format (text or json)",
				DefaultText: "text",
				Destination: &format,
				Value:       "text",
			},
		},
		Action: func(cCtx *cli.Context) error {
			account, err := app.NewAccount(address)
			if err != nil {
				return fmt.Errorf("%s is not a valid Ethereum wallet address", address)
			}

			bp := vaultsprovider.New(provider)
			vaults, err := bp.ListVaults(cCtx.Context, app.ListVaultsParams{Account: account})
			if err != nil {
				return fmt.Errorf("failed to list vaults: %s", err)
			}

			if format == "text" {
				for _, vault := range vaults {
					fmt.Printf("%s\n", vault)
				}
			} else if format == "json" {
				jsonData, err := json.Marshal(vaults)
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

func newListEventsCommand() *cli.Command {
	var vault, provider, before, after, at, format string
	var limit, offset, latest int

	return &cli.Command{
		Name:      "events",
		Usage:     "List events of a given vault",
		UsageText: "vaults events [command options]",
		Description: "Vault events can be filtered by date ranges (unix, ISO 8601 date,\n" +
			"or ISO 8601 date & time), returning the event metadata and \n" +
			"corresponding CID.\n\n" +
			"EXAMPLE:\n\nvaults events --vault my.vault \\\n" +
			"--limit 10 --offset 3 \\\n--after 2023-09-01 --before 2023-12-01 \\\n" +
			"--format json",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "vault",
				Aliases:     []string{"v"},
				Category:    "REQUIRED:",
				Usage:       "Vault name",
				Destination: &vault,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider",
				Aliases:     []string{"p"},
				Category:    "OPTIONAL:",
				Usage:       "The provider's address and port (e.g., localhost:8080)",
				DefaultText: DefaultProviderHost,
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.IntFlag{
				Name:        "limit",
				Category:    "OPTIONAL:",
				Usage:       "The number of deals to fetch",
				DefaultText: "10",
				Destination: &limit,
				Value:       10,
			},
			&cli.IntFlag{
				Name:        "latest",
				Category:    "OPTIONAL:",
				Usage:       "The latest N deals to fetch",
				Destination: &latest,
			},
			&cli.IntFlag{
				Name:        "offset",
				Category:    "OPTIONAL:",
				Usage:       "The epoch to start from",
				DefaultText: "0",
				Destination: &offset,
				Value:       0,
			},
			&cli.StringFlag{
				Name:        "before",
				Category:    "OPTIONAL:",
				Usage:       "Filter deals created before this timestamp",
				Destination: &before,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "after",
				Category:    "OPTIONAL:",
				Usage:       "Filter deals created after this timestamp",
				Destination: &after,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "at",
				Category:    "OPTIONAL:",
				Usage:       "Filter deals created at this timestamp",
				Destination: &at,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "format",
				Category:    "OPTIONAL:",
				Usage:       "The output format (table or json)",
				DefaultText: "table",
				Destination: &format,
				Value:       "table",
			},
		},
		Action: func(cCtx *cli.Context) error {
			ns, rel, err := parseVaultName(vault)
			if err != nil {
				return err
			}

			bp := vaultsprovider.New(provider)

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
				table.SetHeader([]string{"CID", "Timestamp", "Archived", "Cache Expiry"})

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
						event.CID, timestamp, isArchived, event.CacheExpiry,
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

func newSignCommand() *cli.Command {
	var privateKey string

	return &cli.Command{
		Name:      "sign",
		Usage:     "Sign a file with a private key",
		ArgsUsage: "<file_path>",
		Description: "Signing a file with take a provide key and a path to the desired file\n" +
			"to produce a hex encoded string (e.g., can be used in the HTTP API).\n\n" +
			"EXAMPLE:\n\nvaults sign --private-key 0x1234abcd /path/to/file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "private-key",
				Aliases:     []string{"k"},
				Category:    "REQUIRED:",
				Usage:       "Ethereum wallet private key",
				Destination: &privateKey,
				Required:    true,
			},
		},
		Action: func(cCtx *cli.Context) error {
			if cCtx.NArg() != 1 {
				return errors.New("must provide a file path")
			}
			filepath := cCtx.Args().First()

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				return err
			}

			signer := signing.NewSigner(privateKey)
			signatureBytes, err := signer.SignFile(filepath)
			if err != nil {
				return fmt.Errorf("failed to sign file: %s", err)
			}
			signature := signing.SignatureBytesToHex(signatureBytes)
			fmt.Println(signature)

			return nil
		},
	}
}

func newRetrieveCommand() *cli.Command {
	var output, provider string
	var timeout int64

	return &cli.Command{
		Name:      "retrieve",
		Usage:     "Retrieve an event by CID",
		ArgsUsage: "<event_cid>",
		Description: "Retrieving an event will download the event's CAR file into the \n" +
			"specified file or to stdout.\n\n" +
			"EXAMPLE:\n\nvaults retrieve --output filename bafy...",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Category:    "OPTIONAL:",
				Usage:       "The file to store the retrieved content, or '-' for stdout",
				DefaultText: "stdout",
				Destination: &output,
			},
			&cli.StringFlag{
				Name:        "provider",
				Aliases:     []string{"p"},
				Category:    "OPTIONAL:",
				Usage:       "The provider's address and port (e.g., localhost:8080)",
				DefaultText: DefaultProviderHost,
				Destination: &provider,
				Value:       DefaultProviderHost,
			},
			&cli.Int64Flag{
				Name:        "timeout",
				Aliases:     []string{"t"},
				Category:    "OPTIONAL:",
				Usage:       "Timeout for retrieval operation (seconds)",
				DefaultText: "no timeout",
				Destination: &timeout,
				Value:       0,
			},
		},
		Action: func(cCtx *cli.Context) error {
			arg := cCtx.Args().Get(0)
			if arg == "" {
				return errors.New("must provide an event CID")
			}

			rootCid, err := cid.Parse(arg)
			if err != nil {
				return errors.New("CID is invalid")
			}

			retriever := app.NewRetriever(vaultsprovider.New(provider), timeout)
			if err := retriever.Retrieve(cCtx.Context, rootCid, output); err != nil {
				return fmt.Errorf("failed to retrieve: %s", err)
			}

			return nil
		},
	}
}

func newWalletCommand() *cli.Command {
	var pkString string

	return &cli.Command{
		Name:      "account",
		Usage:     "Account management for an Ethereum-style wallet",
		UsageText: "vaults account <subcommand> [arguments...]",
		Subcommands: []*cli.Command{
			{
				Name:      "create",
				Usage:     "Creates a new account",
				UsageText: "vaults account create <file_path>",
				Description: "Create an Ethereum-style wallet (secp256k1 key pair) at a \n" +
					"provided file path.\n\n" +
					"EXAMPLE:\n\nvaults account create /path/to/file",
				Action: func(cCtx *cli.Context) error {
					filename := cCtx.Args().Get(0)
					if filename == "" {
						return errors.New("filename is empty")
					}

					privateKey, err := crypto.GenerateKey()
					if err != nil {
						return fmt.Errorf("generate key: %s", err)
					}
					privateKeyBytes := crypto.FromECDSA(privateKey)

					if err := os.WriteFile(filename, []byte(hexutil.Encode(privateKeyBytes)[2:]), 0o644); err != nil {
						return fmt.Errorf("writing to file %s: %s", filename, err)
					}
					pubk, _ := privateKey.Public().(*ecdsa.PublicKey)
					publicKey := common.HexToAddress(crypto.PubkeyToAddress(*pubk).Hex())

					fmt.Printf("Wallet address %s created\n", publicKey)
					fmt.Printf("Private key saved in %s\n", filename)
					return nil
				},
			},
			{
				Name:      "address",
				Usage:     "Print the public key for an account's private key",
				UsageText: "vaults account address [command options] <value>",
				Description: "The result of the `vaults account create` command will write a private key to a file, and \n" +
					"this lets you retrieve the public key value for the file, or a private key hex string.\n" +
					"If no `--string` flag is provided, then the presumption is the argument is a filepath.\n\n" +
					"EXAMPLES:\n\nvaults account address /path/to/file\nvaults account address --string abcd1234",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "string",
						Category:    "OPTIONAL:",
						Usage:       "Specify if the argument is a hex string",
						Destination: &pkString,
					},
				},
				Action: func(cCtx *cli.Context) error {
					pkFile := cCtx.Args().Get(0)
					if pkFile == "" && pkString == "" {
						return errors.New("no argument provided")
					}

					var privateKey *ecdsa.PrivateKey
					var err error
					if pkString == "" {
						privateKey, err = crypto.LoadECDSA(pkFile)
					} else {
						privateKey, err = crypto.HexToECDSA(pkString)
					}
					if err != nil {
						return fmt.Errorf("loading key: %s", err)
					}

					pubk, _ := privateKey.Public().(*ecdsa.PublicKey)
					publicKey := common.HexToAddress(crypto.PubkeyToAddress(*pubk).Hex())

					fmt.Println(publicKey)
					return nil
				},
			},
		},
	}
}

func parseVaultName(name string) (ns string, rel string, err error) {
	match := vaultNameRx.FindStringSubmatch(name)
	if len(match) != 3 {
		return "", "", errors.New(
			"vault name must be of the form `namespace.relation_name` using only letters, numbers, " +
				"and underscores (_), where `namespace` and `relation` do not start with a number",
		) // nolint
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

func createVault(
	ctx context.Context,
	dburi string,
	ns string,
	rel string,
	provider string,
	account *app.Account,
	cacheDuration int64,
) (exists bool, err error) {
	bp := vaultsprovider.New(provider)
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
