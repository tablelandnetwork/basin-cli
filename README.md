# vaults-cli

[![License](https://img.shields.io/github/license/tablelandnetwork/basin-cli.svg)](./LICENSE)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Continuously publish data from your database or file uploads to the Tableland Vaults network.

## Table of Contents

- [Background](#background)
- [Usage](#usage)
  - [Install](#install)
  - [Postgres Setup](#postgres-setup)
  - [Supabase](#supabase)
  - [Create a vault](#create-a-vault)
  - [Start replicating a database](#start-replicating-a-database)
  - [Write a Parquet file](#write-a-parquet-file)
  - [Listing Vaults](#listing-vaults)
  - [Listing Events](#listing-events)
  - [Retrieving](#retrieving)
- [Development](#development)
  - [Running](#running)
  - [Run tests](#run-tests)
- [Contributing](#contributing)
- [License](#license)

## Background

Textile Vaults is a secure and verifiable open data platform. The Vaults CLI is a tool that allows you to continuously replicate a table or view from your database to the network (currently, only PostgreSQL is supported). Or, you can directly upload files to the vault (currently, parquet is only supported)

> 🚧 Vaults is currently not in a production-ready state. Any data that is pushed to the network may be subject to deletion. 🚧

## Usage

### Install

You can either install the CLI from the remote source:

```bash
go install github.com/tablelandnetwork/basin-cli/cmd/basin@latest
```

Or clone from source and run the Makefile `install` command:

```bash
git clone https://github.com/tablelandnetwork/basin-cli.git
cd basin-cli
make install
```

### Postgres Setup

#### Self-hosted

- Make sure you have access to a superuser role. For example, you can create a new role such as `CREATE ROLE vaults WITH PASSWORD NULL LOGIN SUPERUSER;`.
- Check that your Postgres installation has the [wal2json](https://github.com/eulerto/wal2json) plugin installed.
- Check if logical replication is enabled:

  ```sql
  SHOW wal_level;
  ```

  The `wal_level` setting must be set to logical: `ALTER SYSTEM SET wal_level = logical;`.

- Restart the database in order for the new `wal_level` to take effect (be careful!).

#### Amazon RDS

- Make sure you have a user with the `rds_superuser` role, and use `psql` to connect to your database.

  ```console
  psql -h [HOST] -U [USER] -d [DATABASE]
  ```

- Check if logical replication is enabled:

  ```sql
  SELECT name, setting
  FROM pg_settings
  WHERE name = 'rds.logical_replication';
  ```

- If it's on, go to [Create a vault](#create-a-vault)
- If it's off, follow the next steps:
  - [Create a custom RDS parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating)
  - After creation, edit it and set the `rds.logical_replication` parameter to `1`
  - [Associate the recently created parameter group with you DB instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating)
    - You can choose **Apply immediately** to apply the changes immediately
    - You'll probably need to reboot the instance for changes to take effect (be careful!)
- After reboot, check if logical replication is enabled

### Supabase

- Log into the [Supabase](https://supabase.io/) dashboard and go to your project, or create a new one.
- Check if logical replication is enabled. This should be the default setting, so you shouldn't have to change anything. You can do this in the `SQL Editor` section on the left hand side of the Supabase dashboard by running `SHOW wal_level;` query, which should log `logical`.
- You can find the database connection information on the left hand side under `Project Settings` > `Database`. You will need the `Host`, `Port`, `Database`, `Username`, and `Password` to connect to your database.
  - When you create a vault, the `--dburi` should follow this format:
    ```sh
    postgresql://postgres:[PASSWORD]@db.[PROJECT_ID].supabase.co:5432/postgres
    ```

### Create a vault

_Vaults_ define the place you push data into.

Vaults uses public key authentication, so you will need an Ethereum style (ECDSA, secp256k1) wallet to create a new vault. You can use an existing wallet or set up a new one with `vaults wallet create`. Your private key is only used locally for signing.

```bash
vaults account create [FILENAME]
```

A new private key will be written to `FILENAME`.

The name of a vault contains a `namespace` (e.g. `my_company`) and the name of an existing database relation (e.g. `my_table`), separated by a period (`.`). Use `vaults create` to create a new vault. See `vaults create --help` for more info.

```bash
vaults create --dburi [DBURI] --account [WALLET_ADDRESS] namespace.relation_name
```

🚧 Vaults currently only replicates `INSERT` statements, which means that it only replicates append-only data (e.g., log-style data). Row updates and deletes will be ignored. 🚧

### Start replicating a database

Use `vaults stream` to start a daemon that will continuously push changes to the underlying table/view to the network. See `vaults stream --help` for more info.

```bash
vaults stream --private-key [PRIVATE_KEY] namespace.relation_name
```

### Write a Parquet file

Before writing a Parquet file, you need to [Create a vault](#create-a-vault), if not already created. You can omit the `--dburi` flag, in this case.

Then, use `vaults write` to write a Parquet file.

```bash
vaults write --vault [namespace.relation_name] --private-key [PRIVATE_KEY] filepath
```

You can attach a timestamp to that file write, e.g.

```bash
vaults write --vault [namespace.relation_name] --private-key [PRIVATE_KEY] --timestamp 1699984703 filepath

# or use data format
vaults write --vault [namespace.relation_name] --private-key [PRIVATE_KEY] --timestamp 2006-01-02 filepath

# or use RFC3339 format
vaults write --vault [namespace.relation_name] --private-key [PRIVATE_KEY] --timestamp 2006-01-02T15:04:05Z07:00 filepath
```

If a timestamp is not provided, the CLI will assume the timestamp is the current client epoch in UTC.

### Listing Vaults

You can list the vaults from an account by running:

```bash
vaults list --account [ETH_ADDRESS]
```

### Listing Events

You can list events of a given vault by running:

```bash
vaults events --vault [VAULT_NAME] --latest 5
```

Events command accept `--before`,`--after` , and `--at` flags to filter events by timestamp

```bash
# examples
vaults events --vault demotest.data --at 1699569502
vaults events --vault demotest.data --before 2023-11-09T19:38:23-03:00
vaults events --vault demotest.data --after 2023-11-09
```

### Retrieving

You can retrieve a file from a vault by running:

```bash
vaults retrieve bafybeifr5njnrw67yyb2h2t7k6ukm3pml4fgphsxeurqcmgmeb7omc2vlq
```

You can also specify where to save the file:

```bash
vaults retrieve --output /path/to/dir bafybeifr5njnrw67yyb2h2t7k6ukm3pml4fgphsxeurqcmgmeb7omc2vlq
```

Or stream the file to stdout the `-` value (note: the short form `-o` is for `--output`), and then pipe it to something like [`car extract`](https://github.com/ipld/go-car) to unpack the CAR file's contents:

```bash
vaults retrieve -o - bafybeifr5njnrw67yyb2h2t7k6ukm3pml4fgphsxeurqcmgmeb7omc2vlq | car extract
```

## Development

### Running

You can make use of the scripts inside `scripts` to facilitate running the CLI locally without building.

```bash
# Starting the Provider Server
PORT=8888 ./scripts/server.sh

# Create an account
./scripts/run.sh account create pk.out

# Start replicating
./scripts/run.sh vaults stream --private-key [PRIVATE_KEY] namespace.relation_name
```

### Run tests

```bash
make test
```

Note: One of the tests requires Docker Engine to be running.

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the
[standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
