# basin-cli

[![License](https://img.shields.io/github/license/tablelandnetwork/basin-cli.svg)](./LICENSE)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Continuously publish data from your database to the Tableland network.

# Table of Contents

- [basin-cli](#basin-cli)
- [Table of Contents](#table-of-contents)
- [Background](#background)
- [Usage](#usage)
  - [Install](#install)
  - [Postgres Setup](#postgres-setup)
  - [Create a publication](#create-a-publication)
  - [Start replicating a publication](#start-replicating-a-publication)
  - [Create a wallet](#create-a-wallet)
- [Development](#development)
  - [Running](#running)
  - [Run tests](#run-tests)
  - [Generate Cap'N Proto code](#generate-capn-proto-code)
- [Contributing](#contributing)
- [License](#license)

# Background

Tableland Basin is a secure and verifiable open data platform. The Basin CLI is a tool that allows you to continuously replicate a table or view from your database to the network. Currently, only PostgreSQL is supported.

🚧 Basin is currently not in a production-ready state. Any data that is pushed to the network may be subject to deletion. 🚧

# Usage

## Install

```bash
git clone https://github.com/tablelandnetwork/basin-cli.git
cd basin-cli
go install ./cmd/basin
```

## Postgres Setup

- Make sure you have access to a superuser or a role with `LOGIN` and `REPLICATION` options.
For example, you can create a new role such as `CREATE ROLE basin WITH PASSWORD NULL LOGIN REPLICATION;`.
- Check that your Postgres installation has the [wal2json](https://github.com/eulerto/wal2json) plugin installed. If you're using AWS RDS or Google Cloud SQL, that is already installed.
- Check if logical replication is enabled:

    ```sql
    SHOW wal_level;
    ```

    The `wal_level` setting must be set to logical: `ALTER SYSTEM SET wal_level = logical;`.

## Create a publication

_Publications_ define the data you are pushing to Basin.  

Basin uses public key authentication, so you will need an Ethereum style (ECDSA, secp256k1) wallet to create a new publication. You can use an existing wallet or set up a new one with `basin wallet create`. Your private key is only used locally for signing.

```bash
basin wallet create [FILENAME]
```

A new private key will be written to `FILENAME`.

The name of a publication contains a `namespace` (e.g. `my_company`) and the name of an existing database relation (e.g. `my_table`), separated by a period (`.`). Use `basin publication create` to create a new publication. See `basin publication create --help` for more info.

```bash
basin publication create  --dburi [DBURI] --address [WALLET_ADDRESS] namespace.relation_name
```

🚧 Basin currently only replicates `INSERT` statements, which means that it only replicates append-only data (e.g., log-style data). Row updates and deletes will be ignored. 🚧

## Start replicating a publication

Use `basin publication start` to start a daemon that will continuously push changes to the underlying table/view to the network. See `basin publication start --help` for more info.

```bash
basin publication start --private-key [PRIVATE_KEY] namespace.relation_name
```

# Development

## Running

You can make use of the scripts inside `scripts` to facilitate running the CLI locally without building.

```bash
# Starting the Provider Server
PORT=8888 ./scripts/server.sh

# Create a wallet
./scripts/run.sh wallet create pk.out  

# Start replicating
./scripts/run.sh publication start --private-key [PRIVATE_KEY] namespace.relation_name 
```

## Run tests

```bash
make test
```

Note: One of the tests requires Docker Engine to be running.

## Generate Cap'N Proto code

```bash
make generate
```

# Contributing

PRs accepted.

Small note: If editing the README, please conform to the
[standard-readme](https://github.com/RichardLitt/standard-readme) specification.

# License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
