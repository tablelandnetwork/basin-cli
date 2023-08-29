# Basin CLI

Publish data from your database to the Tableland network.

## Build

```bash
git clone https://github.com/tablelandnetwork/basin-cli.git
cd basin-cli
go install ./cmd/basin
```

## Usage

- Make sure you have access to a superuser or a role with `LOGIN` and `REPLICATION` options.
For example, you can create a new role such as `CREATE ROLE basin WITH PASSWORD NULL LOGIN REPLICATION;`.
- Check that your Postgres installation has the [wal2json](https://github.com/eulerto/wal2json) plugin installed. If you're using AWS RDS or Google Cloud SQL, that is already installed.
- Check if logical replication is enabled:

    ```sql
    SHOW wal_level;
    ```

    The `wal_level` setting must be set to logical: `ALTER SYSTEM SET wal_level = logical;`.

### Create a publication

```bash
basin publication create  --dburi [DBURI] --address [ETH_ADDRESS] namespace.relation_name
```

`namespace.relation_name` is something like `my_company.my_table`. 

### Start replicating a publication

```bash
basin publication start --private-key [PRIVATE_KEY] namespace.relation_name
```

### Create a wallet

```bash
basin wallet create filename
```

## Local development

### Running

You can make use of the scripts inside `scripts` to facilitate running the CLI locally without building.

```bash
# Starting the Provider Server
PORT=8888 ./scripts/server.sh

# Create a wallet
./scripts/run.sh wallet create pk.out  

# Start replicating
./scripts/run.sh publication start --private-key [PRIVATE_KEY] namespace.relation_name 
```

### Run tests

```bash
make test
```

Note: One of the tests requires Docker Engine to be running.

### Generate Cap'N Proto code

```bash
make generate
```
