# Basin CLI

Lorem ipsum

## Build

```bash
make build
```

## Usage

- Make sure you have access to a superuser or a role with `LOGIN` and `REPLICATION` options.
For example, you can create a new role such as `CREATE ROLE basin WITH PASSWORD NULL LOGIN REPLICATION;`.
- Check that your Postgres installation has the [wal2json](https://github.com/eulerto/wal2json) plugin installed. If you're using AWS RDS or Google Cloud SQL, that is already installed.
- Check if logical replication is enabled:

    ```sql
    SHOW wal_level;
    ```

    The `wal_level` setting must be set to logical.

### Create a publication

```bash
basin publication create  --dburi [DBURI] --address [ETH_ADDRESS] table_name
```

### Start replicating a publication

```bash
basin publication start --private-key [PRIVATE_KEY] --name table_name
```

### Create a wallet

```bash
basin wallet create filename
```

## Running Locally

You can make use of the scripts inside `scripts` to facilitate running the CLI locally without building.

```bash
# Starting the Provider Server
PORT=8888 ETH_ADDRESS=0x8773d1D1BB8A4Bb2bc5EA1d55E2614aEdbe5351c ./scripts/server.sh

# Create a wallet
./scripts/run.sh wallet create pk.out  

# Start replicating
./scripts/run.sh publication start --name t --private-key ae91bbaffac7beb8143dc6ea5ef50aa2d37274c581f2b3f7a5faa2d6ee86b3bd 
```

## Run tests

```bash
make test
```

Note: One of the tests requires Docker Engine to be running.
