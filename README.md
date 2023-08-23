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

```basb
basin publication start --private-key [PRIVATE_KEY] --name table_name
```
