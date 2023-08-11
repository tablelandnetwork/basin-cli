# Basin CLI

Lorem ipsum

## Build

```bash
make build
```

## Usage

First make sure you have access to a superuser or a role with `LOGIN` and `REPLICATION` options.
For example, you can create a new role such as `CREATE ROLE basin WITH PASSWORD NULL LOGIN REPLICATION;`.

```bash
# Setup your connection to Postgres by running
basin setup

# Start replicating
basin replicate
```
