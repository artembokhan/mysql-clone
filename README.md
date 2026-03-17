# mysql-clone

> **Note**
> This codebase is based on the MySQL clone client/server implementation and protocol behavior.
>
> It is largely machine-generated code produced with GPT-5.2 / GPT-5.3.
>
> It should be considered experimental proof-of-concept code rather than production-ready software. Review, validation, and thorough testing are strongly recommended before using it in production.

A network client for the MySQL/Percona clone protocol (`COM_CLONE`). It reads a clone stream and either:

- restores a data directory (`mode=innodb`), or
- writes the raw binary stream (`mode=binary`).

The client behaves similarly to native clone and always requests a backup lock on the donor.

## Quick Start

### Build

```bash
make build
```

### Example: restore a data directory

```bash
./bin/mysql-clone \
  --addr 127.0.0.1:3306 \
  --user clone_user \
  --password 'secret' \
  --out ./dump-out \
  --mode innodb
```

### Dry run

Executes the clone protocol without writing anything to disk:

```bash
./bin/mysql-clone \
  --addr 127.0.0.1:3306 \
  --user clone_user \
  --password 'secret' \
  --mode innodb \
  --dry-run
```

## How the Clone Protocol Works

The client communicates with the server using `COM_CLONE` directly, not SQL.

1. `COM_INIT` — negotiates the protocol version and options such as DDL timeout and backup lock.
2. The server returns configuration, plugins, and locators.
3. `COM_EXECUTE` — starts streaming files and data:
   - `CLONE_DESC_*` descriptors describe files, states, and data;
   - `COM_RES_DATA` contains data chunks.
4. `COM_ACK` — a separate acknowledgement channel used for state transitions and task metadata.
5. `COM_EXIT` — cleanly terminates the session.

The client behavior depends on the selected mode:

- In `mode=innodb`, it reconstructs the data directory layout and writes files.
- In `mode=binary`, it stores the stream as `stream.bin` and `data.bin`.
- On failure, it removes only artifacts created during the current run.
- On success, it writes `manifest.json` unless `--dry-run` is enabled.

## CLI Flags

### Core Options

- `--addr` — `host[:port]` or a Unix socket path. If no port is specified, `3306` is used.
- `--user` — MySQL user name. Required.
- `--password` — MySQL password.
- `--out` — output directory. Required unless `--dry-run` is used.
- `--mode` — `innodb` or `binary`. Default: `innodb`.
- `--dry-run` — runs the protocol without writing to disk.
- `--verify-checksums` — verifies InnoDB checksums after clone. This can be very slow on large datasets.
- `--concurrency` — number of parallel data connections. Supported only in `innodb` mode.

### Timeouts and Streaming

- `--ddl-timeout-sec` — DDL timeout passed in `COM_INIT`, in seconds.
- `--connect-timeout` — connection timeout.
- `--read-timeout` — socket read timeout.
- `--write-timeout` — socket write timeout.
- `--progress-interval` — progress print interval. `0` disables progress output. Also used for `--verify-checksums` progress reporting.

### Transport

- `--compress` — enables protocol compression.
- `--tls` — enables TLS.
- `--tls-insecure-skip-verify` — skips certificate verification.

### Diagnostics

- `--debug` — enables debug logging.
- `--debug-packets` — enables per-packet debug logging.
- `--version` — prints the version and exits.

## innodb-checksum

Checksum verification is compatible with `innochecksum` and supports the following modes:

- `innodb`
- `crc32`
- `none`

Usage:

```bash
./bin/innodb-checksum <path>
```

`<path>` can be either a tablespace file or a data directory.

## Server Setup for Clone (Native SQL)

Below is a minimal example for a remote clone using `CLONE INSTANCE FROM`.

The command is executed on the **recipient**. The donor responds to the clone protocol automatically; there is no separate SQL command to start clone on the donor.

### Donor

```sql
INSTALL PLUGIN clone SONAME 'mysql_clone.so';
CREATE USER 'clone_user'@'recipient.host' IDENTIFIED BY 'secret';
GRANT BACKUP_ADMIN ON *.* TO 'clone_user'@'recipient.host';
```

## Tests

### Unit tests

```bash
make test
```

### Integration scenarios

Tested with Percona Server 8.0.28 and `testcontainers-go`:

```bash
make test-it
make test-repl
make test-repl-native
```

The `make test-it`, `make test-repl`, and `make test-repl-native` targets run with `-v` and print step-by-step status output.

In clone scenarios, `innodb-checksum` is also run for the data directory before fingerprint verification.

## Integration Test Configuration

- `DATASET_ROWS` — dataset size. Default: `1000`.
- `CLONE_CONCURRENCY` — number of parallel clone connections. Default: `8`.
- `MYSQL_IMAGE` — MySQL/Percona image. Default: `percona/percona-server:8.0.28`.
- `ROOT_PASSWORD` — root password. Default: `rootpass`.
- `REPL_USER`, `REPL_PASSWORD` — replication user credentials. Defaults: `repl` / `replpass`.
