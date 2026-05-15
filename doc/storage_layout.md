# PostgreSQL Data, WAL, and Tablespace Layout

This guide explains how to use Hysteron keeper flags for PostgreSQL storage
layout:

* `--pg-wal-dir`
* `--pg-tablespace-dir` (repeatable)

## What Each Flag Does

* `--data-dir`:
  keeper working directory; PostgreSQL data is stored under
  `<data-dir>/postgres`.
* `--pg-wal-dir`:
  optional WAL location passed to `initdb`/`pg_basebackup` via `--waldir`.
* `--pg-tablespace-dir`:
  one or more allowed root directories for user-managed PostgreSQL
  tablespaces.

## Example Keeper Command

```bash
hysteron keeper \
  --cluster-name demo \
  --store-backend etcdv3 \
  --store-endpoints 127.0.0.1:2379 \
  --uid keeper1 \
  --data-dir /var/lib/hysteron/keeper1 \
  --pg-wal-dir /var/lib/postgresql/wal/keeper1 \
  --pg-tablespace-dir /var/lib/postgresql/tablespaces \
  --pg-listen-address 127.0.0.1 \
  --pg-port 5432
```

## PITR/Restore Command Expansion

When `initMode: "pitr"` is used, restore command templates support:

* `%d`: PostgreSQL data directory
* `%w`: WAL directory (when `--pg-wal-dir` is configured)

Example:

```bash
pg_basebackup --pgdata %d --waldir %w ...
```

## Tablespace Operational Notes

* Hysteron does not create tablespaces automatically.
  Create them in PostgreSQL (`CREATE TABLESPACE ... LOCATION ...`).
* Keep tablespace paths stable across keeper restarts/resync.
* Use dedicated directories per keeper host/node in production.
* Avoid sharing one writable tablespace path between multiple active
  PostgreSQL instances on the same host.

## Safety Behavior

Keeper cleanup removes managed PostgreSQL data/WAL directories.
Tablespace target directories are treated as user-managed and are preserved.
This avoids accidental deletion of shared tablespace paths.
