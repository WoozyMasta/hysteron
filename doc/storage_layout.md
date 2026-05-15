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
* During clone/resync, keeper uses `pg_basebackup --tablespace-mapping`
  so each keeper can materialize tablespace files under its own local
  `--pg-tablespace-dir` root.
* Keep tablespace paths stable across keeper restarts/resync.
* Use dedicated directories per keeper host/node in production.
* Avoid sharing one writable tablespace path between multiple active
  PostgreSQL instances on the same host.

## Multi-Keeper Recommendations

* Configure `--pg-tablespace-dir` on every keeper.
* Prefer one keeper-local subdirectory namespace per keeper host, for example:
  * `/var/lib/postgresql/tablespaces/keeper-a`
  * `/var/lib/postgresql/tablespaces/keeper-b`
* Keep root ownership/permissions compatible with the PostgreSQL runtime user.
* Treat tablespace data as persistent operator-managed storage.

Single-host test/dev setups:

* Running multiple keepers on one machine is supported, but each keeper still
  needs isolated physical paths.
* Do not point different active keepers at the same writable tablespace
  directory.

## Common Failure Modes

* `pg_basebackup: directory ".../tablespace..." exists but is not empty`
  usually means path collision between instances.
  Fix: ensure keeper-local tablespace mapping roots and remove stale test data.
* `could not open file "pg_tblspc/...": No such file or directory`
  means PostgreSQL references a missing tablespace target.
  Fix: restore/create expected tablespace paths before restart/resync.

## Safety Behavior

Keeper cleanup removes managed PostgreSQL data/WAL directories.
Tablespace target directories are treated as user-managed and are preserved.
This avoids accidental deletion of shared tablespace paths.
