# Integration Tests

## Context

Process-level integration tests live under `tests/integration` and are guarded
by the explicit `integration` build tag. The default unit-test suite does not
run them.

The harness starts local Stolon binaries, an etcd v3 process, and PostgreSQL
processes. It expects PostgreSQL binaries such as `postgres`, `pg_ctl`,
`initdb`, `psql`, and related tools to be available on `PATH`.

## Local Run

Build and run the integration suite against local PostgreSQL and etcd binaries:

```sh
INTEGRATION_STORE_BACKEND=etcdv3 ETCD_BIN=etcd make integration
```

Useful variables:

* `INTEGRATION_TIMEOUT`: Go test timeout, default `20m`.
* `INTEGRATION_STORE_BACKEND`: storage backend for the suite, default `etcdv3`.
* `ETCD_BIN`: etcd server binary, default `etcd`.
* `DEBUG`: pass a non-empty value to enable debug logging in started Stolon
  processes.

## Docker Compose Matrix

Use Docker Compose when local PostgreSQL binaries are missing or when checking
multiple PostgreSQL majors:

```sh
make integration-matrix
```

The default matrix covers PostgreSQL `18 17 16 15 14`. Override it with
`PG_MATRIX`:

```sh
PG_MATRIX="18 16 14" make integration-matrix
```

Run one specific major:

```sh
PG_MAJOR=17 make integration-compose
```

For CI, use the reduced matrix command (default `18 14`):

```sh
make integration-matrix-ci
```

Override the CI subset with `PG_MATRIX_CI` when needed:

```sh
PG_MATRIX_CI="18" make integration-matrix-ci
```

The compose runner builds a small test image from `golang:${GO_VERSION}-bookworm`,
installs PostgreSQL from PGDG packages, installs `etcd-server`, mounts the
repository into `/workspace`, and runs `make integration`.

Set `INTEGRATION_DEBUG=1` to enable debug logging inside the compose runner.

As of 2026-05-04, the first default advertised support target tracks current
PGDG-supported PostgreSQL majors: 18, 17, 16, 15, and 14. PostgreSQL 12 and 13
can be checked as best-effort compatibility where images and packages are still
available, but they are not part of default advertised support.

Reference: <https://www.postgresql.org/support/versioning/>.

## Notes

The integration package compile-checks on Windows and Linux with
`-tags integration`. Full process-level runtime coverage is still expected to
run in Linux or Docker first because PostgreSQL service behavior and some
process-freeze scenarios are platform-sensitive.
