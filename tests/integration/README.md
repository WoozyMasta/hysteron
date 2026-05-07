# Integration Suite (Unified CLI)

This suite targets the unified `stolon` CLI contract.

Requirements:

* `STOLON_BIN` must point to the unified binary.
* `STOLON_TEST_STORE_BACKEND` must be `etcd` or `etcdv3`.
* `ETCD_BIN` must point to an `etcd` binary (recommended: `v3.6.x`).
* `STOLON_INTEGRATION_MAX_STORES` optionally limits concurrent test stores
  (defaults to `8`, capped by `GOMAXPROCS`).
* PostgreSQL binaries (`initdb`, `postgres`, `pg_ctl`) must be available in
  `PATH`.

Run:

```bash
export STOLON_BIN=./build/stolon
export STOLON_TEST_STORE_BACKEND=etcd
export STOLON_INTEGRATION_MAX_STORES=8
export ETCD_BIN=$(command -v etcd)
go test -tags integration ./tests/integration/...
```
