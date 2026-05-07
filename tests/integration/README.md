# Integration Suite (Unified CLI)

This suite targets the unified `hysteron` CLI contract.

Requirements:

* `HYSTERON_BIN` must point to the unified binary.
* `HYSTERON_TEST_STORE_BACKEND` must be `etcd` or `etcdv3`.
* `ETCD_BIN` must point to an `etcd` binary (recommended: `v3.6.x`).
* `HYSTERON_INTEGRATION_MAX_STORES` optionally limits concurrent test stores
  (defaults to `8`, capped by `GOMAXPROCS`).
* PostgreSQL binaries (`initdb`, `postgres`, `pg_ctl`) must be available in
  `PATH`.

Run:

```bash
export HYSTERON_BIN=./build/hysteron
export HYSTERON_TEST_STORE_BACKEND=etcd
export HYSTERON_INTEGRATION_MAX_STORES=8
export ETCD_BIN=$(command -v etcd)
go test -tags integration ./tests/integration/...
```
