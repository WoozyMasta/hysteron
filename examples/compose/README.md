# Hysteron Compose Basic HA Example

This example provides a simple but production-like local deployment:

* `etcd` backend (single node for simplicity) with auto-compaction and periodic
  defragmentation helper
* `2` sentinels
* `4` keepers:
  * cluster `a`: primary + synchronous standby (`keeper-a-1`, `keeper-a-2`)
  * cluster `b`: primary + asynchronous standby (`keeper-b-1`, `keeper-b-2`)
* `3` proxies:
  * `proxy-a-1` and `proxy-a-2` for cluster `a`
  * `proxy-b-1` for cluster `b`
* sentinel web status panel with basic auth (`hysteron`/`hysteron`)
* Prometheus with preconfigured scrape targets
* Grafana with auto-provisioned Prometheus datasource and a stub dashboard

## Prerequisites

* Docker with Compose v2
* repository root as build context

## Build Images

Run from repository root (`stolon`):

```bash
# Build hysteron runtime image.
docker build -f Dockerfile -t hysteron:latest .
# Build keeper image (PostgreSQL + hysteron keeper binary).
docker build -f examples/compose/Dockerfile \
  --build-arg HYSTERON_IMAGE=hysteron \
  --build-arg HYSTERON_TAG=latest
  --build-arg POSTGRES_VERSION=17 \
  -t hysteron-pg:17 .
```

## Start Stack

```bash
cd examples/compose
docker compose up -d
```

## Endpoints

* Sentinel web:
  * `http://localhost:8080/` (sentinel1)
  * `http://localhost:8081/` (sentinel2)
  * login/password: `hysteron` / `hysteron`
* Proxy writable ports:
  * `localhost:5432` (proxy-a-1, cluster `a`)
  * `localhost:5433` (proxy-a-2, cluster `a`)
  * `localhost:5543` (proxy-b-1, cluster `b`)
* Proxy read-only port:
  * `localhost:5434` (proxy-a-2 read-only listener for cluster `a`)
  * listener settings are passed as regular proxy flags
    (`--listen-address`, `--port`, `--read-only-*`).
* Prometheus: `http://localhost:9090`
* Grafana: `http://localhost:3000` (`admin` / `admin`)

## Quick Checks

Sentinel health endpoints (not under `web-base-path`):

```bash
# Readiness probe for sentinel1 web endpoint.
curl -s http://localhost:8080/health/ready
# Full dashboard API payload (requires basic auth).
curl -u hysteron:hysteron -s http://localhost:8080/api/v1/status | jq .
```

Cluster status from inside sentinel container:

```bash
# Human-readable status for cluster a.
docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name a \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

# Human-readable status for cluster b.
docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name b \
  --store-backend etcd \
  --store-endpoints http://etcd:2379
```

Additional useful CLI checks:

```bash
# Cluster a effective specification.
docker compose exec sentinel1 \
  /bin/hysteron cluster spec \
  --cluster-name a \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

# Cluster b effective specification.
docker compose exec sentinel1 \
  /bin/hysteron cluster spec \
  --cluster-name b \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

# Raw cluster a data document (JSON).
docker compose exec sentinel1 \
  /bin/hysteron cluster data read \
  --cluster-name a \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

# Raw cluster b data document (JSON).
docker compose exec sentinel1 \
  /bin/hysteron cluster data read \
  --cluster-name b \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

# Cluster a computed status (JSON).
docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name a \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

# Cluster b computed status (JSON).
docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name b \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .
```

## Traffic Smoke Test (psql / pgbench)

Writable (must work for writes):

```bash
docker run --rm --network compose_default postgres:17 \
  psql "postgresql://postgres:$(tr -d '\r\n' < secrets/pg_su.txt)@proxy-a-2:5432/postgres" \
  -c "create table if not exists smoke(id int primary key);" \
  -c "insert into smoke values (1) on conflict (id) do nothing;" \
  -c "select count(*) from smoke;"
```

Read-only listener (must reject writes):

```bash
docker run --rm --network compose_default postgres:17 \
  psql "postgresql://postgres:$(tr -d '\r\n' < secrets/pg_su.txt)@proxy-a-2:6432/postgres" \
  -c "select pg_is_in_recovery(), now();" \
  -c "insert into smoke values (2);"
```

### Quick read workload through

Initialize `pgbench` tables through writable proxy port (`5432`).

```bash
docker run --rm --network compose_default \
  -e PGPASSWORD="$(tr -d '\r\n' < secrets/pg_su.txt)" \
  postgres:17 \
  pgbench -h proxy-a-2 -p 5432 -U postgres -d postgres -i -s 10
```

Run a read-only workload through read-only proxy port (`6432`).

```bash
docker run --rm --network compose_default \
  -e PGPASSWORD="$(tr -d '\r\n' < secrets/pg_su.txt)" \
  postgres:17 \
  pgbench -h proxy-a-2 -p 6432 -U postgres -d postgres -T 15 -S
```

## Stop and Cleanup

```bash
docker compose down
# or with full cleanup
docker compose down --volumes --remove-orphans --rmi local
```

## Notes

* This is a local demo, not a hardened production setup.
* etcd is single-node here to keep the example small.
* PostgreSQL superuser and replication passwords come from local files under
  `secrets/`.
