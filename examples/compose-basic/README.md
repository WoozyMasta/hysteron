# Hysteron Compose Basic HA Example

This example provides a simple but production-like local deployment:

* `etcd` backend (single node for simplicity) with auto-compaction and periodic
  defragmentation helper
* `2` sentinels
* `3` keepers:
  * cluster `demo`: primary + synchronous standby
  * cluster `demo2`: single keeper for a minimal second cluster
* `3` proxies:
  * `proxy1` and `proxy2` for `demo`
  * `proxy3` for `demo2`
* sentinel web status panel with basic auth (`hysteron`/`hysteron`)
* Prometheus with preconfigured scrape targets
* Grafana with auto-provisioned Prometheus datasource and a stub dashboard

## Prerequisites

* Docker with Compose v2
* repository root as build context

## Build Images

Run from repository root (`stolon`):

```bash
docker build -f Dockerfile -t hysteron .
docker build -f examples/compose-basic/Dockerfile \
  --build-arg HYSTERON_IMAGE=hysteron \
  --build-arg POSTGRES_VERSION=17 \
  -t hysteron-pg:17 .
```

## Start Stack

```bash
cd examples/compose-basic
docker compose up -d
```

## Endpoints

* Sentinel web:
  * `http://localhost:8080/` (sentinel1)
  * `http://localhost:8081/` (sentinel2)
  * login/password: `hysteron` / `hysteron`
* Proxy writable ports:
  * `localhost:5432` (proxy1)
  * `localhost:5433` (proxy2)
  * `localhost:5543` (proxy3, cluster `demo2`)
* Proxy read-only port:
  * `localhost:5434` (proxy2 read-only listener)
  * listener settings are passed as regular proxy flags
    (`--listen-address`, `--port`, `--read-only-*`).
* Prometheus: `http://localhost:9090`
* Grafana: `http://localhost:3000` (`admin` / `admin`)

## Quick Checks

Sentinel health endpoints (not under `web-base-path`):

```bash
curl -s http://localhost:8080/health/ready
curl -u hysteron:hysteron -s http://localhost:8080/api/v1/status | jq .
```

Cluster status from inside sentinel container:

```bash
docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name demo \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name demo2 \
  --store-backend etcd \
  --store-endpoints http://etcd:2379
```

Additional useful CLI checks:

```bash
docker compose exec sentinel1 \
  /bin/hysteron cluster spec \
  --cluster-name demo \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

docker compose exec sentinel1 \
  /bin/hysteron cluster spec \
  --cluster-name demo2 \
  --store-backend etcd \
  --store-endpoints http://etcd:2379

docker compose exec sentinel1 \
  /bin/hysteron cluster data read \
  --cluster-name demo \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

docker compose exec sentinel1 \
  /bin/hysteron cluster data read \
  --cluster-name demo2 \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name demo \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .

docker compose exec sentinel1 \
  /bin/hysteron cluster status \
  --cluster-name demo2 \
  --store-backend etcd \
  --store-endpoints http://etcd:2379 \
  --format json | jq .
```

## Traffic Smoke Test (psql / pgbench)

Writable (must work for writes):

```bash
docker run --rm --network compose-basic_default postgres:17 \
  psql "postgresql://postgres:$(tr -d '\r\n' < secrets/pg_su.txt)@proxy2:5432/postgres" \
  -c "create table if not exists smoke(id int primary key);" \
  -c "insert into smoke values (1) on conflict (id) do nothing;" \
  -c "select count(*) from smoke;"
```

Read-only listener (must reject writes):

```bash
docker run --rm --network compose-basic_default postgres:17 \
  psql "postgresql://postgres:$(tr -d '\r\n' < secrets/pg_su.txt)@proxy2:6432/postgres" \
  -c "select pg_is_in_recovery(), now();" \
  -c "insert into smoke values (2);"
```

### Quick read workload through

Initialize `pgbench` tables through writable proxy port (`5432`).

```bash
docker run --rm --network compose-basic_default \
  -e PGPASSWORD="$(tr -d '\r\n' < secrets/pg_su.txt)" \
  postgres:17 \
  pgbench -h proxy2 -p 5432 -U postgres -d postgres -i -s 10
```

Run a read-only workload through read-only proxy port (`6432`).

```bash
docker run --rm --network compose-basic_default \
  -e PGPASSWORD="$(tr -d '\r\n' < secrets/pg_su.txt)" \
  postgres:17 \
  pgbench -h proxy2 -p 6432 -U postgres -d postgres -T 15 -S
```

## Stop and Cleanup

```bash
docker compose down -v
```

## Notes

* This is a local demo, not a hardened production setup.
* etcd is single-node here to keep the example small.
* PostgreSQL superuser and replication passwords come from local files under
  `secrets/`.
