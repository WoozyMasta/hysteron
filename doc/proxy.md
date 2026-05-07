# Stolon Proxy

`stolon proxy` routes PostgreSQL client connections to the current cluster
topology discovered from the store.

The proxy can expose a writable listener, a read-only listener, or both. The
writable listener routes traffic to the current primary. The read-only listener
routes traffic to eligible standbys and can optionally fall back to the primary.

The writable listener is enabled by default. Setting read-only flags adds a
read-only listener; it does not disable the writable listener. Use
`--disable-writable-listener` when a proxy process should serve read-only
traffic only.

In the unified CLI, proxy daemon options are passed after `--` so they are
forwarded to the runtime component parser.

For the full command reference, see
[stolon command reference](commands/stolon.md).

## Writable Proxy

The writable listener is the default mode.

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --listen-address 0.0.0.0 \
  --port 5432
```

Clients connecting to this port are routed to the current primary. When the
primary changes, new client connections are routed to the new primary.

Use this mode for applications that need read/write PostgreSQL access.

## Adding A Read-Only Listener

The read-only listener is enabled by setting a read-only listen address or port.
This keeps the default writable listener enabled.

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --read-only-listen-address 0.0.0.0 \
  --read-only-port 5433
```

This command exposes two listeners:

* `5432` for writable traffic to the current primary;
* `5433` for read-only traffic to eligible standbys, with fallback to the
  current primary by default.

By default, the read-only listener accepts standbys with zero replication lag:

```sh
--read-only-max-lag 0
```

If multiple standbys are eligible, new TCP sessions are spread across them with
round-robin selection. Existing client connections stay attached to their chosen
server until they disconnect or the destination is removed from the proxy target
set.

Use the read-only listener for traffic that can be sent to PostgreSQL standbys,
such as reporting queries, analytical reads, dashboards, and application read
pools.

Specify writable flags explicitly when the writable listener should use a
non-default address or port:

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --listen-address 0.0.0.0 \
  --port 15432 \
  --read-only-listen-address 0.0.0.0 \
  --read-only-port 15433
```

In this setup applications can use one connection string for writes and another
one for reads.

## Read-Only Only Proxy

Disable the writable listener when a process should only serve read-only
traffic.

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --disable-writable-listener \
  --read-only-listen-address 0.0.0.0 \
  --read-only-port 5433
```

This mode is useful when writable and read-only traffic are deployed as
separate services, separate Kubernetes Services, or separate load balancer
targets.

## Fallback To Primary

By default, the read-only listener falls back to the current primary when no
eligible standby is available.

This favors availability: read-only clients can still connect during replica
outages, replica catch-up, or planned maintenance. Every fallback is logged as
`read-only proxy falling back to primary`.

Disable fallback when applications must never send read-only traffic to the
primary:

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --disable-writable-listener \
  --read-only-listen-address 0.0.0.0 \
  --read-only-port 5433 \
  --read-only-no-fallback
```

With fallback disabled, the read-only listener has no destination while no
eligible standby exists. Clients should retry or use their writable connection
string according to application policy.

## Including The Primary In The Read Pool

Use `--read-only-include-primary` to include the current primary in the
read-only destination pool even when eligible standbys exist.

```sh
stolon proxy etcd \
  --etcd-endpoints http://127.0.0.1:2379 \
  -- \
  --cluster-name cluster1 \
  --listen-address 0.0.0.0 \
  --port 5432 \
  --read-only-listen-address 0.0.0.0 \
  --read-only-port 5433 \
  --read-only-include-primary
```

This is different from fallback. Fallback uses the primary only when no eligible
standby exists. `--read-only-include-primary` always allows the primary to take
read-only sessions.

`--read-only-include-primary` and `--read-only-no-fallback` are mutually
exclusive.

## Replica Priority

Use `--read-only-replica-priority` to prefer a standby class when both
synchronous and asynchronous standbys are eligible.

```sh
--read-only-replica-priority sync
```

Valid values are:

* `sync`: prefer synchronous standbys when at least one is eligible;
* `async`: prefer asynchronous standbys when at least one is eligible;
* `any`: balance across all eligible standbys.

The priority only matters in mixed clusters. If all eligible standbys are in the
same class, the proxy balances across that class.

## Replication Lag

`--read-only-max-lag` controls the maximum accepted standby lag in bytes.

```sh
--read-only-max-lag 1048576
```

The default is `0`, which means only fully caught-up standbys are eligible.

Increasing the value allows the proxy to keep using standbys during small,
temporary replication delays. This can improve read availability, but clients
may observe older data on the read-only listener.

## Deployment Notes

Keep the writable and read-only ports distinct. A common deployment uses:

* `5432` for writable traffic;
* `5433` for read-only traffic.

For Kubernetes deployments, expose the two listeners as separate Services when
applications should use different connection strings or policies for reads and
writes.
