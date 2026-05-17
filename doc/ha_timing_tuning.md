# HA Timing Tuning

## Why this matters

Failover speed is determined by timing values in the cluster spec. Faster values
reduce recovery time, but they also reduce tolerance to short network stalls,
CPU starvation, and temporary etcd latency spikes.

In practice, HA timing is a tradeoff between:

* reaction speed (faster failover)
* stability (fewer false unhealthy detections and re-elections)
* control-plane load (more frequent keeper/sentinel activity and etcd traffic)

## Core settings

The main timing knobs are:

* `sleepInterval`: sentinel and keeper reconciliation period.
* `requestTimeout`: timeout for component/store operations.
* `failInterval`: time window after repeated errors before a node is treated as
  unhealthy.

Current defaults:

* `sleepInterval: 5s`
* `requestTimeout: 10s`
* `failInterval: 30s`

Hysteron validates this rule:

* `sleepInterval + 2 * requestTimeout <= failInterval`

This is required to avoid self-inflicted failover due to timing geometry.

## Approximate reaction time

A rough estimate for "detect failure and start switch" is:

* around `failInterval` to mark the current master path as failed
* plus up to `sleepInterval` for the next decision cycle

So a practical approximation is:

* `reaction ~= failInterval .. failInterval + sleepInterval`

Then add PostgreSQL promotion and follow-up convergence time.

These are approximate values, not strict SLA guarantees.

## Kubernetes notes

There are two distinct Kubernetes deployment modes and they have different
control-plane behavior.

### Mode A: `store-backend=kubernetes` (Kubernetes Lease/native backend)

In this mode Hysteron uses Kubernetes resources through the API server.
There is no dedicated Hysteron etcd backend to tune.

Primary risks when tightening timings:

* API server latency spikes
* control-plane throttling/rate limits
* pod CPU throttling and node pressure
* CNI jitter between workload and API endpoints

Operational focus:

* size keeper/sentinel/proxy pod CPU requests properly
* verify API server health and latency before aggressive timings
* prefer gradual tuning with fault tests on the real cluster

### Mode B: `store-backend=etcdv3` inside Kubernetes

This is operationally the same as any etcdv3 deployment: timing tuning affects
Hysteron behavior, and etcd maintenance remains mandatory.

See: [etcdv3 compaction and maintenance](architecture.md#etcdv3-compaction)

How to apply in both modes:

* during bootstrap: set values in the initial cluster spec
* on a running cluster: `hysteron cluster update --patch '{ ... }'`

## Side effects and control-plane load

Lower timings increase control-plane pressure:

* more frequent keeper state publications
* more sentinel reconciliation loops
* higher retry and CAS contention during partial outages

Most overhead appears during incidents (not steady state).

For `store-backend=etcdv3`, plan etcd maintenance accordingly:

* periodic compaction
* regular defragmentation
* sensible retention and backup strategy

See: [etcdv3 compaction and maintenance](architecture.md#etcdv3-compaction)

## Recommended ranges

Use these as practical starting boundaries:

* `sleepInterval`: `1s .. 10s`
* `requestTimeout`: `500ms .. 5s`
* `failInterval`: `5s .. 60s` and must satisfy the timing rule

Going below these ranges is possible, but usually requires stronger isolation,
clean network paths, and better etcd sizing.

## Example profiles

These are templates. Adjust after observing real metrics and incident behavior.

### Fast profile

Use when:

* low-latency LAN
* dedicated etcd
* strict RTO priority
* acceptance of higher false-positive risk during turbulence

Example:

```bash
hysteron cluster update --patch '{
  "sleepInterval": "2s",
  "requestTimeout": "1s",
  "failInterval": "6s"
}'
```

Approximate reaction:

* about `6s .. 8s` + promotion/convergence

### Standard profile

Use when:

* general production baseline
* balanced stability/speed
* mixed workload and moderate platform jitter

Example:

```bash
hysteron cluster update --patch '{
  "sleepInterval": "5s",
  "requestTimeout": "2s",
  "failInterval": "12s"
}'
```

Approximate reaction:

* about `12s .. 17s` + promotion/convergence

### Conservative profile

Use when:

* unstable network paths
* noisy shared infrastructure
* priority is minimizing false failover/re-elections

Example:

```bash
hysteron cluster update --patch '{
  "sleepInterval": "10s",
  "requestTimeout": "5s",
  "failInterval": "30s"
}'
```

Approximate reaction:

* about `30s .. 40s` + promotion/convergence

## Rollout guidance

Tune incrementally:

1. change one profile step at a time
1. observe failover behavior under load and fault tests
1. track etcd write pressure and transaction conflicts
1. keep rollback patch ready

A practical sequence is Conservative -> Standard -> Fast, not the reverse.
