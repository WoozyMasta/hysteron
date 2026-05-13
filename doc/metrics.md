# Metrics Guidelines

This document defines the metric model for Hysteron runtimes.

## Scope

Applies to keeper, sentinel, proxy, and shared runtime/store metrics.

## Naming

* Prefix all project metrics with `hysteron_`.
* Use stable nouns for entities and actions.
* Use `_total` suffix for counters.
* Use `_seconds` suffix for durations and timestamps.
* Use `_bytes` suffix for byte quantities.

## Types

* Counter: monotonic events and outcomes.
* Gauge: current state snapshots and flags.
* Histogram: operation latency and duration distributions.

Do not use gauge for event counts.

## Labels

* Keep labels bounded and low-cardinality.
* Prefer labels like: `cluster_name`, `component`, `stage`, `operation`,
  `result`, `reason`.
* Do not include unbounded identifiers such as UID, pod name, slot name,
  database name, endpoint, or free-form error text.

## Runtime conventions

* `*_last_*_seconds` means Unix epoch seconds of the last successful event.
* Role/state gauges should expose mutually exclusive values where possible.
* Reconciliation loops should expose:
  * check/cycle duration histogram;
  * error counter by stable stage/reason.

## Controlled label values

Keep these label vocabularies stable and extend deliberately.

### `hysteron_dcs_operation_errors_total{code=...}`

Allowed values:

* `key_not_found`
* `key_modified`
* `election_no_leader`
* `other`

### `hysteron_proxy_check_errors_total{stage=...}`

Current values:

* `get_cluster_data`
* `start_writable_listener`
* `start_read_only_listener`
* `unsupported_clusterdata_format`
* `invalid_cluster_spec`
* `resolve_master_address`
* `set_proxy_info`
* `check_timeout`
* `check_failed`
* `writable_proxy_runtime`
* `read_only_proxy_runtime`

### `hysteron_proxy_connect_errors_total{reason=...}`

Current values:

* `no_destination`
* `dial`
* `non_tcp_destination`

### `hysteron_keeper_basebackup_total{result=...}`

### `hysteron_keeper_pgrewind_total{result=...}`

### `hysteron_keeper_bootstrap_total{mode=...,result=...}`

Current `result` values:

* `success`
* `error`

Current `mode` values:

* `new`
* `pitr`

## Compatibility policy

This fork does not require strict backward compatibility for metric names.
When a metric semantic is wrong, prefer replacing it with correct semantics.

## PromQL examples

Use these as starting points for alerts/dashboards.

* Sentinel checks stalled:
  `time() - max(hysteron_sentinel_last_cluster_check_success_seconds) > 30`
* Failover happened recently:
  `increase(hysteron_sentinel_failovers_total[10m]) > 0`
* Keeper cannot reach DCS:
  `max(hysteron_keeper_dcs_degraded) > 0`
* Keeper pending restart signal:
  `max(hysteron_pg_pending_restart) > 0`
* Proxy data-path connect errors:
  `sum by (mode, reason) (increase(hysteron_proxy_connect_errors_total[5m])) > 0`
* Proxy route disabled unexpectedly:
  `hysteron_proxy_route_state{mode="writable",state="disabled"} == 1`
* Slow keeper resync operations (p95):
  `histogram_quantile(0.95, sum by (le) (rate(hysteron_keeper_basebackup_duration_seconds_bucket[10m])))`
* Slow sentinel failovers (p95):
  `histogram_quantile(0.95, sum by (le) (rate(hysteron_sentinel_failover_duration_seconds_bucket[10m])))`
