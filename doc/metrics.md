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

## Compatibility policy

This fork does not require strict backward compatibility for metric names.
When a metric semantic is wrong, prefer replacing it with correct semantics.
