## Logical Replication Slots

This document describes the current Hysteron behavior for managed logical
replication slots and what it means for CDC tools such as Kafka Connect
Debezium.

## Current behavior

Hysteron supports cluster-spec driven management of logical slots via
`managedLogicalReplicationSlots`.

What works now:

* logical slots are reconciled on the current primary;
* missing managed slots are created on primary;
* mismatched existing slots are not dropped/recreated automatically;
* unmanaged inactive `hysteron_*` logical slots can be cleaned up on primary.
* keeper publishes logical slot `confirmed_flush_lsn` state from primary into
  cluster data, and standby path computes safe advance targets
  (`min(desired_lsn, replay_lsn)`, forward-only) for managed slots.

What is not implemented yet:

* full logical-slot state transfer across failover;
* full standby-side slot lifecycle parity with primary
  (standby slot create/drop orchestration before promotion).

`enableLogicalSlotFailover` currently enables an experimental, readiness-only
mode on standby, with staged safe-advance foundations.
It does not provide full Debezium-style failover slot continuity.

## Version policy

When `enableLogicalSlotFailover=true`, behavior is version-gated:

* PostgreSQL 17+:
  Hysteron uses native PostgreSQL logical failover slots on create path.
  This is the primary production path.
* PostgreSQL 14-16:
  native logical failover slots are not available.
  Behavior remains experimental and continuity guarantees are limited.
* Standby-side logical slot advance path:
  currently enabled only on PostgreSQL 16+ when
  `enableLogicalSlotFailover=true`.

`hot_standby_feedback` is required for logical slot failover mode.
Hysteron enforces `hot_standby_feedback=on` in generated runtime parameters.

## Implications for Debezium

Today you can run Debezium against a managed logical slot on primary, but after
failover there is no guarantee that the slot position is continued exactly from
the previous primary slot state.

Practical expectation today:

* failover is supported;
* slot may need connector-level recovery/re-sync logic;
* exactly-once continuity across failover must not be assumed.

## Minimal cluster spec example

```json
{
  "pgParameters": {
    "wal_level": "logical"
  },
  "managedLogicalReplicationSlots": [
    {
      "name": "hysteron_debezium_orders",
      "database": "orders",
      "plugin": "pgoutput"
    }
  ],
  "enableLogicalSlotFailover": false
}
```

Notes:

* `wal_level=logical` is required when managed logical slots are configured.
* `enableLogicalSlotFailover` should be treated as PG17+ oriented mode.
* On PG14-16, keep `enableLogicalSlotFailover=false` unless you explicitly
  test experimental behavior in staging.

## Debezium connector example

Example fields for a PostgreSQL Debezium connector:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "proxy-or-primary-host",
  "database.port": "5432",
  "database.user": "debezium",
  "database.password": "secret",
  "database.dbname": "orders",
  "plugin.name": "pgoutput",
  "slot.name": "hysteron_debezium_orders",
  "publication.autocreate.mode": "filtered"
}
```

Use a stable service endpoint for writer traffic
(for example Hysteron proxy or Kubernetes writable service publishing),
then validate connector behavior during planned
and unplanned failover in staging before production rollout.

## Observability And Alerts

For logical-slot standby advance behavior, track these keeper metrics:

* `hysteron_keeper_logical_slot_standby_advance_attempts_total`:
  total advance attempts.
* `hysteron_keeper_logical_slot_standby_advance_success_total`:
  successful advance operations.
* `hysteron_keeper_logical_slot_standby_advance_failures_total`:
  failed advance operations.
* `hysteron_keeper_logical_slot_standby_advance_skipped_backoff_total`:
  attempts skipped due to active retry backoff.
* `hysteron_keeper_logical_slot_standby_advance_retry_slots`:
  number of slots currently in retry state.
* `hysteron_keeper_logical_slot_standby_advance_pending_slots`:
  queue size of pending slot advances.
* `hysteron_keeper_logical_slot_standby_advance_active_conflicts_total`:
  transient `SQLSTATE 55006` conflict observations.

How to interpret:

* Healthy steady state usually shows low or zero
  `pending_slots` and `retry_slots`.
* A short spike of `active_conflicts_total` during replay pressure can be
  normal if it quickly settles.
* Growing `failures_total` together with persistent non-zero `retry_slots`
  indicates an operational issue that should be investigated.

Suggested initial alerts (tune for your load profile):

* `retry_slots > 0` for 10 minutes.
* `pending_slots > 0` for 10 minutes.
* Increase in `failures_total` over 10 minutes above a small threshold
  (for example `> 10`) with no matching growth in `success_total`.
