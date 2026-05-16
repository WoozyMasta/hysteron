# Management Operations

This document explains the intent and operational model of Hysteron management
actions. Command syntax is documented in
[`commands/hysteron.md`](commands/hysteron.md); here we focus on behavior,
operator expectations, and safe usage patterns.

## Why these operations exist

Hysteron now exposes explicit cluster control actions for day-2 operations:

* temporarily freeze topology mutations during maintenance
* request planned or targeted leadership changes
* force resync of a broken replica
* run bootstrap logic safely in idempotent automation flows

The goal is to avoid ad-hoc local interventions and keep all cluster-changing
intent visible in cluster state and sentinel reconciliation.

## Operational model

Management actions are declarative requests, not imperative process-local
scripts. In practice this means:

* an operator submits intent (CLI or Web API)
* intent is persisted in cluster data
* sentinel applies it only when HA safety conditions allow it

This keeps one authority for state transitions and avoids split-brain behavior
from out-of-band node-level actions.

## Pause and resume

`pause` is a temporary control-plane freeze for mutating management operations.
It is intended for planned maintenance, risky migrations, or controlled
diagnostics.

When pause is active:

* mutating management actions are rejected (`resume first`)
* sentinel does not apply new operator mutation requests
* pause can carry optional metadata (`reason`, `ttl`) for operator context

`resume` explicitly returns the cluster to normal mutation flow. A short TTL is
recommended for routine maintenance to reduce risk of forgotten paused state.

## Manual switchover and targeted failover

Switchover and targeted failover express operator preference for where writable
leadership should move.

Important behavior:

* request acceptance does not bypass HA filters
* target keeper must still be currently eligible under sentinel safety rules
* requests are best understood as "prefer this eligible target", not "force at
  all costs"

Use switchover for planned role movement. Use failover target when recovering
from failure while steering to a known-good candidate.

## Manual replica reinitialize

Reinitialize requests a full resync for a replica keeper assignment that cannot
recover cleanly with regular convergence.

Typical cases:

* diverged or corrupted local data directory
* unrecoverable replication state on one standby
* controlled reset of one replica before rejoining

The current master is intentionally protected from reinit requests.

## Idempotent initialize

`initialize --skip-if-present` exists for automation and GitOps-style flows
where "create if absent" behavior is required.

If cluster data already exists, initialize becomes a successful no-op instead
of a hard error. This avoids brittle bootstrap scripts and repeated-run
failures.

## Priority policy and unsafe auto-failback

Keeper `master-priority` influences tie-break ranking among candidates that are
already considered safe and eligible by sentinel.

`unsafeAutoFailback` is an opt-in behavior that can automatically return
leadership to a higher-priority keeper after recovery. It is intentionally
disabled by default because unstable environments can flap.

Guardrails:

* `autoFailbackMinUptime`
* `autoFailbackCooldown`

These controls reduce oscillation risk but do not eliminate it.

## What status fields mean

Status output includes:

* sync role (`master`, `sync`, `async`)
* keeper priority
* hostname and node metadata for keepers, sentinels, and proxies

These are diagnostics for operator visibility. They are not standalone control
inputs and do not override HA safety policy.

## CLI and API parity

Each management action is available through both CLI and Web Admin API with the
same semantics and safety gates. Choose transport based on your integration
model; behavior should remain aligned.
