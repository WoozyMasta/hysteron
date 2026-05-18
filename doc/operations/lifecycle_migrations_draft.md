# Lifecycle Migrations (Draft)

Status: draft.

This document is a working outline for migration and upgrade operations.
It is intentionally incomplete and will be refined with implementation and
integration tests.

## Goals

* Make lifecycle operations explicit and repeatable.
* Reduce operator risk during onboarding, upgrades, and offboarding.
* Define prerequisites that can later be enforced by preflight tooling.

## Scenarios

1. Upgrade Hysteron and PostgreSQL major versions.
1. Move from plain PostgreSQL to Hysteron-managed HA.
1. Move from Hysteron-managed HA back to plain PostgreSQL.

## Scenario A: PostgreSQL major upgrade under Hysteron

High-level flow (draft):

1. Run preflight checks.
1. Freeze mutating operations window.
1. Validate backups and rollback point.
1. Perform staged switchover/upgrade sequence.
1. Validate replication and client routing.
1. Resume normal operations.

Open questions:

* Direct jump support policy (for example 14->11. vs step-by-step only.
* Extension compatibility matrix ownership.
* Automated fallback behavior if one stage fails.

## Scenario B: Plain PostgreSQL -> Hysteron

High-level flow (draft):

1. Inventory current topology and constraints.
1. Prepare keeper nodes and storage.
1. Initialize cluster spec and credentials.
1. Attach primary and add replicas.
1. Cut over client traffic (proxy or Kubernetes service publishing).
1. Validate failover behavior and observability.

Open questions:

* Minimal downtime path for single-primary installations.
* Required compatibility checks for existing replication slots and settings.

## Scenario C: Hysteron -> Plain PostgreSQL

High-level flow (draft):

1. Choose final primary and freeze topology changes.
1. Disconnect Hysteron control loops cleanly.
1. Keep PostgreSQL running under plain supervision.
1. Reconfigure clients and monitoring.
1. Remove Hysteron artifacts and credentials.

Open questions:

* Exact cut sequence to avoid accidental dual writers.
* Cleanup checklist for Kubernetes resources and DCS entries.

## Preflight checks (target)

Candidate checks for future CLI/API implementation:

* PostgreSQL major/version compatibility.
* Required binaries availability.
* Extension compatibility with target version.
* Free disk space and WAL volume headroom.
* Critical parameter compatibility.
* Replication health and lag bounds.

## Test strategy (target)

* Separate upgrade suite outside fast default CI matrix.
* Sequential major chain tests: 14->15->16->17->18.
* Optional direct jump tests where supported.
* Migration tests for onboarding/offboarding flows.
