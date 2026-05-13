# Hysteron

Stable PostgreSQL failover without flapping.

Hysteron is a PostgreSQL high-availability control plane focused on safe
failover, deterministic recovery, and operational clarity.
It coordinates keeper, sentinel, and proxy runtimes
using etcd v3 or Kubernetes as the distributed state store.

Inspired by the original [Stolon][] design,
Hysteron continues the same HA spirit with modernized internals
and new capabilities for current PostgreSQL releases.

![Hysteron Logo][logo]

## Highlights

* Safety-first failover and promotion decisions
  with consistency over availability in ambiguous partition scenarios.
* Unified `hysteron` CLI for runtime commands and cluster operations.
* etcd v3 and Kubernetes store backends.
* Asynchronous and synchronous replication support.
* Read/write and read-only proxy modes.
* Managed logical replication slots workflow for modern PostgreSQL versions.
* Optional embedded Sentinel web status UI.
* PITR and standby-cluster workflows.
* Optional `pg_rewind`-based fast resync.

## Architecture

Hysteron is composed of 3 main components:

* `keeper`: converges a local PostgreSQL instance to the desired cluster view.
* `sentinel`: computes cluster view, leader selection, and failover actions.
* `proxy`: client entrypoint that routes to current writable primary
  and can expose read-only routing.

Read: [Hysteron Architecture and Requirements][architecture]

![Hysteron architecture][architecture-diagram]

## Documentation

Start from the [Documentation Index][docs-index].

Key docs:

* [CLI reference][cli-reference]
* [Architecture][architecture]
* [Proxy modes][proxy-modes]
* [Logical slots][logical-slots]
* [Metrics][metrics]
* [Integration tests][integration-tests]

## Quick Start and Examples

* [Simple cluster][simple-cluster]
* [Docker Compose example][compose-example]
* [Kubernetes example][k8s-example]
* [Two (or more) nodes setup][two-nodes]

## Requirements

* PostgreSQL 18, 17, 16, 15, 14 by default.
* PostgreSQL 12+ can be used
  as best-effort legacy compatibility targets when explicitly enabled.
* etcd v3 or Kubernetes (depends on selected store backend).
* Cross-platform binaries are supported;
  Linux remains the primary production target.

## Operational Notes

When using etcd v3, periodic compaction and defragmentation
are required operational tasks.
Hysteron does not run global etcd maintenance automatically.

See [etcd v3 compaction][etcd-compaction].

## Project Status

Hysteron is under active development.
Breaking changes are allowed
when they improve safety, maintainability, or operational behavior,
and are documented in release notes and changelog entries.

## FAQ

See [FAQ][faq].

## Contributing

Hysteron is open source under Apache 2.0. Contributions are welcome.

<!-- LInks -->
[Stolon]: https://github.com/sorintlab/stolon
[logo]: doc/hysteron.svg
[architecture]: doc/architecture.md
[architecture-diagram]: doc/architecture.svg
[docs-index]: doc/README.md
[cli-reference]: doc/commands/hysteron.md
[proxy-modes]: doc/proxy.md
[logical-slots]: doc/logical_slots.md
[metrics]: doc/metrics.md
[integration-tests]: doc/integration-tests.md
[simple-cluster]: doc/simplecluster.md
[compose-example]: examples/compose/README.md
[k8s-example]: examples/kubernetes/README.md
[two-nodes]: doc/twonodes.md
[etcd-compaction]: doc/architecture.md#etcdv3-compaction
[faq]: doc/faq.md
