# hysteron - PostgreSQL cloud native High Availability

![Hysteron Logo](doc/hysteron.svg)

hysteron is a cloud native PostgreSQL manager for PostgreSQL high availability. It's cloud native because it'll let you keep an high available PostgreSQL inside your containers (kubernetes integration) but also on every other kind of infrastructure (cloud IaaS, old style infrastructures etc...)

For an introduction to hysteron you can also take a look at [this post](https://sgotti.dev/post/hysteron-introduction/)

## Features

* Leverages PostgreSQL streaming replication.
* Resilient to any kind of partitioning. While trying to keep the maximum availability, it prefers consistency over availability.
* [kubernetes integration](examples/kubernetes/README.md) letting you achieve postgreSQL high availability.
* Uses [etcd v3](https://etcd.io) or the Kubernetes API server as a highly
  available data store and for leader election.
* Asynchronous (default) and [synchronous](doc/syncrepl.md) replication.
* Full cluster setup in minutes.
* Easy cluster administration with the unified
  [`hysteron` CLI](doc/commands/hysteron.md)
* Can do point in time recovery integrating with your preferred backup/restore tool.
* [Standby cluster](doc/standbycluster.md) (for multi site replication and near zero downtime migration).
* Automatic service discovery and dynamic reconfiguration (handles postgres and hysteron processes changing their addresses).
* Can use [pg_rewind](doc/pg_rewind.md) for fast instance resynchronization with current master.

## Architecture

Hysteron is composed of 3 main components

* keeper: it manages a PostgreSQL instance converging to the clusterview computed by the leader sentinel.
* sentinel: it discovers and monitors keepers and proxies and computes the optimal clusterview.
* proxy: the client's access point. It enforce connections to the right PostgreSQL master and forcibly closes connections to old masters.

For more details and requirements see [Hysteron Architecture and Requirements](doc/architecture.md)

![Hysteron architecture](doc/architecture.svg)

## Documentation

[Documentation Index](doc/README.md)

## Quick start and examples

* [Simple cluster example](doc/simplecluster.md)
* [Kubernetes example](examples/kubernetes/README.md)
* [Two (or more) nodes setup](doc/twonodes.md)

## Project Status

Hysteron is under active development and used in different environments. Probably its on disk format (store hierarchy and key contents) will change in future to support new features. If a breaking change is needed it'll be documented in the release notes and an upgrade path will be provided.

Anyway it's quite easy to reset a cluster from scratch keeping the current master instance working and without losing any data.

## Requirements

* PostgreSQL 18, 17, 16, 15, 14 by default. PostgreSQL 12 and newer are
  best-effort compatibility targets when explicitly allowed and verified.
* etcd v3 or Kubernetes, based on the store backend you're going to use.
* OS: currently hysteron is tested on GNU/Linux, with reports of people using it
  also on Solaris, *BSD and Darwin.

## High availability

Hysteron tries to be resilient to any partitioning problem. The cluster view is computed by the leader sentinel and is useful to avoid data loss (one example over all avoid that old dead masters coming back are elected as the new master).

There can be tons of different partitioning cases. The primary ones are covered (and in future more will be added) by various [integration tests](tests/integration)

## FAQ

See [here](doc/faq.md) for a list of faq. If you have additional questions please ask.

## Contributing to hysteron

hysteron is an open source project under the Apache 2.0 license, and contributions are gladly welcomed!
To submit your changes please open a pull request.
