# Hysteron Documentation

We suggest that you first read the
[Hysteron Architecture and Requirements](architecture.md) to understand the
primary concepts and avoid possible mistakes.

* [Hysteron Architecture and Requirements](architecture.md)
* [Commands Invocation](commands_invocation.md)
* [Unified `hysteron` Command Reference](commands/hysteron.md)
* [Cluster Specification](cluster_spec.md)
* [Config Variable Expansion](config_expansion.md)
* [Cluster Initialization](initialization.md)
* [PostgreSQL Data/WAL/Tablespace Layout](storage_layout.md)
* [Hysteron Proxy](proxy.md)
* [Kubernetes Service Publishing](kubernetes_service_publishing.md)
* [Runtime Auto-Provisioning](runtime_autoprovision.md)
* [Management Operations](management_operations.md)
* [HA Timing Tuning](ha_timing_tuning.md)
* [Web Admin API](web_admin_api.md)
* [Setting instance parameters](postgres_parameters.md)
* [Metrics Guidelines](metrics.md)
* [Managed Logical Slots](logical_slots.md)
* [Custom pg_hba.conf entries](custom_pg_hba_entries.md)
* Backup/Restore
  * [Point In Time Recovery](pitr.md)
  * [Point In Time Recovery with wal-e](pitr_wal-e.md)
  * [Point In Time Recovery with wal-g](pitr_wal-g.md)
* [Standby Cluster](standbycluster.md)

## Misc topics

* [Enabling pg_rewind](pg_rewind.md)
* [Enabling synchronous replication](syncrepl.md)
* [PostgreSQL SSL/TLS setup](ssl.md)
* [Forcing a failover](forcefailover.md)

## Recipes

* [Manual switchover without transactions loss](manual_switchover.md)

## Examples

* [Simple test cluster](simplecluster.md)
* [Kubernetes](../examples/kubernetes/README.md)
* [Two (or more) nodes setup](twonodes.md)

[FAQ](faq.md)
