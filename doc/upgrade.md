# Upgrading from v0.4.0 to v0.5.0

## Removed commands options

These hysteron commands options were removed. You should update your scripts invoking the hysteron components removing them.

### hysteron keeper

`--listen-address`
`--port`
`--pg-conf-dir`
`--id` has been deprecated (but yet available). `--uid` should be used instead.

### hysteron sentinel

`--listen-address`
`--port`
`--discovery-type`
`--initial-cluster-config` (the equivalent for the new cluster spec format is `--initial-cluster-spec`)
`--keeper-kube-label-selector`
`--keeper-port`
`--kubernetes-namespace`

### Upgrade for new cluster data

Hysteron v0.5.0 received a big rework to improve its internal data model and implement new features. To upgrade an existing cluster from v0.4.0 to v0.5.0 you can follow the steps below (we suggest to try them in a test environment).

* Annotate the master keeperUID (previously called keeper id). You can retrieve this using `hysteron cluster status`.
* Stop all the cluster processes (keepers, sentinels and proxies)
* Upgrade the binaries to hysteron v0.5.0
* Relaunch all the cluster processes. They will loop reporting `unsupported clusterdata format version 0`.
* Initialize a new cluster data using the master keeperUID:

```shell
hysteron cluster initialize '{ "initMode": "existing", "existingConfig": { "keeperUID": "keeper01" } }'
```

The leader sentinel will choose the other keepers as standbys and they'll resync with the current master (they will do this also if before the upgrade they were already standbys since this is needed to adapt to the new cluster data format).
