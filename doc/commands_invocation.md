## Commands Invocation

Stolon now provides one command: `stolon`.

The unified CLI is split into runtime and management command groups.

Runtime commands:

* `stolon keeper <etcd|kubernetes>`
* `stolon sentinel <etcd|kubernetes>`
* `stolon proxy <etcd|kubernetes>`

Management commands:

* `stolon cluster ...`
* `stolon failover ...`

For the complete command and option reference, see
[commands/stolon.md](commands/stolon.md).

Every command option can be passed on the CLI or via environment variables.
Environment variables use one canonical prefix: `STOLON_`.

Examples:

* `--cluster-name` -> `STOLON_CLUSTER_NAME`
* `--etcd-endpoints` -> `STOLON_ETCD_ENDPOINTS`
* `--k8s-resource-kind` -> `STOLON_K8S_RESOURCE_KIND`

Cluster configuration files passed to management and runtime flows can use
YAML/JSON variable expansion. See
[Config Variable Expansion](config_expansion.md) for `${VAR}` syntax.
