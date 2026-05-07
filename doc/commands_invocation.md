## Commands Invocation

Hysteron now provides one command: `hysteron`.

The unified CLI is split into runtime and management command groups.

Runtime commands:

* `hysteron keeper <etcd|kubernetes>`
* `hysteron sentinel <etcd|kubernetes>`
* `hysteron proxy <etcd|kubernetes>`

Management commands:

* `hysteron cluster ...`
* `hysteron failover ...`

For the complete command and option reference, see
[commands/hysteron.md](commands/hysteron.md).

Every command option can be passed on the CLI or via environment variables.
Environment variables use one canonical prefix: `HYSTERON_`.

Examples:

* `--cluster-name` -> `HYSTERON_CLUSTER_NAME`
* `--etcd-endpoints` -> `HYSTERON_ETCD_ENDPOINTS`
* `--k8s-resource-kind` -> `HYSTERON_K8S_RESOURCE_KIND`

Cluster configuration files passed to management and runtime flows can use
YAML/JSON variable expansion. See
[Config Variable Expansion](config_expansion.md) for `${VAR}` syntax.
