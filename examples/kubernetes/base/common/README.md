# base/common

Shared control-plane primitives for one Hysteron instance.

Includes:

* cluster state Secret (`hysteron-state`)
* bootstrap Job (`hysteron-initialize`)
* ServiceAccount + Role + RoleBinding
* immutable common ConfigMaps (`hysteron-common-env`, `hysteron-initialize-spec`)

Key behavior:

* Bootstrap Job runs idempotent `cluster initialize --skip-if-present`.
* Initial spec is loaded from `cluster-spec.json`.
* Cluster identity is defined by `HYSTERON_CLUSTER_NAME`
  and `HYSTERON_K8S_RESOURCE_NAME`.

Use this layer in every composition (`full` and `keeper+sentinel`).
