# base/keeper

Keeper workload base layer.

Includes:

* keeper StatefulSet
* headless keeper Service (`hysteron-keeper`)
* keeper runtime env ConfigMap (`hysteron-keeper-env`)

Operational defaults in this layer:

* 3 replicas
* `startupProbe`/`livenessProbe`/`readinessProbe`
* `emptyDir` storage
* Guaranteed QoS requests/limits for keeper container

Not included here (added by components when needed):

* anti-affinity
* PDB
* persistent storage mode patches (PVC / claim template)
