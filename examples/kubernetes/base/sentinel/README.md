# base/sentinel

Sentinel workload base layer.

Includes:

* sentinel Deployment
* sentinel Service (`hysteron-sentinel`) for web + metrics
* sentinel runtime env ConfigMap (`hysteron-sentinel-env`)

Role in topology:

* computes cluster state
* performs leader election and failover decisions
* updates cluster data in DCS (Kubernetes backend)

Not included here (added by components when needed):

* PDB
* rw/ro service publishing (`components/kube-service`)
