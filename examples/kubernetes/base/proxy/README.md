# base/proxy

Proxy workload base layer.

Includes:

* proxy Deployment
* proxy Service (`hysteron-proxy`) with rw/ro/metrics ports
* proxy runtime env ConfigMap (`hysteron-proxy-env`)

Not included here (added by components when needed):

* HPA
* PDB

Use proxy base for full topology where clients connect through Hysteron proxy.
For keeper+sentinel topology without proxy, omit this layer and use
`components/kube-service` for direct rw/ro service publishing.
