# components/kube-service

Adds sentinel-managed Kubernetes Services for direct database publishing:

* writable service (`hysteron-rw`)
* read-only service (`hysteron-ro`)

Use this component mainly for keeper+sentinel mode without proxy.

> [!CAUTION]
> Do not use this as a replacement for proxy in client-facing topologies unless
> you intentionally want direct database service publishing.
