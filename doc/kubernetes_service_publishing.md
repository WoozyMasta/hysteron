# Kubernetes Service Publishing

`stolon-sentinel` can publish the current writable PostgreSQL endpoint
through a Kubernetes Service and EndpointSlice instead of
requiring `stolon-proxy` in the client path.

This mode is optional and only works with `--store-backend=kubernetes`.

## How It Works

When enabled, the sentinel leader creates or updates:

* a Kubernetes Service without a selector;
* one EndpointSlice managed by Stolon and linked to that Service.

The EndpointSlice contains the current writable PostgreSQL endpoint
selected by the sentinel.
When there is no safe writable endpoint,
the EndpointSlice is kept with an empty endpoint list.

The Service name defaults to `{resource}-rw`, where `{resource}`
is the resolved Kubernetes resource name from `--kube-resource-name`.
The default resource name template
is `stolon-cluster-{cluster}` for compatibility.

```sh
stolon-sentinel \
  --cluster-name kube-stolon \
  --store-backend kubernetes \
  --kube-resource-kind secret \
  --kube-service-publishing
```

Use a custom resource name to avoid collisions when multiple Stolon
installations share a namespace and cluster name:

```sh
stolon-sentinel \
  --cluster-name postgres \
  --store-backend kubernetes \
  --kube-resource-name app-a-postgres \
  --kube-service-publishing
```

Use a custom Service name or port when needed:

```sh
stolon-sentinel \
  --cluster-name kube-stolon \
  --store-backend kubernetes \
  --kube-service-publishing \
  --kube-service-name postgres-rw \
  --kube-service-port 5432
```

## Multiple Clusters

One sentinel process can manage multiple Stolon clusters.
With Kubernetes Service publishing enabled,
each cluster runner publishes its own Service and EndpointSlice.

For multiple clusters, `--kube-resource-name` must include `{cluster}`.
This keeps ConfigMap or Secret clusterdata, Lease election objects,
and default Service names separate.

```sh
stolon-sentinel \
  --cluster-name app-a \
  --cluster-name app-b \
  --store-backend kubernetes \
  --kube-resource-kind secret \
  --kube-resource-name pg-{cluster} \
  --kube-service-publishing
```

This creates independent Kubernetes resources:

```text
pg-app-a        Secret or ConfigMap, plus Lease
pg-app-a-rw     writable Service
pg-app-b        Secret or ConfigMap, plus Lease
pg-app-b-rw     writable Service
```

Override the Service name with a template when the default is not suitable:

```sh
stolon-sentinel \
  --cluster-name app-a \
  --cluster-name app-b \
  --store-backend kubernetes \
  --kube-resource-name pg-{cluster} \
  --kube-service-publishing \
  --kube-service-name postgres-{cluster}-rw
```

## Why EndpointSlice

A selector-based Service is not a good fit for Stolon primary routing because
the selected backend changes with PostgreSQL cluster state,
not with static Pod labels.
Updating Pod labels during failover would make Pod metadata part of the
routing protocol and can interfere with other controllers or selectors.

A Service without a selector and a Stolon-managed EndpointSlice
is the intended Kubernetes shape for externally managed endpoints.
It keeps the Service stable while the sentinel changes only the endpoint list.

## Tradeoffs

Compared to `stolon-proxy`, Service publishing removes one TCP hop and one
runtime component from the data path.

The tradeoff is that Kubernetes networking owns connection routing once
the EndpointSlice is published. Existing client TCP connections
are not actively closed by Stolon during failover.
Applications must tolerate broken or stale connections
and reconnect according to their PostgreSQL client policy.

`stolon-proxy` remains the stricter option when Stolon
should directly close connections,
gate proxy activation through proxy heartbeats,
and control read/write routing behavior in process.

## Requirements

The sentinel ServiceAccount needs namespace-scoped access to:

* Services in the core API group;
* EndpointSlices in `discovery.k8s.io`;
* Leases in `coordination.k8s.io`;
* the selected Kubernetes store resource kind, ConfigMap or Secret.

The Service managed by Stolon must not define a selector.
If a Service with the configured name already exists and has a selector,
the sentinel refuses to manage it.

## Read-Only Service

Read-only Service publishing is not implemented yet.
It should reuse the same EndpointSlice mechanism
and the read-only selection policy already used by `stolon-proxy`:
standby lag filtering, sync or async priority, optional primary fallback,
and optional primary inclusion.
