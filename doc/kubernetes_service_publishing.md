# Kubernetes Service Publishing

`hysteron sentinel` can publish writable and read-only PostgreSQL endpoints
through Kubernetes Services and EndpointSlice objects instead of
requiring `hysteron proxy` in the client path.

This mode is optional and only works with the `kubernetes` backend command.

## How It Works

When enabled, the sentinel leader creates or updates:

* selectorless Kubernetes Service objects;
* one Hysteron-managed EndpointSlice per Service.

The writable EndpointSlice contains the current primary endpoint selected
by the sentinel. When there is no safe writable endpoint, it is kept with
an empty endpoint list.

The writable Service name defaults to `{resource}`, where `{resource}`
is the resolved Kubernetes resource name from `--k8s-resource-name`.
The default resource name template
is `hysteron-{cluster}` for compatibility.

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name kube-hysteron \
  --k8s-resource-kind secret \
  -- --kube-service-publishing
```

Use a custom resource name to avoid collisions when multiple Hysteron
installations share a namespace and cluster name:

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name postgres \
  --k8s-resource-name app-a-postgres \
  -- --kube-service-publishing
```

Use a custom Service name or port when needed:

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name kube-hysteron \
  -- --kube-service-publishing \
  --kube-service-name postgres \
  --kube-service-port 5432
```

## Multiple Clusters

One sentinel process can manage multiple Hysteron clusters.
With Kubernetes Service publishing enabled,
each cluster runner publishes its own Service and EndpointSlice.

For multiple clusters, `--k8s-resource-name` must include `{cluster}`.
This keeps ConfigMap or Secret clusterdata, Lease election objects,
and default Service names separate.

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name app-a \
  --cluster-name app-b \
  --k8s-resource-kind secret \
  --k8s-resource-name pg-{cluster} \
  -- --kube-service-publishing
```

This creates independent Kubernetes resources:

```text
pg-app-a        Secret or ConfigMap, plus Lease
pg-app-a        writable Service
pg-app-b        Secret or ConfigMap, plus Lease
pg-app-b        writable Service
```

Override the Service name with a template when the default is not suitable:

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name app-a \
  --cluster-name app-b \
  --k8s-resource-name pg-{cluster} \
  -- --kube-service-publishing \
  --kube-service-name postgres-{cluster}
```

## Read-Only Service

Enable read-only publishing with a separate Service:

```sh
hysteron sentinel \
  kubernetes \
  --cluster-name kube-hysteron \
  -- --kube-service-publishing \
  --kube-read-only-service-publishing
```

Defaults:

* writable Service name: `{resource}`;
* read-only Service name: `{resource}-ro`;
* both Service ports: `5432`.

Read-only endpoint selection follows the same policy as `hysteron proxy`:

* only healthy standby databases with matching generation are eligible;
* standby lag is filtered by `--kube-read-only-max-lag`;
* priority policy is set by `--kube-read-only-replica-priority`
  (`sync`, `async`, `any`);
* fallback to primary is enabled by default and can be disabled with
  `--kube-read-only-no-fallback`;
* `--kube-read-only-include-primary` adds primary to the normal read-only
  pool and is mutually exclusive with `--kube-read-only-no-fallback`.

## Why EndpointSlice

A selector-based Service is not a good fit for Hysteron primary routing because
the selected backend changes with PostgreSQL cluster state,
not with static Pod labels.
Updating Pod labels during failover would make Pod metadata part of the
routing protocol and can interfere with other controllers or selectors.

A Service without a selector and a Hysteron-managed EndpointSlice
is the intended Kubernetes shape for externally managed endpoints.
It keeps the Service stable while the sentinel changes only the endpoint list.

## Tradeoffs

Compared to `hysteron proxy`, Service publishing removes one TCP hop and one
runtime component from the data path.

The tradeoff is that Kubernetes networking owns connection routing once
the EndpointSlice is published. Existing client TCP connections
are not actively closed by Hysteron during failover.
Applications must tolerate broken or stale connections
and reconnect according to their PostgreSQL client policy.

`hysteron proxy` remains the stricter option when Hysteron
should directly close connections,
gate proxy activation through proxy heartbeats,
and control read/write routing behavior in process.

## Requirements

The sentinel ServiceAccount needs namespace-scoped access to:

* Services in the core API group;
* EndpointSlices in `discovery.k8s.io`;
* Leases in `coordination.k8s.io`;
* the selected Kubernetes store resource kind, ConfigMap or Secret.

The Service managed by Hysteron must not define a selector.
If a Service with the configured name already exists and has a selector,
the sentinel refuses to manage it.
