# Health Endpoints And Listener Layout

This document describes runtime health endpoints, probe semantics, and
listener configuration for `keeper`, `proxy`, and `sentinel`.

## Endpoints

All runtime components expose the same health routes:

* `/health`
* `/healthz`
* `/health/live`
* `/health/ready`
* `/health/startup`

Health routes are always unauthenticated.

## Probe Semantics

`/health/live`:

* process liveness only
* should stay lightweight

`/health/startup`:

* startup completion gate
* returns non-200 until component-specific startup conditions are met

`/health/ready`:

* runtime readiness gate
* reflects whether the component is ready to serve its role

## Component Readiness Logic

### Keeper

* `startup`: first PostgreSQL state snapshot was collected
* `ready`: last cached PostgreSQL state is healthy

This is the practical equivalent of a `pg_isready`-style check, but uses
keeper's existing cached state instead of spawning external checks per request.

### Proxy

* `startup`: at least one successful cluster check loop completed
* `ready`:
  * startup completed
  * writable listener is active (if writable mode enabled)
  * writable destination is currently resolved (if writable mode enabled)

### Sentinel

* `startup`: each configured cluster runner completed at least one successful
  reconciliation loop
* `ready`: each cluster runner has recent successful reconciliation
  (staleness window is enforced)

## Listener Configuration

The runtime supports independent listener addresses:

* `--web-listen-address` (sentinel only)
* `--metrics-listen-address`
* `--health-listen-address`

When two or more route groups use the same address, listeners are coalesced
into a single HTTP server with one mux.

Route boundaries:

* web/ui/api: `/`, `/api/*`, static routes
* health: `/health`, `/health/*`
* metrics: `/metrics`

## Metrics Auth

Metrics can be optionally protected with Basic auth:

* `--metrics-auth-username`
* `--metrics-auth-password`

Default behavior remains unchanged when auth is not configured.

## Recommended Port Profile

Recommended baseline profile used in examples:

* web: `8080`
* health: `8081`
* metrics: `9108`

## Kubernetes Probe Example

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: health
readinessProbe:
  httpGet:
    path: /health/ready
    port: health
startupProbe:
  httpGet:
    path: /health/startup
    port: health
```

Optional fallback for selected cases:

* `tcpSocket` probe on PostgreSQL/proxy listener can be used, but HTTP health
  probes are preferred for role-aware readiness.
