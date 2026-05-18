# components/monitoring

Adds Prometheus Operator monitoring resources:

* PodMonitor for keeper
* ServiceMonitor for sentinel and proxy

> [!IMPORTANT]
> Requires CRDs from a monitoring operator that supports
> `monitoring.coreos.com/v1` resources (`PodMonitor`, `ServiceMonitor`),
> for example Prometheus Operator
> or VictoriaMetrics Operator compatibility mode.
