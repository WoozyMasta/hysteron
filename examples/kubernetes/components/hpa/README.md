# components/hpa

Adds proxy HorizontalPodAutoscaler (CPU utilization target).

Optional operational component.
Tune min/max replicas and metric targets for your workload.

> [!CAUTION]
> Validate HPA behavior together with proxy timeout/check settings to avoid
> noisy scale oscillation during short failover events.
>
> **Requires Metrics API in cluster**
> (for example metrics-server or equivalent).
