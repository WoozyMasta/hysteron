# components/anti-affinity

Adds preferred pod anti-affinity for keeper pods
(`topologyKey: kubernetes.io/hostname`).

Use when you want best-effort spreading across nodes.

> [!CAUTION]
> This is soft anti-affinity (`preferred...`), not strict isolation.
> Scheduler may still co-locate pods when cluster capacity is limited.
