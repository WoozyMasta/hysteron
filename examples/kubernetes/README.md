# Kubernetes Kustomize Deployment

This directory is structured as a Kustomize constructor:

* `base`: keeper + sentinel, RBAC, namespace.
* `features/proxy`: optional in-cluster proxy deployment.
* `features/monitoring`: optional `ServiceMonitor`/`PodMonitor` resources.
* `features/storage-pvc`: optional standalone PVC-backed keeper storage.
* `features/storage-claimtemplate`: optional StatefulSet
  `volumeClaimTemplates` storage.
* `profiles/*`: ready-to-apply compositions.

## Profiles

* `profiles/keeper-sentinel`: keeper + sentinel only (no proxy).
* `profiles/keeper-sentinel-pvc`: keeper + sentinel with standalone PVC
  storage.
* `profiles/keeper-sentinel-claimtemplate`: keeper + sentinel with
  `volumeClaimTemplates`.
* `profiles/keeper-sentinel-monitoring`: keeper + sentinel + monitors.
* `profiles/full`: keeper + sentinel + proxy.
* `profiles/full-pvc`: full stack + standalone PVC storage.
* `profiles/full-claimtemplate`: full stack + `volumeClaimTemplates`.
* `profiles/full-monitoring`: full + monitors.

Root `kustomization.yaml` points to `profiles/full`.

## Quick start (base example)

Optional create namespace:

```bash
NS=hysteron
kubectl create ns "$NS"
```

Create runtime secret values first:

```bash
kubectl -n "$NS" create secret generic hysteron \
  --from-literal=HYSTERON_PG_SU_PASSWORD=change-me \
  --from-literal=HYSTERON_PG_REPL_PASSWORD=change-me
```

Apply selected profile:

```bash
kubectl apply -n "$NS" -k examples/kubernetes/profiles/full
```

## Key behavior

* Keeper auto-provisioning is enabled by downward API signals:
  `POD_NAME`, `POD_IP`.
* Base profile uses `emptyDir` for keeper data.
  Persistent storage is enabled through storage features/profiles.
* Sentinel runs with Kubernetes Service publishing enabled by default:
  writable and read-only Service endpoints are managed by sentinel.
* Keeper resources are `Guaranteed` QoS (`requests == limits`).
* Keeper anti-affinity is `preferred` (best effort spreading).
* Keeper storage uses dedicated standalone PVCs (not `volumeClaimTemplates`).

## Apply examples

Keeper + sentinel only:

```bash
kubectl apply -k examples/kubernetes/profiles/keeper-sentinel
```

Full stack with proxy:

```bash
kubectl apply -k examples/kubernetes/profiles/full
```

Full stack with standalone PVC storage:

```bash
kubectl apply -k examples/kubernetes/profiles/full-pvc
```

Note: `storage-pvc` intentionally patches keeper to `replicas: 1` because a
single standalone PVC cannot be shared across multiple keeper replicas.

Full stack with `volumeClaimTemplates` storage:

```bash
kubectl apply -k examples/kubernetes/profiles/full-claimtemplate
```

Full stack with monitoring resources:

```bash
kubectl apply -k examples/kubernetes/profiles/full-monitoring
```

## Cluster init

```bash
hysteron cluster \
  --cluster-name=kube-hysteron \
  --store-backend=kubernetes \
  --k8s-resource-kind=configmap \
  initialize
```

## Fast failover tuning (example)

```bash
hysteron cluster \
  --cluster-name=kube-hysteron \
  --store-backend=kubernetes \
  --k8s-resource-kind=configmap \
  update --patch '{
    "sleepInterval": "2s",
    "requestTimeout": "3s",
    "failInterval": "10s"
  }'
```

Validate timing against your API-server/network stability before production use.

## Monitoring notes

`features/monitoring` requires Prometheus Operator CRDs in the cluster:

* `monitoring.coreos.com/v1 ServiceMonitor`
* `monitoring.coreos.com/v1 PodMonitor`

## Storage retention notes

With standalone PVC objects or `volumeClaimTemplates`, data lifecycle follows
your `StorageClass`/PV reclaim policy. Use `Retain` for production data
safety.
