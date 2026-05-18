# Kubernetes Kustomize Deployment

This directory contains a modular Kustomize layout for Hysteron.

## Structure

Base resources:

* `base/common`: shared runtime objects (state secret, service account, RBAC,
  common env config).
* `base/keeper`: keeper StatefulSet, service, PDB, keeper env.
* `base/sentinel`: sentinel Deployment, service, PDB, sentinel env.
* `base/proxy`: proxy Deployment, service, PDB, HPA, proxy env.

And additional components:

* `components/kube-service-publishing`: optional RW/RO services managed by
  sentinel (used for keeper+sentinel mode without proxy).
* `components/monitoring`: optional Prometheus Operator monitors.
* `components/storage-pvc`: optional standalone PVC mode for keeper data.
* `components/storage-claimtemplate`: optional `volumeClaimTemplates` mode for
  keeper data.

Root `kustomization.yaml` builds `base` and enables selected components.

## Quick Start

Optional create namespace:

```bash
NS=hysteron
kubectl create ns "$NS" || true
```

Create runtime secret first:

```bash
kubectl -n "$NS" create secret generic hysteron \
  --from-literal=HYSTERON_PG_SU_PASSWORD=change-me \
  --from-literal=HYSTERON_PG_REPL_PASSWORD=change-me
```

Apply current root composition:

```bash
kubectl apply -k examples/kubernetes
```

## Operational Notes

* `hysteron-common-env` is immutable by design in this example.
* `HYSTERON_CLUSTER_NAME` and `HYSTERON_K8S_RESOURCE_NAME`
  are cluster identity settings and are not expected to change in-place.
* Keeper uses `startupProbe`; sentinel and proxy use readiness/liveness only.
* Keeper anti-affinity is `preferred` (soft spread).
* Base storage is `emptyDir`;
  persistent storage requires one of storage components.

## Monitoring

`components/monitoring` requires Prometheus Operator CRDs:

* `monitoring.coreos.com/v1 ServiceMonitor`
* `monitoring.coreos.com/v1 PodMonitor`

Current monitoring component uses:

* `PodMonitor` for keeper.
* `ServiceMonitor` for sentinel and proxy.

Do not add overlapping monitors for the same metrics endpoint unless you
explicitly want duplicated scrape series.

## Cluster Initialization

This example uses Kubernetes backend with state in Secret.
Base manifests include a bootstrap `Job` (`hysteron-initialize`) that runs:

```bash
hysteron cluster \
  --cluster-name=kube-hysteron \
  --store-backend=kubernetes \
  --k8s-resource-kind=secret \
  --file=/etc/hysteron-init/cluster-spec.json \
  initialize --skip-if-present
```

The job has:

* `backoffLimit: 6`
* `activeDeadlineSeconds: 300`
* `ttlSecondsAfterFinished: 300`

Default initial cluster spec (`base/common/cluster-spec.json`):

```json
{
  "initMode": "new",
  "synchronousReplication": false,
  "minSynchronousStandbys": 1,
  "maxSynchronousStandbys": 1,
  "maxStandbys": 20,
  "maxStandbysPerSender": 3
}
```

## Remote Usage Examples

Use pinned remote URLs (`?ref=<tag-or-commit>`) for reproducible builds.
Replace `v0.18.0` with an existing release tag or commit.

### Full mode (keeper + sentinel + proxy)

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/woozymasta/hysteron//examples/kubernetes/base?timeout=120&ref=v0.18.0

components:
  - https://github.com/woozymasta/hysteron//examples/kubernetes/components/storage-claimtemplate?timeout=120&ref=v0.18.0
  - https://github.com/woozymasta/hysteron//examples/kubernetes/components/monitoring?timeout=120&ref=v0.18.0

namespace: my-ns

images:
  - name: docker.io/woozymasta/hysteron
    newTag: &version 0.18.0
  - name: docker.io/woozymasta/hysteron-pg
    newTag: &version-pg 18.0

labels:
  - includeSelectors: false
    includeTemplates: true
    pairs:
      app.kubernetes.io/app: hysteron
      app.kubernetes.io/version: *version

generatorOptions:
  disableNameSuffixHash: true

secretGenerator:
  - name: hysteron
    literals:
      - HYSTERON_PG_SU_PASSWORD=change-me
      - HYSTERON_PG_REPL_PASSWORD=change-me

buildMetadata:
  - managedByLabel
```

### Keeper + sentinel mode (without proxy)

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/woozymasta/hysteron//examples/kubernetes/base/common?timeout=120&ref=v0.18.0
  - https://github.com/woozymasta/hysteron//examples/kubernetes/base/keeper?timeout=120&ref=v0.18.0
  - https://github.com/woozymasta/hysteron//examples/kubernetes/base/sentinel?timeout=120&ref=v0.18.0

components:
  - https://github.com/woozymasta/hysteron//examples/kubernetes/components/kube-service-publishing?timeout=120&ref=v0.18.0
```

> [!NOTE]
> `secretGenerator` is shown only as a simple example.
> In production, use the secret workflow that matches your operations model:
> pre-created Kubernetes Secret (for example via `kubectl create secret`)
> or an external secret management system.
