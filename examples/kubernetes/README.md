# Kubernetes Kustomize Deployment

This directory contains a modular Kustomize layout for Hysteron.

## Structure

Base layers:

* [base][]:
  aggregate convenience layer (`common + keeper + sentinel + proxy`).
* [base/common][]:
  shared runtime objects (state, RBAC, bootstrap job, common config).
* [base/keeper][]: keeper StatefulSet and service.
* [base/sentinel][]: sentinel Deployment and service.
* [base/proxy][]: proxy Deployment and service.

Optional components:

* [components/anti-affinity][]: keeper anti-affinity patch.
* [components/pdb][]: PDBs for keeper and sentinel.
* [components/hpa][]: proxy HPA.
* [components/monitoring][]: PodMonitor/ServiceMonitor resources.
* [components/kube-service][]:
   sentinel-managed RW/RO services for topology without proxy.
* [components/storage-pvc][]: standalone PVC storage mode example.
* [components/storage-claimtemplate][]:
  `volumeClaimTemplates` storage mode example.
* [components/custom-selectors][]: custom selector example component.

Root `kustomization.yaml` builds `base`
and enables selected components by default.

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

Monitoring details are documented in
[components/monitoring][].

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

Bootstrap job and initial spec details are documented in
[base/common][].

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
  - https://github.com/woozymasta/hysteron//examples/kubernetes/components/kube-service?timeout=120&ref=v0.18.0
```

> [!NOTE]
> `secretGenerator` is shown only as a simple example.
> In production, use the secret workflow that matches your operations model:
> pre-created Kubernetes Secret (for example via `kubectl create secret`)
> or an external secret management system.

[base]: ./base
[base/common]: ./base/common
[base/keeper]: ./base/keeper
[base/sentinel]: ./base/sentinel
[base/proxy]: ./base/proxy
[components/anti-affinity]: ./components/anti-affinity
[components/pdb]: ./components/pdb
[components/hpa]: ./components/hpa
[components/monitoring]: ./components/monitoring
[components/kube-service]: ./components/kube-service
[components/storage-pvc]: ./components/storage-pvc
[components/storage-claimtemplate]: ./components/storage-claimtemplate
[components/custom-selectors]: ./components/custom-selectors
