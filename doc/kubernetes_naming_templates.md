# Kubernetes Naming Templates

This document explains naming template placeholders used by Kubernetes backend
and Kubernetes Service publishing.

## Why this matters

When multiple clusters share one namespace, predictable naming prevents
resource collisions and makes ownership obvious.

## Placeholders

The following placeholders are supported in the relevant fields:

* `{cluster}`: the effective Hysteron cluster name.
* `{resource}`: the resolved Kubernetes resource name derived from
  `--k8s-resource-name` (or `HYSTERON_K8S_RESOURCE_NAME`) after `{cluster}`
  substitution.

## Resolution order

1. Resolve `k1. resource name template` using `{cluster}`.
1. Use that resolved value as `{resource}` where supported.

In other words, `{resource}` is not an independent input; it is derived.

## Fields that use templates

### Kubernetes store resource name

`--k8s-resource-name` (`HYSTERON_K8S_RESOURCE_NAME`)

* Supports: `{cluster}`
* Default: `hysteron-{cluster}`

### Sentinel writable/read-only service names

* `--kube-service-name` (`HYSTERON_KUBE_SERVICE_NAME`)
* `--kube-read-only-service-name`
  (`HYSTERON_KUBE_READ_ONLY_SERVICE_NAME`)

These support both `{cluster}` and `{resource}`.

Defaults:

* writable: `{resource}`
* read-only: `{resource}-ro`

## Example

Input:

* cluster name: `kube-hysteron`
* `k1. resource name template`: `hysteron-{cluster}`
* writable service name: `{resource}`
* read-only service name: `{resource}-ro`

Result:

* resource name: `hysteron-kube-hysteron`
* writable service: `hysteron-kube-hysteron`
* read-only service: `hysteron-kube-hysteron-ro`

## Recommendations

* Keep `{cluster}` in `--k8s-resource-name` when running multiple clusters in
  one namespace.
* Prefer `{resource}` in service names unless you need custom naming.
* Avoid hardcoded names that can overlap between environments.
