# base

Aggregate convenience layer.

`base/kustomization.yaml` includes all core runtime layers:

* `common`
* `keeper`
* `sentinel`
* `proxy`

Use this when you need full topology quickly.

If you need selective topology,
compose layers directly instead of using this aggregate:

* keeper+sentinel without proxy: `common + keeper + sentinel`
* full with explicit optional components:
  add `anti-affinity`, `pdb`, `hpa`, `monitoring`, storage components as needed

This file is a convenience entrypoint, not a mandatory composition model.
