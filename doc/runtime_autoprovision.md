# Runtime Auto-Provisioning

This document describes runtime auto-provisioning behavior used to reduce
shell-wrapper logic in deployments (especially Kubernetes).

## Scope

Current implementation applies to `hysteron keeper` runtime startup only.

Auto-provisioning runs before component startup validation and fills only empty
fields. Explicit values provided by normal runtime configuration remain intact.

## Inputs Used By Auto-Provisioning

Auto-provisioning reads only environment/runtime signals:

* `POD_NAME`
* `POD_IP`
* `HOSTNAME`
* `os.Hostname()`

It does not read `HYSTERON_*` option variables directly.

## Field Mapping

`keeper uid`:

* If `uid` is empty and `POD_NAME` matches StatefulSet-style
  `<name>-<ordinal>`, then `uid = keeper<ordinal>`.
* Else if `uid` is empty, derive from host name as
  `uid = keeper_<sanitized-hostname>`.

`keeper pg listen address`:

* If `pg.listenAddress` is empty and `POD_IP` is set,
  `pg.listenAddress = POD_IP`.

## Precedence Model

Effective precedence is:

* existing explicit value in runtime target
* auto-provisioned value
* later component defaults/validation behavior

Auto-provisioning never overwrites already populated fields.

## Kubernetes Notes

For best results in StatefulSet deployments:

* expose `POD_NAME` via downward API
* expose `POD_IP` via downward API

This allows deterministic keeper identity and avoids shell startup wrappers for
basic UID/listen-address bootstrap.
