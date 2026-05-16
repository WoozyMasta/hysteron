# Web Admin API

Sentinel exposes management endpoints under `/api/v1/admin`.

These endpoints are transport wrappers over the same operation layer used by
CLI commands.

## Endpoints

* `POST /api/v1/admin/pause`
* `POST /api/v1/admin/resume`
* `POST /api/v1/admin/switchover`
* `POST /api/v1/admin/failover-target`
* `POST /api/v1/admin/reinit`

## Request payloads

### Pause

```json
{
  "reason": "maintenance window",
  "ttl": "30m"
}
```

Fields are optional.

### Switchover / Failover target / Reinit

```json
{
  "keeper_uid": "keeper-02"
}
```

## Auth and safety

Use admin API with authentication enabled.

If cluster is paused, mutating operations are rejected until `resume`.

## Parity mapping

* `POST /pause` <-> `hysteron cluster pause`
* `POST /resume` <-> `hysteron cluster resume`
* `POST /switchover` <-> `hysteron cluster switchover --keeper-uid ...`
* `POST /failover-target` <-> `hysteron failover target --keeper-uid ...`
* `POST /reinit` <-> `hysteron cluster reinit --keeper-uid ...`
