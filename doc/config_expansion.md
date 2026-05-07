## Config Variable Expansion

Hysteron accepts JSON and YAML for user-facing cluster specification files:

* `hysteron cluster initialize -f`
* `hysteron cluster update -f`
* `hysteron cluster data write -f`
* `hysteron sentinel <etcd|kubernetes> -- --initial-cluster-spec`

Both formats use the same `json` field names documented in the cluster
specification. Before decoding, Hysteron expands environment variables in scalar
values using Bash-style syntax:

```yaml
initMode: new
newConfig:
  locale: ${HYSTERON_INIT_LOCALE:-C}
```

Supported forms:

* `${VAR}` expands to the environment value when it is set.
* `${VAR:-default}` uses `default` when the environment value is unset.
* `${VAR:=default}` uses `default` when unset and may assign it in resolvers
  that support assignment.
* `${VAR:?message}` fails decoding when the environment value is unset.
* `$${VAR}` escapes expansion and decodes as literal `${VAR}`.

Expansion runs uniformly over every string scalar, including PITR restore
commands, archive restore commands, `pgParameters`, `pgHBA`, and standby
primary connection settings, so operators can parameterize them through
environment variables without an opaque allow-list. Whenever a literal
`${VAR}` must survive config loading (for example a shell snippet that
references a variable resolved later by PostgreSQL or by the operating
shell), escape it with `$${VAR}`.
