<!-- markdownlint-disable MD013 MD024 MD036 -->
# stolon-keeper

## NAME

**stolon-keeper**

## SYNOPSIS

`stolon-keeper [OPTIONS]`

## OPTIONS

### Application Options

* `-c`, `--cluster-name` -
  cluster name
  * Environment: `STKEEPER_CLUSTER_NAME`
* `-i`, `--uid` -
  keeper uid (must be unique in the cluster and can contain only lower-case
letters, numbers and the underscore character). If not provided a random
uid will be generated.
  * Environment: `STKEEPER_UID`
* `-d`, `--data-dir` -
  data directory
  * Environment: `STKEEPER_DATA_DIR`
* `--can-be-master` -
  allow keeper to be elected as master (default true)
  * Environment: `STKEEPER_CAN_BE_MASTER`
* `--can-be-synchronous-replica` -
  allow keeper to be chosen as synchronous replica (default true)
  * Environment: `STKEEPER_CAN_BE_SYNCHRONOUS_REPLICA`
* `--disable-data-dir-locking` -
  disable locking on data dir. Warning! It'll cause data corruptions if two
keepers are concurrently running with the same data dir.
  * Environment: `STKEEPER_DISABLE_DATA_DIR_LOCKING`
* `--allow-unsupported-postgres-version` -
  allow running with PostgreSQL versions outside the default supported major
versions. This is best-effort and may break recovery, replication slots,
configuration rendering, or file layout handling.
  * Environment: `STKEEPER_ALLOW_UNSUPPORTED_POSTGRES_VERSION`

### Store

* `--store-backend` -
  store backend type
  * Environment: `STKEEPER_STORE_BACKEND`
  * Choices: `etcdv3, kubernetes`
* `--store-endpoints` -
  a comma-delimited list of store endpoints (use https scheme for tls
communication) (defaults: `http://127.0.0.1:2379` for etcdv3)
  * Environment: `STKEEPER_STORE_ENDPOINTS`
* `--store-prefix` -
  the store base prefix
  * Defaults: `stolon/cluster`
  * Environment: `STKEEPER_STORE_PREFIX`
* `--store-cert-file` -
  certificate file for client identification to the store
  * Environment: `STKEEPER_STORE_CERT_FILE`
* `--store-key` -
  private key file for client identification to the store
  * Environment: `STKEEPER_STORE_KEY`
* `--store-ca-file` -
  verify certificates of HTTPS-enabled store servers using this CA bundle
  * Environment: `STKEEPER_STORE_CA_FILE`
* `--store-timeout` -
  store request timeout
  * Defaults: `5s`
  * Environment: `STKEEPER_STORE_TIMEOUT`
* `--store-skip-tls-verify` -
  skip store certificate verification (insecure!!!)
  * Environment: `STKEEPER_STORE_SKIP_TLS_VERIFY`

### Logging

* `--log-level` -
  log verbosity
  * Defaults: `info`
  * Environment: `STKEEPER_LOG_LEVEL`
  * Choices: `debug, info, warn, error`
* `--log-color` -
  enable color in log output (default if attached to a terminal)
  * Environment: `STKEEPER_LOG_COLOR`

### Metrics

* `--metrics-listen-address` -
  metrics listen address i.e "0.0.0.0:8080" (disabled by default)
  * Environment: `STKEEPER_METRICS_LISTEN_ADDRESS`

### Kubernetes

* `--kubeconfig` -
  path to kubeconfig file. Overrides $KUBECONFIG
  * Environment: `STKEEPER_KUBECONFIG`
* `--kube-resource-kind` -
  the k8s resource kind to be used to store stolon clusterdata
  * Environment: `STKEEPER_KUBE_RESOURCE_KIND`
  * Choices: `configmap`
* `--kube-context` -
  name of the kubeconfig context to use
  * Environment: `STKEEPER_KUBE_CONTEXT`
* `--kube-namespace` -
  name of the kubernetes namespace to use
  * Environment: `STKEEPER_KUBE_NAMESPACE`

### PostgreSQL

* `--pg-listen-address` -
  postgresql instance listening address, local address used for the postgres
instance. For all network interface, you can set the value to '*'.
  * Environment: `STKEEPER_PG_LISTEN_ADDRESS`
* `--pg-advertise-address` -
  postgresql instance address from outside. Use it to expose ip different
than local ip with a NAT networking config
  * Environment: `STKEEPER_PG_ADVERTISE_ADDRESS`
* `-p`, `--pg-port` -
  postgresql instance listening port
  * Defaults: `5432`
  * Environment: `STKEEPER_PG_PORT`
* `--pg-advertise-port` -
  postgresql instance port from outside. Use it to expose port different
than local port with a PAT networking config
  * Environment: `STKEEPER_PG_ADVERTISE_PORT`
* `--pg-bin-path` -
  absolute path to postgresql binaries. If empty they will be searched in
the current PATH
  * Environment: `STKEEPER_PG_BIN_PATH`

### PostgreSQL Replication User

* `--pg-repl-auth-method` -
  postgres replication user auth method
  * Defaults: `md5`
  * Environment: `STKEEPER_PG_REPL_AUTH_METHOD`
  * Choices: `md5, trust`
* `--pg-repl-username` -
  postgres replication user name. Required. It'll be created on db
initialization. Must be the same for all keepers.
  * Environment: `STKEEPER_PG_REPL_USERNAME`
* `--pg-repl-password` -
  postgres replication user password. Mutually exclusive with
--pg-repl-passwordfile. Must be the same for all keepers.
  * Environment: `STKEEPER_PG_REPL_PASSWORD`
* `--pg-repl-passwordfile` -
  postgres replication user password file. Mutually exclusive with
--pg-repl-password. Must be the same for all keepers.
  * Environment: `STKEEPER_PG_REPL_PASSWORDFILE`

### PostgreSQL Superuser

* `--pg-su-auth-method` -
  postgres superuser auth method
  * Defaults: `md5`
  * Environment: `STKEEPER_PG_SU_AUTH_METHOD`
  * Choices: `md5, trust`
* `--pg-su-username` -
  postgres superuser user name. Defaults to the effective user running
stolon-keeper. Must be the same for all keepers.
  * Environment: `STKEEPER_PG_SU_USERNAME`
* `--pg-su-password` -
  postgres superuser password. Mutually exclusive with --pg-su-passwordfile.
Must be the same for all keepers.
  * Environment: `STKEEPER_PG_SU_PASSWORD`
* `--pg-su-passwordfile` -
  postgres superuser password file. Mutually exclusive with
--pg-su-password. Must be the same for all keepers.
  * Environment: `STKEEPER_PG_SU_PASSWORDFILE`

### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

## COMMANDS

**Help Commands**

### help

Show help

**Usage:** `stolon-keeper [OPTIONS] help`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### version

Show version information

**Usage:** `stolon-keeper [OPTIONS] version`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### completion

Generate shell completion

**Usage:** `stolon-keeper [OPTIONS] completion [completion-OPTIONS]`

#### Generate shell completion

* `--shell SHELL` -
  Shell completion format
  * Choices: `bash, zsh, pwsh`
  * Value name: `SHELL`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

#### Arguments

* `output`
  * Description: Output file path

### config

Generate INI configuration example

**Usage:** `stolon-keeper [OPTIONS] config [config-OPTIONS]`

#### Generate INI configuration example

* `--comment-width COLUMNS` -
  Maximum width for wrapped comments
  * Defaults: `80`
  * Value name: `COLUMNS`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

#### Arguments

* `output`
  * Description: Output file path

### docs

Generate documentation

**Usage:** `stolon-keeper [OPTIONS] docs`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### docs html

Generate HTML documentation

**Usage:** `stolon-keeper [OPTIONS] docs html [html-OPTIONS]`

#### Generate HTML documentation

* `--template TEMPLATE` -
  HTML documentation template
  * Defaults: `default`
  * Choices: `default, styled`
  * Value name: `TEMPLATE`
* `--program-name NAME` -
  Override program name used in generated documentation templates
  * Value name: `NAME`
* `--toc` -
  Include table of contents in output
* `--trim-descriptions` -
  Trim description whitespace in generated output
* `--include-hidden` -
  Include hidden options, groups and commands
* `--mark-hidden` -
  Mark hidden entities in documentation output

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

#### Arguments

* `output`
  * Description: Output file path

### docs man

Generate man page documentation

**Usage:** `stolon-keeper [OPTIONS] docs man [man-OPTIONS]`

#### Generate man page documentation

* `--program-name NAME` -
  Override program name used in generated documentation templates
  * Value name: `NAME`
* `--trim-descriptions` -
  Trim description whitespace in generated output
* `--include-hidden` -
  Include hidden options, groups and commands
* `--mark-hidden` -
  Mark hidden entities in documentation output

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

#### Arguments

* `output`
  * Description: Output file path

### docs md

Generate Markdown documentation

**Usage:** `stolon-keeper [OPTIONS] docs md [md-OPTIONS]`

#### Generate Markdown documentation

* `--template TEMPLATE` -
  Markdown documentation template
  * Defaults: `list`
  * Choices: `list, table, code`
  * Value name: `TEMPLATE`
* `--program-name NAME` -
  Override program name used in generated documentation templates
  * Value name: `NAME`
* `--toc` -
  Include table of contents in output
* `--trim-descriptions` -
  Trim description whitespace in generated output
* `--include-hidden` -
  Include hidden options, groups and commands
* `--mark-hidden` -
  Mark hidden entities in documentation output

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

#### Arguments

* `output`
  * Description: Output file path
