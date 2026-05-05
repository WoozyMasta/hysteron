<!-- markdownlint-disable MD013 MD024 MD036 -->
# stolon-proxy

## NAME

**stolon-proxy**

## SYNOPSIS

`stolon-proxy [OPTIONS]`

## OPTIONS

### Application Options

* `-c`, `--cluster-name` -
  cluster name
  * Environment: `STPROXY_CLUSTER_NAME`
* `--stop-listening` -
  stop listening on store error (default true)
  * Environment: `STPROXY_STOP_LISTENING`

### Writable Proxy

* `-l`, `--listen-address` -
  proxy listening address
  * Defaults: `127.0.0.1`
  * Environment: `STPROXY_LISTEN_ADDRESS`
* `-p`, `--port` -
  proxy listening port
  * Defaults: `5432`
  * Environment: `STPROXY_PORT`
* `--disable-writable-listener` -
  disable the writable proxy listener
  * Environment: `STPROXY_DISABLE_WRITABLE_LISTENER`

### Read-Only Proxy

* `--read-only-listen-address` -
  read-only proxy listening address
  * Environment: `STPROXY_READ_ONLY_LISTEN_ADDRESS`
* `--read-only-port` -
  read-only proxy listening port
  * Environment: `STPROXY_READ_ONLY_PORT`
* `--read-only-max-lag` -
  maximum standby WAL lag in bytes for read-only routing
  * Defaults: `0`
  * Environment: `STPROXY_READ_ONLY_MAX_LAG`
* `--read-only-no-fallback` -
  do not route read-only connections to primary when no eligible standby exists
  * Environment: `STPROXY_READ_ONLY_NO_FALLBACK`
* `--read-only-include-primary` -
  include primary in the normal read-only backend pool
  * Environment: `STPROXY_READ_ONLY_INCLUDE_PRIMARY`
* `--read-only-replica-priority` -
  read-only replica priority policy
  * Defaults: `sync`
  * Environment: `STPROXY_READ_ONLY_REPLICA_PRIORITY`
  * Choices: `sync, async, any`

### Metrics

* `--metrics-listen-address` -
  metrics listen address i.e "0.0.0.0:8080" (disabled by default)
  * Environment: `STPROXY_METRICS_LISTEN_ADDRESS`

### Store

* `--store-backend` -
  store backend type
  * Environment: `STPROXY_STORE_BACKEND`
  * Choices: `etcdv3, kubernetes`
* `--store-endpoints` -
  a comma-delimited list of store endpoints (use https scheme for tls
communication) (defaults: `http://127.0.0.1:2379` for etcdv3)
  * Environment: `STPROXY_STORE_ENDPOINTS`
* `--store-prefix` -
  the store base prefix
  * Defaults: `stolon/cluster`
  * Environment: `STPROXY_STORE_PREFIX`
* `--store-cert-file` -
  certificate file for client identification to the store
  * Environment: `STPROXY_STORE_CERT_FILE`
* `--store-key` -
  private key file for client identification to the store
  * Environment: `STPROXY_STORE_KEY`
* `--store-ca-file` -
  verify certificates of HTTPS-enabled store servers using this CA bundle
  * Environment: `STPROXY_STORE_CA_FILE`
* `--store-timeout` -
  store request timeout
  * Defaults: `5s`
  * Environment: `STPROXY_STORE_TIMEOUT`
* `--store-skip-tls-verify` -
  skip store certificate verification (insecure!!!)
  * Environment: `STPROXY_STORE_SKIP_TLS_VERIFY`

### Logging

* `--log-level` -
  log verbosity
  * Defaults: `info`
  * Environment: `STPROXY_LOG_LEVEL`
  * Choices: `debug, info, warn, error`
* `--log-color` -
  enable color in log output (default if attached to a terminal)
  * Environment: `STPROXY_LOG_COLOR`

### Kubernetes

* `--kubeconfig` -
  path to kubeconfig file. Overrides $KUBECONFIG
  * Environment: `STPROXY_KUBECONFIG`
* `--kube-resource-kind` -
  the k8s resource kind to be used to store stolon clusterdata
  * Environment: `STPROXY_KUBE_RESOURCE_KIND`
  * Choices: `configmap`
* `--kube-context` -
  name of the kubeconfig context to use
  * Environment: `STPROXY_KUBE_CONTEXT`
* `--kube-namespace` -
  name of the kubernetes namespace to use
  * Environment: `STPROXY_KUBE_NAMESPACE`

### TCP Keep-Alive

* `--tcp-keepalive-idle` -
  set tcp keepalive idle (seconds)
  * Defaults: `0`
  * Environment: `STPROXY_TCP_KEEPALIVE_IDLE`
* `--tcp-keepalive-count` -
  set tcp keepalive probe count number
  * Defaults: `0`
  * Environment: `STPROXY_TCP_KEEPALIVE_COUNT`
* `--tcp-keepalive-interval` -
  set tcp keepalive interval (seconds)
  * Defaults: `0`
  * Environment: `STPROXY_TCP_KEEPALIVE_INTERVAL`

### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

## COMMANDS

**Help Commands**

### help

Show help

**Usage:** `stolon-proxy [OPTIONS] help`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### version

Show version information

**Usage:** `stolon-proxy [OPTIONS] version`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### completion

Generate shell completion

**Usage:** `stolon-proxy [OPTIONS] completion [completion-OPTIONS]`

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

**Usage:** `stolon-proxy [OPTIONS] config [config-OPTIONS]`

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

**Usage:** `stolon-proxy [OPTIONS] docs`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### docs html

Generate HTML documentation

**Usage:** `stolon-proxy [OPTIONS] docs html [html-OPTIONS]`

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

**Usage:** `stolon-proxy [OPTIONS] docs man [man-OPTIONS]`

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

**Usage:** `stolon-proxy [OPTIONS] docs md [md-OPTIONS]`

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
