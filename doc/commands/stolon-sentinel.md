<!-- markdownlint-disable MD013 MD024 MD036 -->
# stolon-sentinel

## NAME

**stolon-sentinel**

## SYNOPSIS

`stolon-sentinel [OPTIONS]`

## OPTIONS

### Application Options

* `-c`, `--cluster-name` -
  cluster name. Can be repeated to run one sentinel process for multiple
clusters
  * Environment: `STSENTINEL_CLUSTER_NAME`
* `--metrics-listen-address` -
  metrics listen address i.e "0.0.0.0:8080" (disabled by default)
  * Environment: `STSENTINEL_METRICS_LISTEN_ADDRESS`
* `--kubeconfig` -
  path to kubeconfig file. Overrides $KUBECONFIG
  * Environment: `STSENTINEL_KUBECONFIG`
* `-f`, `--initial-cluster-spec` -
  a file providing the initial cluster specification, used only at cluster
initialization, ignored if cluster is already initialized
  * Environment: `STSENTINEL_INITIAL_CLUSTER_SPEC`
* `--cluster-spec` -
  per-cluster initial cluster specification override as
`<cluster-name>=<path>`; can be repeated
  * Environment: `STSENTINEL_CLUSTER_SPEC`

### Store

* `--store-backend` -
  store backend type
  * Environment: `STSENTINEL_STORE_BACKEND`
  * Choices: `etcdv3, kubernetes`
* `--store-endpoints` -
  a comma-delimited list of store endpoints (use https scheme for tls
communication) (defaults: <http://127.0.0.1:2379> for etcdv3)
  * Environment: `STSENTINEL_STORE_ENDPOINTS`
* `--store-prefix` -
  the store base prefix
  * Defaults: `stolon/cluster`
  * Environment: `STSENTINEL_STORE_PREFIX`
* `--store-cert-file` -
  certificate file for client identification to the store
  * Environment: `STSENTINEL_STORE_CERT_FILE`
* `--store-key` -
  private key file for client identification to the store
  * Environment: `STSENTINEL_STORE_KEY`
* `--store-ca-file` -
  verify certificates of HTTPS-enabled store servers using this CA bundle
  * Environment: `STSENTINEL_STORE_CA_FILE`
* `--store-timeout` -
  store request timeout
  * Defaults: `5s`
  * Environment: `STSENTINEL_STORE_TIMEOUT`
* `--store-skip-tls-verify` -
  skip store certificate verification (insecure!!!)
  * Environment: `STSENTINEL_STORE_SKIP_TLS_VERIFY`

### Logging

* `--log-level` -
  log verbosity
  * Defaults: `info`
  * Environment: `STSENTINEL_LOG_LEVEL`
  * Choices: `debug, info, warn, error`
* `--log-color` -
  enable color in log output (default if attached to a terminal)
  * Environment: `STSENTINEL_LOG_COLOR`

### Kubernetes

* `--kube-resource-kind` -
  the k8s resource kind to be used to store stolon clusterdata
  * Environment: `STSENTINEL_KUBE_RESOURCE_KIND`
  * Choices: `configmap`
* `--kube-context` -
  name of the kubeconfig context to use
  * Environment: `STSENTINEL_KUBE_CONTEXT`
* `--kube-namespace` -
  name of the kubernetes namespace to use
  * Environment: `STSENTINEL_KUBE_NAMESPACE`

### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

## COMMANDS

**Help Commands**

### help

Show help

**Usage:** `stolon-sentinel [OPTIONS] help`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### version

Show version information

**Usage:** `stolon-sentinel [OPTIONS] version`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### completion

Generate shell completion

**Usage:** `stolon-sentinel [OPTIONS] completion [completion-OPTIONS]`

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

**Usage:** `stolon-sentinel [OPTIONS] config [config-OPTIONS]`

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

**Usage:** `stolon-sentinel [OPTIONS] docs`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### docs html

Generate HTML documentation

**Usage:** `stolon-sentinel [OPTIONS] docs html [html-OPTIONS]`

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

**Usage:** `stolon-sentinel [OPTIONS] docs man [man-OPTIONS]`

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

**Usage:** `stolon-sentinel [OPTIONS] docs md [md-OPTIONS]`

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
