<!-- markdownlint-disable MD013 MD024 MD033 MD034 MD036 -->
# stolonctl

## NAME

**stolonctl**

## SYNOPSIS

`stolonctl [OPTIONS]`

## OPTIONS

### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### Application Options

* `-c`, `--cluster-name` -
  cluster name. Can be repeated by components that support multiple clusters
  * Environment: `$STOLONCTL_CLUSTER_NAME`

### Metrics

* `--metrics-listen-address` -
  metrics listen address i.e "0.0.0.0:8080" (disabled by default)
  * Environment: `$STOLONCTL_METRICS_LISTEN_ADDRESS`

### Kubernetes

* `--kubeconfig` -
  path to kubeconfig file. Overrides $KUBECONFIG
  * Environment: `$STOLONCTL_KUBECONFIG`
* `--kube-resource-kind` -
  the k8s resource kind to be used to store stolon clusterdata
  * Environment: `$STOLONCTL_KUBE_RESOURCE_KIND`
  * Choices: `configmap`
* `--kube-context` -
  name of the kubeconfig context to use
  * Environment: `$STOLONCTL_KUBE_CONTEXT`
* `--kube-namespace` -
  name of the kubernetes namespace to use
  * Environment: `$STOLONCTL_KUBE_NAMESPACE`

### Logging

* `--log-level` -
  log verbosity (trace is verbose; use for short-lived diagnostics)
  * Defaults: `info`
  * Environment: `$STOLONCTL_LOG_LEVEL`
  * Choices: `trace, debug, info, warn, error`
* `--log-format` -
  log output format (text or JSON)
  * Defaults: `text`
  * Environment: `$STOLONCTL_LOG_FORMAT`
  * Choices: `text, json`
* `--log-output` -
  log destination: stdout, stderr, or a file path
  * Defaults: `stderr`
  * Environment: `$STOLONCTL_LOG_OUTPUT`
* `--log-file-mode` -
  when output is a file: append or truncate existing content
  * Defaults: `append`
  * Environment: `$STOLONCTL_LOG_FILE_MODE`
  * Choices: `append, truncate`
* `--log-time-format` -
  timestamp: Go layout or rfc3339|rfc3339nano|unix|unixms|unixmicro|unixnano
  * Defaults: `2006-01-02T15:04:05.000Z07:00`
  * Environment: `$STOLONCTL_LOG_TIME_FORMAT`
* `--log-color-policy` -
  console colors: auto honors NO_COLOR, FORCE_COLOR, and TTY detection
  * Defaults: `auto`
  * Environment: `$STOLONCTL_LOG_COLOR_POLICY`
  * Choices: `auto, always, never`

### Store

* `--store-backend` -
  store backend type
  * Environment: `$STOLONCTL_STORE_BACKEND`
  * Choices: `etcdv3, kubernetes`
* `--store-endpoints` -
  a comma-delimited list of store endpoints (use https scheme for tls
communication) (defaults: http://127.0.0.1:2379 for etcdv3)
  * Environment: `$STOLONCTL_STORE_ENDPOINTS`
* `--store-prefix` -
  the store base prefix
  * Defaults: `stolon/cluster`
  * Environment: `$STOLONCTL_STORE_PREFIX`
* `--store-cert-file` -
  certificate file for client identification to the store
  * Environment: `$STOLONCTL_STORE_CERT_FILE`
* `--store-key` -
  private key file for client identification to the store
  * Environment: `$STOLONCTL_STORE_KEY`
* `--store-ca-file` -
  verify certificates of HTTPS-enabled store servers using this CA bundle
  * Environment: `$STOLONCTL_STORE_CA_FILE`
* `--store-timeout` -
  store request timeout
  * Defaults: `5s`
  * Environment: `$STOLONCTL_STORE_TIMEOUT`
* `--store-skip-tls-verify` -
  skip store certificate verification (insecure!!!)
  * Environment: `$STOLONCTL_STORE_SKIP_TLS_VERIFY`

## COMMANDS

**Help Commands**

### help

Show help

**Usage:** `stolonctl [OPTIONS] help`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### version

Show version information

**Usage:** `stolonctl [OPTIONS] version`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### completion

Generate shell completion

**Usage:** `stolonctl [OPTIONS] completion [completion-OPTIONS]`

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

### clusterdata

Manage current cluster data

**Usage:** `stolonctl [OPTIONS] clusterdata`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### clusterdata read

Retrieve the current cluster data

**Usage:** `stolonctl [OPTIONS] clusterdata read [read-OPTIONS]`

#### Retrieve the current cluster data

* `--pretty` -
  pretty print

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### clusterdata write

Write cluster data

**Usage:** `stolonctl [OPTIONS] clusterdata write [write-OPTIONS]`

#### Write cluster data

* `-f`, `--file` -
  file containing the new cluster data
* `-y`, `--yes` -
  don't ask for confirmation

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### failkeeper

Force a keeper as temporarily failed

**Usage:** `stolonctl [OPTIONS] failkeeper`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### init

Initialize a new cluster

**Usage:** `stolonctl [OPTIONS] init [init-OPTIONS]`

#### Initialize a new cluster

* `-f`, `--file` -
  file containing the new cluster spec
* `-y`, `--yes` -
  don't ask for confirmation

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### ls

List clusters in the configured store

**Usage:** `stolonctl [OPTIONS] ls`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### promote

Promote a standby cluster to a primary cluster

**Usage:** `stolonctl [OPTIONS] promote [promote-OPTIONS]`

#### Promote a standby cluster to a primary cluster

* `-y`, `--yes` -
  don't ask for confirmation

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### removekeeper

Remove a keeper from cluster data

**Usage:** `stolonctl [OPTIONS] removekeeper`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### spec

Retrieve the current cluster specification

**Usage:** `stolonctl [OPTIONS] spec [spec-OPTIONS]`

#### Retrieve the current cluster specification

* `--defaults` -
  also show default values

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### status

Display the current cluster status

**Usage:** `stolonctl [OPTIONS] status [status-OPTIONS]`

#### Display the current cluster status

* `-f`, `--format` -
  output format
  * Defaults: `text`
  * Choices: `text, json`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### update

Update a cluster specification

**Usage:** `stolonctl [OPTIONS] update [update-OPTIONS]`

#### Update a cluster specification

* `-f`, `--file` -
  file containing a complete cluster specification or a patch to apply to
the current cluster specification

* `-p`, `--patch` -
  patch the current cluster specification instead of replacing it

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information
