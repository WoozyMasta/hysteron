<!-- markdownlint-disable MD013 MD024 MD036 -->
# stolonctl

## NAME

**stolonctl**

## SYNOPSIS

`stolonctl [OPTIONS]`

## OPTIONS

### Application Options

* `-c`, `--cluster-name` -
  cluster name
  * Environment: `STOLONCTL_CLUSTER_NAME`
* `--metrics-listen-address` -
  metrics listen address i.e "0.0.0.0:8080" (disabled by default)
  * Environment: `STOLONCTL_METRICS_LISTEN_ADDRESS`
* `--kubeconfig` -
  path to kubeconfig file. Overrides $KUBECONFIG
  * Environment: `STOLONCTL_KUBECONFIG`

### Store

* `--store-backend` -
  store backend type
  * Environment: `STOLONCTL_STORE_BACKEND`
  * Choices: `etcdv3, kubernetes`
* `--store-endpoints` -
  a comma-delimited list of store endpoints (use https scheme for tls
communication) (defaults: <http://127.0.0.1:2379> for etcdv3)
  * Environment: `STOLONCTL_STORE_ENDPOINTS`
* `--store-prefix` -
  the store base prefix
  * Defaults: `stolon/cluster`
  * Environment: `STOLONCTL_STORE_PREFIX`
* `--store-cert-file` -
  certificate file for client identification to the store
  * Environment: `STOLONCTL_STORE_CERT_FILE`
* `--store-key` -
  private key file for client identification to the store
  * Environment: `STOLONCTL_STORE_KEY`
* `--store-ca-file` -
  verify certificates of HTTPS-enabled store servers using this CA bundle
  * Environment: `STOLONCTL_STORE_CA_FILE`
* `--store-timeout` -
  store request timeout
  * Defaults: `5s`
  * Environment: `STOLONCTL_STORE_TIMEOUT`
* `--store-skip-tls-verify` -
  skip store certificate verification (insecure!!!)
  * Environment: `STOLONCTL_STORE_SKIP_TLS_VERIFY`

### Logging

* `--log-level` -
  log verbosity
  * Defaults: `info`
  * Environment: `STOLONCTL_LOG_LEVEL`
  * Choices: `debug, info, warn, error`
* `--log-color` -
  enable color in log output (default if attached to a terminal)
  * Environment: `STOLONCTL_LOG_COLOR`

### Kubernetes

* `--kube-resource-kind` -
  the k8s resource kind to be used to store stolon clusterdata
  * Environment: `STOLONCTL_KUBE_RESOURCE_KIND`
  * Choices: `configmap`
* `--kube-context` -
  name of the kubeconfig context to use
  * Environment: `STOLONCTL_KUBE_CONTEXT`
* `--kube-namespace` -
  name of the kubernetes namespace to use
  * Environment: `STOLONCTL_KUBE_NAMESPACE`

### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

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

### config

Generate INI configuration example

**Usage:** `stolonctl [OPTIONS] config [config-OPTIONS]`

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

**Usage:** `stolonctl [OPTIONS] docs`

#### Help Options

* `-h`, `--help` -
  Show this help message
* `-v`, `--version` -
  Show version information

### docs html

Generate HTML documentation

**Usage:** `stolonctl [OPTIONS] docs html [html-OPTIONS]`

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

**Usage:** `stolonctl [OPTIONS] docs man [man-OPTIONS]`

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

**Usage:** `stolonctl [OPTIONS] docs md [md-OPTIONS]`

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
