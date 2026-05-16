// Copyright 2017 Sorint.lab
// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

// Package runtimecommon contains shared command-line and store helpers reused by
// runtime component command packages.
package runtimecommon

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/woozymasta/flags"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	stlog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/buildflags"
)

// CommonConfig groups CLI/env options shared by every Hysteron binary.
//
// Related options are organized into nested groups so the parser can
// derive long-name and env prefixes (`store`/`STORE`, `log`/`LOG`,
// `kube`/`KUBE`) from a single declaration. Defaults are expressed via
// `default:` tags; we never mutate the struct before parse.
type CommonConfig struct {
	Metrics      MetricsOptions  `group:"Metrics"`
	Kube         KubeOptions     `group:"Kubernetes"`
	ClusterNames []string        `short:"c" long:"cluster-name" env:"CLUSTER_NAME" description:"cluster name. Can be repeated by components that support multiple clusters"`
	Log          stlog.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Store        StoreOptions    `group:"Store" namespace:"store" env-namespace:"STORE"`
	Debug        bool            `long:"debug" env:"DEBUG" hidden:"true" description:"deprecated: forces debug logging"`
}

// StoreOptions configures the cluster data store backend (etcd v3 or
// kubernetes). Long names and env keys are prefixed with `store-`/`STORE_`
// because the enclosing CommonConfig declares the group namespaces.
type StoreOptions struct {
	Backend       string        `long:"backend" env:"BACKEND" choices:"etcd;etcdv3;kubernetes;k8s" validate-non-empty:"true" description:"store backend type"`
	Endpoints     string        `long:"endpoints" env:"ENDPOINTS" description:"a comma-delimited list of store endpoints (use https scheme for tls communication) (defaults: http://127.0.0.1:2379 for etcdv3)"`
	Prefix        string        `long:"prefix" env:"PREFIX" default:"hysteron/cluster" description:"the store base prefix"`
	CertFile      string        `long:"cert-file" env:"CERT_FILE" description:"certificate file for client identification to the store"`
	KeyFile       string        `long:"key" env:"KEY" description:"private key file for client identification to the store"`
	CAFile        string        `long:"ca-file" env:"CA_FILE" description:"verify certificates of HTTPS-enabled store servers using this CA bundle"`
	Timeout       time.Duration `long:"timeout" env:"TIMEOUT" default:"5s" description:"store request timeout"`
	SkipTLSVerify bool          `long:"skip-tls-verify" env:"SKIP_TLS_VERIFY" description:"skip store certificate verification (insecure!!!)"`
}

// MetricsOptions configures metrics serving for Hysteron binaries.
type MetricsOptions struct {
	ListenAddress string `long:"metrics-listen-address" env:"METRICS_LISTEN_ADDRESS" description:"metrics listen address i.e \"0.0.0.0:8080\" (disabled by default)"`
}

// KubeOptions configures the kubernetes-backed store. Long names are explicit
// to keep the existing public CLI while grouping the options in help output.
type KubeOptions struct {
	Config       string `long:"kubeconfig" env:"KUBECONFIG" description:"path to kubeconfig file. Overrides $KUBECONFIG"`
	ResourceKind string `long:"kube-resource-kind" env:"KUBE_RESOURCE_KIND" choices:"configmap;secret" description:"the k8s resource kind to be used to store hysteron clusterdata"`
	ResourceName string `long:"kube-resource-name" env:"KUBE_RESOURCE_NAME" default:"hysteron-{cluster}" description:"Kubernetes resource name template for cluster data and sentinel election objects; {cluster} is replaced with the cluster name"`
	Context      string `long:"kube-context" env:"KUBE_CONTEXT" description:"name of the kubeconfig context to use"`
	Namespace    string `long:"kube-namespace" env:"KUBE_NAMESPACE" description:"name of the kubernetes namespace to use"`
}

// NewParser creates a Hysteron command parser with repository-wide
// defaults (help/version flags, env-prefix, build metadata) and scans
// the data struct for option/command tags.
func NewParser(name, envPrefix string, data any, opts flags.Options) *flags.Parser {
	parser := flags.NewNamedParser(
		name,
		flags.Default|
			flags.HelpCommand|
			flags.VersionCommand|
			flags.CompletionCommand|
			flags.DocsCommand|
			flags.VersionFlag|
			flags.PassDoubleDash|
			flags.DetectShellFlagStyle|
			flags.DetectShellEnvStyle|
			opts,
	)
	parser.SetEnvPrefix(envPrefix)
	// Use a single dash instead of the default dot so that namespaced
	// long flags read like a flat hyphenated name (e.g. `--store-backend`
	// rather than `--store.backend`). EnvNamespaceDelimiter keeps its
	// default underscore which already matches the Hysteron convention.
	parser.NamespaceDelimiter = "-"
	parser.SetVersion(buildflags.Version)
	parser.SetVersionCommit(buildflags.Commit)
	parser.SetVersionTime(buildflags.BuildTime())
	parser.SetVersionURL(buildflags.URL)
	parser.SetVersionFields(flags.VersionFieldsAll)
	if err := parser.SetMaxLongNameLength(64); err != nil {
		common.MustNot(err, "set maximum CLI long option length")
	}
	if err := parser.SetBuiltinCommandHidden("docs", true); err != nil {
		common.MustNot(err, "hide built-in CLI docs command")
	}
	if data != nil {
		if _, err := parser.AddGroup("Application Options", "", data); err != nil {
			common.MustNot(err, "add root CLI option group")
		}
	}
	return parser
}

// InitLogging configures the shared zerolog root from CLI options. Defer the
// returned closer on long-running daemons; one-shot CLIs may ignore it.
func InitLogging(cfg *CommonConfig) (io.Closer, error) {
	opts := cfg.Log.ToOptions()
	if cfg.Debug {
		opts.Level = "debug"
	}
	return stlog.Configure(opts)
}

// CloseLogging closes the logging output and reports close failures when a
// logger is available.
func CloseLogging(closer io.Closer, logger *zerolog.Logger) {
	if closer == nil {
		return
	}
	if err := closer.Close(); err != nil && logger != nil {
		logger.Error().Err(err).Msg("failed to close log output")
	}
}

var clusterIdentifier = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "hysteron_cluster_identifier",
		Help: "Set to 1, is labelled with the cluster_name and component",
	},
	[]string{"cluster_name", "component"},
)

func init() {
	prometheus.MustRegister(clusterIdentifier)
}

// CheckCommonConfig validates store backend specific requirements that
// cannot be expressed in struct tags alone.
func CheckCommonConfig(cfg *CommonConfig) error {
	return stconfig.CheckCommonConfig(toConfig(cfg))
}

// SetMetrics labels the cluster identifier metric for the running component.
func SetMetrics(cfg *CommonConfig, component string) {
	SetMetricsForCluster(cfg.ClusterName(), component)
}

// SetMetricsForCluster labels the cluster identifier metric for one cluster.
func SetMetricsForCluster(clusterName, component string) {
	clusterIdentifier.WithLabelValues(clusterName, component).Set(1)
}

// NewKVStore creates the configured key-value store backend.
func NewKVStore(cfg *CommonConfig) (store.KVStore, error) {
	return stconfig.NewKVStore(toConfig(cfg))
}

// NewStore creates the configured cluster-data store. The requirePod
// flag controls whether the kubernetes backend is allowed to skip
// resolving the local pod identity for non-pod control flows.
func NewStore(cfg *CommonConfig, requirePod bool) (store.Store, error) {
	return stconfig.NewStore(toConfig(cfg), requirePod)
}

// NewStoreForCluster creates the configured cluster-data store for one cluster.
func NewStoreForCluster(cfg *CommonConfig, clusterName string, requirePod bool) (store.Store, error) {
	return stconfig.NewStoreForCluster(toConfig(cfg), clusterName, requirePod)
}

// NewElection creates the configured sentinel leader election backend.
func NewElection(cfg *CommonConfig, uid string) (store.Election, error) {
	return stconfig.NewElection(toConfig(cfg), uid)
}

// NewElectionForCluster creates the configured election backend for one cluster.
func NewElectionForCluster(cfg *CommonConfig, clusterName, uid string) (store.Election, error) {
	return stconfig.NewElectionForCluster(toConfig(cfg), clusterName, uid)
}

// ListClusters returns cluster names that have cluster data in the configured
// store backend.
func ListClusters(ctx context.Context, cfg *CommonConfig) ([]string, error) {
	return stconfig.ListClusters(ctx, toConfig(cfg))
}

// KubeResourceNameForCluster resolves and validates the Kubernetes resource
// name template used for cluster data and sentinel election objects.
func KubeResourceNameForCluster(cfg *CommonConfig, clusterName string) (string, error) {
	return stconfig.KubeResourceNameForCluster(toConfig(cfg), clusterName)
}

// ClusterNamesList returns normalized cluster names from CLI/env input.
func (cfg *CommonConfig) ClusterNamesList() []string {
	return normalizeClusterNames(cfg.ClusterNames)
}

// ClusterName returns the single configured cluster name, if present.
func (cfg *CommonConfig) ClusterName() string {
	names := cfg.ClusterNamesList()
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func normalizeClusterNames(values []string) []string {
	names := []string{}
	for _, value := range values {
		for name := range strings.SplitSeq(value, ",") {
			name = strings.TrimSpace(name)
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

// CheckClusterName fails unless exactly one cluster name has been provided.
// Single-cluster components keep this validation even though sentinel can run
// multiple clusters.
func CheckClusterName(cfg *CommonConfig) error {
	return stconfig.CheckClusterName(toConfig(cfg))
}

// ResolveHostNodeMetadata returns host and optional node identity used for
// diagnostic status fields.
func ResolveHostNodeMetadata() (hostname, nodeName string) {
	hostname, _ = os.Hostname()
	hostname = strings.TrimSpace(hostname)
	for _, key := range []string{"HYSTERON_NODE_NAME", "NODE_NAME"} {
		value := strings.TrimSpace(os.Getenv(key))
		if value != "" {
			nodeName = value
			break
		}
	}
	return hostname, nodeName
}

// CheckClusterNames fails when no cluster names are provided or any duplicate
// name is configured.
func CheckClusterNames(cfg *CommonConfig) ([]string, error) {
	return stconfig.CheckClusterNames(toConfig(cfg))
}

func toConfig(cfg *CommonConfig) *stconfig.CommonConfig {
	if cfg == nil {
		return &stconfig.CommonConfig{}
	}
	return &stconfig.CommonConfig{
		Metrics: stconfig.MetricsOptions{
			ListenAddress: cfg.Metrics.ListenAddress,
		},
		K8s: stconfig.K8sOptions{
			Config:       cfg.Kube.Config,
			ResourceKind: cfg.Kube.ResourceKind,
			ResourceName: cfg.Kube.ResourceName,
			Context:      cfg.Kube.Context,
			Namespace:    cfg.Kube.Namespace,
		},
		ClusterNames: cfg.ClusterNamesList(),
		Log:          cfg.Log,
		Store: stconfig.StoreOptions{
			Backend:       cfg.Store.Backend,
			Endpoints:     cfg.Store.Endpoints,
			Prefix:        cfg.Store.Prefix,
			CertFile:      cfg.Store.CertFile,
			KeyFile:       cfg.Store.KeyFile,
			CAFile:        cfg.Store.CAFile,
			Timeout:       cfg.Store.Timeout,
			SkipTLSVerify: cfg.Store.SkipTLSVerify,
		},
		Debug: cfg.Debug,
	}
}
