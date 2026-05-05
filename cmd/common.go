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

// Package cmd contains shared command-line and store helpers reused by
// every Stolon binary (keeper, sentinel, proxy, stolonctl).
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sorintlab/stolon/internal/buildflags"
	"github.com/sorintlab/stolon/internal/common"
	stlog "github.com/sorintlab/stolon/internal/log"
	"github.com/sorintlab/stolon/internal/store"
	"github.com/sorintlab/stolon/internal/util"
	"github.com/woozymasta/flags"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	// Register optional Kubernetes auth plugins.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

// CommonConfig groups CLI/env options shared by every Stolon binary.
//
// Related options are organized into nested groups so the parser can
// derive long-name and env prefixes (`store`/`STORE`, `log`/`LOG`,
// `kube`/`KUBE`) from a single declaration. Defaults are expressed via
// `default:` tags; we never mutate the struct before parse.
type CommonConfig struct {
	Kube                 KubeOptions     `group:"Kubernetes" namespace:"kube" env-namespace:"KUBE"`
	ClusterNames         []string        `short:"c" long:"cluster-name" env:"CLUSTER_NAME" description:"cluster name. Can be repeated by components that support multiple clusters"`
	MetricsListenAddress string          `long:"metrics-listen-address" env:"METRICS_LISTEN_ADDRESS" description:"metrics listen address i.e \"0.0.0.0:8080\" (disabled by default)"`
	KubeConfig           string          `long:"kubeconfig" env:"KUBECONFIG" description:"path to kubeconfig file. Overrides $KUBECONFIG"`
	Log                  stlog.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Store                StoreOptions    `group:"Store" namespace:"store" env-namespace:"STORE"`
	Debug                bool            `long:"debug" env:"DEBUG" hidden:"true" description:"deprecated: forces debug logging"`
}

// StoreOptions configures the cluster data store backend (etcd v3 or
// kubernetes). Long names and env keys are prefixed with `store-`/`STORE_`
// because the enclosing CommonConfig declares the group namespaces.
type StoreOptions struct {
	Backend       string        `long:"backend" env:"BACKEND" choices:"etcdv3;kubernetes" description:"store backend type"`
	Endpoints     string        `long:"endpoints" env:"ENDPOINTS" description:"a comma-delimited list of store endpoints (use https scheme for tls communication) (defaults: http://127.0.0.1:2379 for etcdv3)"`
	Prefix        string        `long:"prefix" env:"PREFIX" default:"stolon/cluster" description:"the store base prefix"`
	CertFile      string        `long:"cert-file" env:"CERT_FILE" description:"certificate file for client identification to the store"`
	KeyFile       string        `long:"key" env:"KEY" description:"private key file for client identification to the store"`
	CAFile        string        `long:"ca-file" env:"CA_FILE" description:"verify certificates of HTTPS-enabled store servers using this CA bundle"`
	Timeout       time.Duration `long:"timeout" env:"TIMEOUT" default:"5s" description:"store request timeout"`
	SkipTLSVerify bool          `long:"skip-tls-verify" env:"SKIP_TLS_VERIFY" description:"skip store certificate verification (insecure!!!)"`
}

// KubeOptions configures the kubernetes-backed store. KubeConfig is
// kept on CommonConfig (outside the group) to preserve the standard
// `--kubeconfig` flag and the conventional `KUBECONFIG` env variable.
type KubeOptions struct {
	ResourceKind string `long:"resource-kind" env:"RESOURCE_KIND" choice:"configmap" description:"the k8s resource kind to be used to store stolon clusterdata"`
	Context      string `long:"context" env:"CONTEXT" description:"name of the kubeconfig context to use"`
	Namespace    string `long:"namespace" env:"NAMESPACE" description:"name of the kubernetes namespace to use"`
}

// NewParser creates a Stolon command parser with repository-wide
// defaults (help/version flags, env-prefix, build metadata) and scans
// the data struct for option/command tags.
func NewParser(name, envPrefix string, data any, opts flags.Options) *flags.Parser {
	parser := flags.NewNamedParser(
		name,
		flags.Default|
			flags.HelpCommands|
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
	// default underscore which already matches the Stolon convention.
	parser.NamespaceDelimiter = "-"
	parser.SetVersion(buildflags.Version)
	parser.SetVersionCommit(buildflags.Commit)
	parser.SetVersionTime(buildflags.BuildTime())
	parser.SetVersionURL(buildflags.URL)
	parser.SetVersionFields(flags.VersionFieldsAll)
	if err := parser.SetMaxLongNameLength(64); err != nil {
		common.MustNot(err, "set maximum CLI long option length")
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

// ParseErrorExitCode maps flags parser errors to process exit codes.
// Help/version requests are reported as a successful exit (0).
func ParseErrorExitCode(err error) int {
	var flagsErr *flags.Error
	if errors.As(err, &flagsErr) && (flagsErr.Type == flags.ErrHelp || flagsErr.Type == flags.ErrVersion) {
		return 0
	}
	return 1
}

var clusterIdentifier = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "stolon_cluster_identifier",
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
	if cfg.Store.Backend == "" {
		return errors.New("store backend type required")
	}
	if cfg.Store.Backend == "kubernetes" && cfg.Kube.ResourceKind == "" {
		return errors.New("unspecified kubernetes resource kind")
	}
	return nil
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
	return store.NewKVStore(store.Config{
		Backend:       store.Backend(cfg.Store.Backend),
		Endpoints:     cfg.Store.Endpoints,
		Timeout:       cfg.Store.Timeout,
		CertFile:      cfg.Store.CertFile,
		KeyFile:       cfg.Store.KeyFile,
		CAFile:        cfg.Store.CAFile,
		SkipTLSVerify: cfg.Store.SkipTLSVerify,
	})
}

// NewStore creates the configured cluster-data store. The requirePod
// flag controls whether the kubernetes backend is allowed to skip
// resolving the local pod identity (useful for stolonctl).
func NewStore(cfg *CommonConfig, requirePod bool) (store.Store, error) {
	return NewStoreForCluster(cfg, cfg.ClusterName(), requirePod)
}

// NewStoreForCluster creates the configured cluster-data store for one cluster.
func NewStoreForCluster(cfg *CommonConfig, clusterName string, requirePod bool) (store.Store, error) {
	switch cfg.Store.Backend {
	case "etcdv3":
		storePath := filepath.Join(cfg.Store.Prefix, clusterName)
		kvstore, err := NewKVStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("cannot create kv store: %v", err)
		}
		return store.NewKVBackedStore(kvstore, storePath), nil
	case "kubernetes":
		kubecli, podName, namespace, err := getKubeValues(cfg, requirePod)
		if err != nil {
			return nil, err
		}
		s, err := store.NewKubeStore(kubecli, podName, namespace, clusterName)
		if err != nil {
			return nil, fmt.Errorf("cannot create store: %v", err)
		}
		return s, nil
	}
	return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
}

// NewElection creates the configured sentinel leader election backend.
func NewElection(cfg *CommonConfig, uid string) (store.Election, error) {
	return NewElectionForCluster(cfg, cfg.ClusterName(), uid)
}

// NewElectionForCluster creates the configured election backend for one cluster.
func NewElectionForCluster(cfg *CommonConfig, clusterName, uid string) (store.Election, error) {
	switch cfg.Store.Backend {
	case "etcdv3":
		storePath := filepath.Join(cfg.Store.Prefix, clusterName)
		kvstore, err := NewKVStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("cannot create kv store: %v", err)
		}
		return store.NewKVBackedElection(kvstore, filepath.Join(storePath, common.SentinelLeaderKey), uid, cfg.Store.Timeout)
	case "kubernetes":
		kubecli, podName, namespace, err := getKubeValues(cfg, true)
		if err != nil {
			return nil, err
		}
		return store.NewKubeElection(kubecli, podName, namespace, clusterName, uid)
	}
	return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
}

// ListClusters returns cluster names that have cluster data in the configured
// store backend.
func ListClusters(ctx context.Context, cfg *CommonConfig) ([]string, error) {
	switch cfg.Store.Backend {
	case "etcdv3":
		kvstore, err := NewKVStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("cannot create kv store: %v", err)
		}
		defer func() {
			_ = kvstore.Close()
		}()

		pairs, err := kvstore.List(ctx, cfg.Store.Prefix)
		if err != nil {
			return nil, fmt.Errorf("cannot list clusters: %v", err)
		}
		clusterNames := map[string]struct{}{}
		for _, pair := range pairs {
			name, ok := clusterNameFromKVClusterDataKey(cfg.Store.Prefix, pair.Key)
			if ok {
				clusterNames[name] = struct{}{}
			}
		}
		return sortedStringSet(clusterNames), nil
	case "kubernetes":
		kubecli, _, namespace, err := getKubeValues(cfg, false)
		if err != nil {
			return nil, err
		}
		configMaps, err := kubecli.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("cannot list cluster configmaps: %v", err)
		}
		clusterNames := map[string]struct{}{}
		for _, cm := range configMaps.Items {
			name, ok := clusterNameFromKubeResourceName(cm.Name)
			if !ok {
				continue
			}
			if _, ok := cm.Annotations[util.KubeClusterDataAnnotation]; ok {
				clusterNames[name] = struct{}{}
			}
		}
		return sortedStringSet(clusterNames), nil
	default:
		return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
	}
}

func clusterNameFromKVClusterDataKey(prefix, key string) (string, bool) {
	cleanPrefix := strings.Trim(filepath.ToSlash(filepath.Clean(prefix)), "/")
	cleanKey := strings.Trim(filepath.ToSlash(filepath.Clean(key)), "/")
	rel, ok := strings.CutPrefix(cleanKey, cleanPrefix+"/")
	if !ok {
		return "", false
	}
	parts := strings.Split(rel, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "clusterdata" {
		return "", false
	}
	return parts[0], true
}

func clusterNameFromKubeResourceName(name string) (string, bool) {
	clusterName, ok := strings.CutPrefix(name, util.KubeResourcePrefix+"-")
	if !ok || clusterName == "" {
		return "", false
	}
	return clusterName, true
}

func sortedStringSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func getKubeValues(cfg *CommonConfig, requirePod bool) (*kubernetes.Clientset, string, string, error) {
	kubeClientConfig := util.NewKubeClientConfig(cfg.KubeConfig, cfg.Kube.Context, cfg.Kube.Namespace)
	kubecfg, err := kubeClientConfig.ClientConfig()
	if err != nil {
		return nil, "", "", err
	}
	kubecfg.Timeout = cfg.Store.Timeout
	kubecli, err := kubernetes.NewForConfig(kubecfg)
	if err != nil {
		return nil, "", "", fmt.Errorf("cannot create kubernetes client: %v", err)
	}
	var podName string
	if requirePod {
		podName, err = util.PodName()
		if err != nil {
			return nil, "", "", err
		}
	}
	namespace, _, err := kubeClientConfig.Namespace()
	if err != nil {
		return nil, "", "", err
	}
	return kubecli, podName, namespace, nil
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
	names := cfg.ClusterNamesList()
	if len(names) == 0 {
		return errors.New("cluster name required")
	}
	if len(names) > 1 {
		return errors.New("exactly one cluster name required")
	}
	return nil
}

// CheckClusterNames fails when no cluster names are provided or any duplicate
// name is configured.
func CheckClusterNames(cfg *CommonConfig) ([]string, error) {
	names := cfg.ClusterNamesList()
	if len(names) == 0 {
		return nil, errors.New("cluster name required")
	}
	seen := map[string]struct{}{}
	for _, name := range names {
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate cluster name %q", name)
		}
		seen[name] = struct{}{}
	}
	return names, nil
}
