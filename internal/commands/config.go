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

package commands

import (
	"time"

	stconfig "github.com/sorintlab/stolon/internal/config"
	stlog "github.com/sorintlab/stolon/internal/log"
)

type runtimeCommonOptions struct {
	Metrics runtimeMetricsOpts `group:"Metrics"`
	clusterSelectionOptions
}

type rootGlobalOptions struct {
	Log   stlog.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Debug bool            `long:"debug" env:"DEBUG" hidden:"true" description:"deprecated: forces debug logging"`
}

type managementCommonOptions struct {
	clusterSelectionOptions
	K8s   k8sStoreOptions    `group:"Kubernetes" namespace:"k8s" env-namespace:"K8S"`
	Store clusterStoreOption `group:"Store" namespace:"store" env-namespace:"STORE"`
}

type clusterSelectionOptions struct {
	ClusterNames []string `short:"c" long:"cluster-name" env:"CLUSTER_NAME" description:"cluster name"`
}

type clusterStoreOption struct {
	Backend string `long:"backend" env:"BACKEND" choices:"etcd;etcdv3;kubernetes;k8s" validate-non-empty:"true" description:"store backend type"`
	storeConnectionOptions
}

type k8sStoreOptions struct {
	Config       string `long:"config" env:"CONFIG" description:"path to kubeconfig file. Overrides $KUBECONFIG"`
	ResourceKind string `long:"resource-kind" env:"RESOURCE_KIND" choices:"configmap;secret" description:"k8s resource kind used to store cluster data"`
	ResourceName string `long:"resource-name" env:"RESOURCE_NAME" default:"stolon-{cluster}" description:"k8s resource name template; {cluster} is replaced with the cluster name"`
	Context      string `long:"context" env:"CONTEXT" description:"kubeconfig context name"`
	Namespace    string `long:"namespace" env:"NAMESPACE" description:"kubernetes namespace name"`
}

type storeConnectionOptions struct {
	Endpoints     string        `long:"endpoints" env:"ENDPOINTS" description:"comma-separated list of store endpoints"`
	Prefix        string        `long:"prefix" env:"PREFIX" default:"stolon/cluster" description:"store base prefix"`
	CertFile      string        `long:"cert-file" env:"CERT_FILE" description:"certificate file for store client identification"`
	KeyFile       string        `long:"key" env:"KEY" description:"private key file for store client identification"`
	CAFile        string        `long:"ca-file" env:"CA_FILE" description:"CA bundle for HTTPS-enabled store servers"`
	Timeout       time.Duration `long:"timeout" env:"TIMEOUT" default:"5s" description:"store request timeout"`
	SkipTLSVerify bool          `long:"skip-tls-verify" env:"SKIP_TLS_VERIFY" description:"skip store certificate verification (insecure)"`
}

type runtimeEtcdOptions struct {
	storeConnectionOptions
}

type runtimeMetricsOpts struct {
	ListenAddress ListenEndpoint `long:"metrics-listen-address" env:"METRICS_LISTEN_ADDRESS" description:"metrics listen address i.e \"0.0.0.0:8080\" (disabled by default)"`
}

func runtimeEtcdConfig(common runtimeCommonOptions, etcd runtimeEtcdOptions) *stconfig.CommonConfig {
	return toRuntimeBackendConfig(common, etcd, k8sStoreOptions{})
}

func runtimeKubernetesConfig(common runtimeCommonOptions, k8s k8sStoreOptions) *stconfig.CommonConfig {
	return toRuntimeBackendConfig(common, runtimeEtcdOptions{}, k8s)
}

func clusterConfig() *stconfig.CommonConfig {
	return toCommonConfig(cfg.Cluster.Common)
}

func failoverConfig() *stconfig.CommonConfig {
	return toCommonConfig(cfg.Failover.Common)
}

func toCommonConfig(opts interface {
	clusterOptions() (clusterStoreOption, k8sStoreOptions, []string)
}) *stconfig.CommonConfig {
	storeOpts, k8sOpts, names := opts.clusterOptions()
	commonConfig := &stconfig.CommonConfig{
		ClusterNames: names,
		Store: stconfig.StoreOptions{
			Backend:       storeOpts.Backend,
			Endpoints:     storeOpts.Endpoints,
			Prefix:        storeOpts.Prefix,
			CertFile:      storeOpts.CertFile,
			KeyFile:       storeOpts.KeyFile,
			CAFile:        storeOpts.CAFile,
			Timeout:       storeOpts.Timeout,
			SkipTLSVerify: storeOpts.SkipTLSVerify,
		},
		K8s: stconfig.K8sOptions{
			Config:       k8sOpts.Config,
			ResourceKind: k8sOpts.ResourceKind,
			ResourceName: k8sOpts.ResourceName,
			Context:      k8sOpts.Context,
			Namespace:    k8sOpts.Namespace,
		},
	}
	applyRootGlobalOptions(commonConfig)
	return commonConfig
}

func toRuntimeBackendConfig(
	common runtimeCommonOptions,
	etcd runtimeEtcdOptions,
	k8s k8sStoreOptions,
) *stconfig.CommonConfig {
	commonConfig := &stconfig.CommonConfig{
		ClusterNames: common.ClusterNames,
		Metrics: stconfig.MetricsOptions{
			ListenAddress: string(common.Metrics.ListenAddress),
		},
		Store: stconfig.StoreOptions{
			Endpoints:     etcd.Endpoints,
			Prefix:        etcd.Prefix,
			CertFile:      etcd.CertFile,
			KeyFile:       etcd.KeyFile,
			CAFile:        etcd.CAFile,
			Timeout:       etcd.Timeout,
			SkipTLSVerify: etcd.SkipTLSVerify,
		},
		K8s: stconfig.K8sOptions{
			Config:       k8s.Config,
			ResourceKind: k8s.ResourceKind,
			ResourceName: k8s.ResourceName,
			Context:      k8s.Context,
			Namespace:    k8s.Namespace,
		},
	}
	applyRootGlobalOptions(commonConfig)
	return commonConfig
}

func (o managementCommonOptions) clusterOptions() (clusterStoreOption, k8sStoreOptions, []string) {
	return o.Store, o.K8s, o.ClusterNames
}

func applyRootGlobalOptions(commonConfig *stconfig.CommonConfig) {
	commonConfig.Log = cfg.Global.Log
	commonConfig.Debug = cfg.Global.Debug
}
