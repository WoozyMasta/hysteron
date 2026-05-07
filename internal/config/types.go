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

package config

import (
	"strings"
	"time"

	stlog "github.com/sorintlab/stolon/internal/log"
)

// CommonConfig groups options shared by unified CLI runtime and management
// command flows.
type CommonConfig struct {
	Metrics      MetricsOptions  `group:"Metrics"`
	K8s          K8sOptions      `group:"Kubernetes"`
	ClusterNames []string        `short:"c" long:"cluster-name" env:"CLUSTER_NAME" description:"cluster name. Can be repeated by components that support multiple clusters"`
	Log          stlog.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Store        StoreOptions    `group:"Store" namespace:"store" env-namespace:"STORE"`
	Debug        bool            `long:"debug" env:"DEBUG" hidden:"true" description:"deprecated: forces debug logging"`
}

// StoreOptions configures etcd v3 or kubernetes-backed cluster-data storage.
type StoreOptions struct {
	Backend       string        `long:"backend" env:"BACKEND" choices:"etcd;etcdv3;kubernetes;k8s" validate-non-empty:"true" description:"store backend type"`
	Endpoints     string        `long:"endpoints" env:"ENDPOINTS" description:"a comma-delimited list of store endpoints (use https scheme for tls communication) (defaults: http://127.0.0.1:2379 for etcdv3)"`
	Prefix        string        `long:"prefix" env:"PREFIX" default:"stolon/cluster" description:"the store base prefix"`
	CertFile      string        `long:"cert-file" env:"CERT_FILE" description:"certificate file for client identification to the store"`
	KeyFile       string        `long:"key" env:"KEY" description:"private key file for client identification to the store"`
	CAFile        string        `long:"ca-file" env:"CA_FILE" description:"verify certificates of HTTPS-enabled store servers using this CA bundle"`
	Timeout       time.Duration `long:"timeout" env:"TIMEOUT" default:"5s" description:"store request timeout"`
	SkipTLSVerify bool          `long:"skip-tls-verify" env:"SKIP_TLS_VERIFY" description:"skip store certificate verification (insecure!!!)"`
}

// MetricsOptions configures metrics endpoint options.
type MetricsOptions struct {
	ListenAddress string `long:"metrics-listen-address" env:"METRICS_LISTEN_ADDRESS" description:"metrics listen address i.e \"0.0.0.0:8080\" (disabled by default)"`
}

// K8sOptions configures kubernetes storage integration.
type K8sOptions struct {
	Config       string `long:"kubeconfig" env:"KUBECONFIG" description:"path to kubeconfig file. Overrides $KUBECONFIG"`
	ResourceKind string `long:"kube-resource-kind" env:"KUBE_RESOURCE_KIND" choices:"configmap;secret" description:"the k8s resource kind to be used to store stolon clusterdata"`
	ResourceName string `long:"kube-resource-name" env:"KUBE_RESOURCE_NAME" default:"stolon-{cluster}" description:"Kubernetes resource name template for cluster data and sentinel election objects; {cluster} is replaced with the cluster name"`
	Context      string `long:"kube-context" env:"KUBE_CONTEXT" description:"name of the kubeconfig context to use"`
	Namespace    string `long:"kube-namespace" env:"KUBE_NAMESPACE" description:"name of the kubernetes namespace to use"`
}

// ClusterNamesList returns normalized cluster names from CLI/env input.
func (cfg *CommonConfig) ClusterNamesList() []string {
	return normalizeClusterNames(cfg.ClusterNames)
}

// ClusterName returns the first configured cluster name.
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
