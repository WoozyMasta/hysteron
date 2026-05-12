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
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/store"
	k8sutil "github.com/woozymasta/hysteron/internal/utils/k8s"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	// Register optional Kubernetes auth plugins.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

// NewKVStore creates the configured key-value backend client.
func NewKVStore(cfg *CommonConfig) (store.KVStore, error) {
	return store.NewKVStore(store.Config{
		Backend:       store.Backend(NormalizeStoreBackend(cfg.Store.Backend)),
		Endpoints:     cfg.Store.Endpoints,
		Timeout:       cfg.Store.Timeout,
		CertFile:      cfg.Store.CertFile,
		KeyFile:       cfg.Store.KeyFile,
		CAFile:        cfg.Store.CAFile,
		SkipTLSVerify: cfg.Store.SkipTLSVerify,
	})
}

// NewStore creates cluster-data store client for configured cluster.
func NewStore(cfg *CommonConfig, requirePod bool) (store.Store, error) {
	return NewStoreForCluster(cfg, cfg.ClusterName(), requirePod)
}

// NewStoreForCluster creates cluster-data store client for one cluster.
func NewStoreForCluster(
	cfg *CommonConfig,
	clusterName string,
	requirePod bool,
) (store.Store, error) {
	backend := NormalizeStoreBackend(cfg.Store.Backend)
	switch backend {
	case storeBackendEtcdV3:
		storePath := filepath.Join(cfg.Store.Prefix, clusterName)
		kvstore, err := NewKVStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("cannot create kv store: %v", err)
		}
		return store.NewKVBackedStore(kvstore, storePath), nil
	case storeBackendKubernetes:
		kubecli, podName, namespace, err := getKubeValues(cfg, requirePod)
		if err != nil {
			return nil, err
		}
		resourceName, err := KubeResourceNameForCluster(cfg, clusterName)
		if err != nil {
			return nil, err
		}
		s, err := store.NewKubeStore(
			kubecli,
			podName,
			namespace,
			clusterName,
			cfg.K8s.ResourceKind,
			resourceName,
		)
		if err != nil {
			return nil, fmt.Errorf("cannot create store: %v", err)
		}
		return s, nil
	}
	return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
}

// NewElection creates sentinel election backend for configured cluster.
func NewElection(cfg *CommonConfig, uid string) (store.Election, error) {
	return NewElectionForCluster(cfg, cfg.ClusterName(), uid)
}

// NewElectionForCluster creates sentinel election backend for one cluster.
func NewElectionForCluster(
	cfg *CommonConfig,
	clusterName, uid string,
) (store.Election, error) {
	backend := NormalizeStoreBackend(cfg.Store.Backend)
	switch backend {
	case storeBackendEtcdV3:
		storePath := filepath.Join(cfg.Store.Prefix, clusterName)
		kvstore, err := NewKVStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("cannot create kv store: %v", err)
		}
		return store.NewKVBackedElection(
			kvstore,
			filepath.Join(storePath, common.SentinelLeaderKey),
			uid,
			cfg.Store.Timeout,
		)
	case storeBackendKubernetes:
		kubecli, podName, namespace, err := getKubeValues(cfg, true)
		if err != nil {
			return nil, err
		}
		resourceName, err := KubeResourceNameForCluster(cfg, clusterName)
		if err != nil {
			return nil, err
		}
		return store.NewKubeElection(
			kubecli,
			podName,
			namespace,
			resourceName,
			clusterName,
			uid,
		)
	}
	return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
}

// ListClusters returns clusters visible in configured store.
func ListClusters(ctx context.Context, cfg *CommonConfig) ([]string, error) {
	backend := NormalizeStoreBackend(cfg.Store.Backend)
	switch backend {
	case storeBackendEtcdV3:
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
	case storeBackendKubernetes:
		kubecli, _, namespace, err := getKubeValues(cfg, false)
		if err != nil {
			return nil, err
		}
		clusterNames := map[string]struct{}{}
		switch cfg.K8s.ResourceKind {
		case "configmap":
			configMaps, err := kubecli.CoreV1().ConfigMaps(namespace).List(
				ctx,
				metav1.ListOptions{},
			)
			if err != nil {
				return nil, fmt.Errorf("cannot list cluster configmaps: %v", err)
			}
			for _, cm := range configMaps.Items {
				if _, ok := cm.Data[k8sutil.KubeClusterDataKey]; ok {
					if name := clusterNameFromKubeObject(cm.Name, cm.Labels); name != "" {
						clusterNames[name] = struct{}{}
					}
				}
			}
		case "secret":
			secrets, err := kubecli.CoreV1().Secrets(namespace).List(
				ctx,
				metav1.ListOptions{},
			)
			if err != nil {
				return nil, fmt.Errorf("cannot list cluster secrets: %v", err)
			}
			for _, secret := range secrets.Items {
				if _, ok := secret.Data[k8sutil.KubeClusterDataKey]; ok {
					if name := clusterNameFromKubeObject(secret.Name, secret.Labels); name != "" {
						clusterNames[name] = struct{}{}
					}
				}
			}
		default:
			return nil, fmt.Errorf(
				"unsupported kubernetes resource kind %q",
				cfg.K8s.ResourceKind,
			)
		}
		return sortedStringSet(clusterNames), nil
	default:
		return nil, fmt.Errorf("unknown store backend: %q", cfg.Store.Backend)
	}
}

// KubeResourceNameForCluster resolves kubernetes object name from template.
func KubeResourceNameForCluster(cfg *CommonConfig, clusterName string) (string, error) {
	name := strings.ReplaceAll(cfg.K8s.ResourceName, "{cluster}", clusterName)
	if errs := k8svalidation.IsDNS1123Label(name); len(errs) != 0 {
		return "", fmt.Errorf(
			"invalid kubernetes resource name %q: %s",
			name,
			strings.Join(errs, "; "),
		)
	}
	return name, nil
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
	clusterName, ok := strings.CutPrefix(name, k8sutil.KubeResourcePrefix+"-")
	if !ok || clusterName == "" {
		return "", false
	}
	return clusterName, true
}

func clusterNameFromKubeObject(name string, labels map[string]string) string {
	if clusterName := labels[k8sutil.KubeClusterLabel]; clusterName != "" {
		return clusterName
	}
	clusterName, ok := clusterNameFromKubeResourceName(name)
	if !ok {
		return ""
	}
	return clusterName
}

func sortedStringSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func getKubeValues(
	cfg *CommonConfig,
	requirePod bool,
) (*kubernetes.Clientset, string, string, error) {
	kubeClientConfig := k8sutil.NewKubeClientConfig(
		cfg.K8s.Config,
		cfg.K8s.Context,
		cfg.K8s.Namespace,
	)
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
		podName, err = k8sutil.PodName()
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
