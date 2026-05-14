// Copyright 2018 Sorint.lab
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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	k8sutil "github.com/woozymasta/hysteron/internal/utils/k8s"

	jsonpatch "github.com/evanphx/json-patch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// ComponentLabelValue is the value of the component discriminator label.
type ComponentLabelValue string

const (
	// DefaultComponentLabel is the default label key used for component type.
	DefaultComponentLabel = "component"

	// KeeperLabelValue identifies keeper pods.
	KeeperLabelValue ComponentLabelValue = "hysteron-keeper"
	// SentinelLabelValue identifies sentinel pods.
	SentinelLabelValue ComponentLabelValue = "hysteron-sentinel"
	// ProxyLabelValue identifies proxy pods.
	ProxyLabelValue ComponentLabelValue = "hysteron-proxy"
)

// KubeStore stores cluster state in Kubernetes objects.
type KubeStore struct {
	// client is the Kubernetes API client.
	client kubernetes.Interface
	// podName is the current component pod name.
	podName string
	// namespace is Kubernetes namespace for all store objects.
	namespace string
	// clusterName is Hysteron cluster name.
	clusterName string
	// resourceKind is the Kubernetes resource kind used for cluster data.
	resourceKind string
	// resourceName is the Kubernetes resource name used for cluster data.
	resourceName string
}

// NewKubeStore creates a Kubernetes-backed store implementation.
func NewKubeStore(
	kubecli kubernetes.Interface,
	podName,
	namespace,
	clusterName,
	resourceKind,
	resourceName string,
) (*KubeStore, error) {
	switch resourceKind {
	case "configmap", "secret":
	default:
		return nil, fmt.Errorf("unsupported kubernetes resource kind %q", resourceKind)
	}

	return &KubeStore{
		client:       kubecli,
		podName:      podName,
		namespace:    namespace,
		clusterName:  clusterName,
		resourceKind: resourceKind,
		resourceName: resourceName,
	}, nil
}

// clusterLabels builds the base label set used for cluster-scoped objects.
func (s *KubeStore) clusterLabels() map[string]string {
	return map[string]string{
		k8sutil.KubeClusterLabel: s.clusterName,
	}
}

// labelSelector builds a selector for one component within current cluster.
func (s *KubeStore) labelSelector(componentLabel ComponentLabelValue) labels.Selector {
	selector := map[string]string{
		DefaultComponentLabel:    string(componentLabel),
		k8sutil.KubeClusterLabel: s.clusterName,
	}
	return labels.SelectorFromSet(selector)
}

// patchKubeStatusAnnotation atomically updates current pod status annotation.
func (s *KubeStore) patchKubeStatusAnnotation(ctx context.Context, annotationData []byte) error {
	podsClient := s.client.CoreV1().Pods(s.namespace)
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := podsClient.Get(ctx, s.podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get latest version of pod: %v", err)
		}

		oldPodJSON, err := json.Marshal(pod)
		if err != nil {
			return fmt.Errorf("failed to marshal pod: %v", err)
		}

		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[k8sutil.KubeStatusAnnnotation] = string(annotationData)

		newPodJSON, err := json.Marshal(pod)
		if err != nil {
			return fmt.Errorf("failed to marshal pod: %v", err)
		}

		patchBytes, err := jsonpatch.CreateMergePatch(oldPodJSON, newPodJSON)
		if err != nil {
			return fmt.Errorf("failed to create pod merge patch: %v", err)
		}

		_, err = podsClient.Patch(ctx, s.podName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
		return err
	})

	if retryErr != nil {
		return fmt.Errorf("update failed: %w", retryErr)
	}

	return nil
}

// SetKeeperInfo publishes keeper info.
func (s *KubeStore) SetKeeperInfo(ctx context.Context, _ string, ms *cluster.KeeperInfo, _ time.Duration) error {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "set_keeper_info", opErr)
	}()

	msj, err := json.Marshal(ms)
	if err != nil {
		opErr = err
		return err
	}

	opErr = s.patchKubeStatusAnnotation(ctx, msj)
	return opErr
}

// GetKeepersInfo lists published keeper info.
func (s *KubeStore) GetKeepersInfo(ctx context.Context) (cluster.KeepersInfo, error) {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "get_keepers_info", opErr)
	}()

	keepers, err := s.getKeepersInfoByComponent(ctx, KeeperLabelValue)
	opErr = err
	if err != nil {
		return nil, err
	}

	return keepers, nil
}

// SetSentinelInfo publishes sentinel info.
func (s *KubeStore) SetSentinelInfo(ctx context.Context, si *cluster.SentinelInfo, _ time.Duration) error {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "set_sentinel_info", opErr)
	}()

	sij, err := json.Marshal(si)
	if err != nil {
		opErr = err
		return err
	}

	opErr = s.patchKubeStatusAnnotation(ctx, sij)
	return opErr
}

// GetSentinelsInfo lists published sentinel info.
func (s *KubeStore) GetSentinelsInfo(ctx context.Context) (cluster.SentinelsInfo, error) {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "get_sentinels_info", opErr)
	}()

	ssi := cluster.SentinelsInfo{}

	podsClient := s.client.CoreV1().Pods(s.namespace)

	listOpts := metav1.ListOptions{
		LabelSelector: s.labelSelector(SentinelLabelValue).String(),
	}
	result, err := podsClient.List(ctx, listOpts)
	if err != nil {
		opErr = err
		return nil, fmt.Errorf("failed to get latest version of pod: %v", err)
	}

	pods := result.Items
	for _, pod := range pods {
		var si cluster.SentinelInfo
		if sij, ok := pod.Annotations[k8sutil.KubeStatusAnnnotation]; ok {
			err = json.Unmarshal([]byte(sij), &si)
			if err != nil {
				opErr = err
				return nil, err
			}
		}
		ssi = append(ssi, &si)
	}

	return ssi, nil
}

// SetProxyInfo publishes proxy info.
func (s *KubeStore) SetProxyInfo(ctx context.Context, pi *cluster.ProxyInfo, _ time.Duration) error {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "set_proxy_info", opErr)
	}()

	pij, err := json.Marshal(pi)
	if err != nil {
		opErr = err
		return err
	}

	opErr = s.patchKubeStatusAnnotation(ctx, pij)
	return opErr
}

// GetProxiesInfo lists published proxy info.
func (s *KubeStore) GetProxiesInfo(ctx context.Context) (cluster.ProxiesInfo, error) {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "get_proxies_info", opErr)
	}()

	psi, err := s.getProxiesInfoByComponent(ctx, ProxyLabelValue)
	opErr = err
	if err != nil {
		return nil, err
	}

	return psi, nil
}
