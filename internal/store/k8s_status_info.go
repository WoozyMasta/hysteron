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

	"github.com/woozymasta/hysteron/internal/cluster"
	k8sutil "github.com/woozymasta/hysteron/internal/utils/k8s"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getKeepersInfoByComponent decodes keeper info from pod status annotations.
func (s *KubeStore) getKeepersInfoByComponent(
	ctx context.Context,
	component ComponentLabelValue,
) (cluster.KeepersInfo, error) {
	annotations, err := s.listStatusAnnotationsByComponent(ctx, component)
	if err != nil {
		return nil, err
	}

	keepers := cluster.KeepersInfo{}
	for _, raw := range annotations {
		var info cluster.KeeperInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			return nil, err
		}
		keepers[info.UID] = &info
	}

	return keepers, nil
}

// getProxiesInfoByComponent decodes proxy info from pod status annotations.
func (s *KubeStore) getProxiesInfoByComponent(
	ctx context.Context,
	component ComponentLabelValue,
) (cluster.ProxiesInfo, error) {
	annotations, err := s.listStatusAnnotationsByComponent(ctx, component)
	if err != nil {
		return nil, err
	}

	proxies := cluster.ProxiesInfo{}
	for _, raw := range annotations {
		var info cluster.ProxyInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			return nil, err
		}
		proxies[info.UID] = &info
	}

	return proxies, nil
}

// listStatusAnnotationsByComponent lists raw status annotation payloads.
func (s *KubeStore) listStatusAnnotationsByComponent(
	ctx context.Context,
	component ComponentLabelValue,
) ([]string, error) {
	podsClient := s.client.CoreV1().Pods(s.namespace)
	listOpts := metav1.ListOptions{
		LabelSelector: s.labelSelector(component).String(),
	}
	result, err := podsClient.List(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version of pod: %v", err)
	}

	annotations := make([]string, 0, len(result.Items))
	for _, pod := range result.Items {
		if status, ok := pod.Annotations[k8sutil.KubeStatusAnnnotation]; ok {
			annotations = append(annotations, status)
		}
	}

	return annotations, nil
}
