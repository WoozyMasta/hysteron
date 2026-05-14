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
	"maps"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	k8sutil "github.com/woozymasta/hysteron/internal/utils/k8s"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// AtomicPutClusterData stores cluster data with optimistic concurrency.
func (s *KubeStore) AtomicPutClusterData(ctx context.Context, cd *cluster.ClusterData, previous *KVPair) (*KVPair, error) {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "atomic_put_cluster_data", opErr)
	}()

	cdj, err := json.Marshal(cd)
	if err != nil {
		opErr = err
		return nil, err
	}
	if s.resourceKind == "secret" {
		pair, err := s.atomicPutSecretClusterData(ctx, cdj, previous)
		opErr = err
		return pair, err
	}
	pair, err := s.atomicPutConfigMapClusterData(ctx, cdj, previous)
	opErr = err
	return pair, err
}

// atomicPutConfigMapClusterData performs CAS-like write for configmap backend.
func (s *KubeStore) atomicPutConfigMapClusterData(ctx context.Context, cdj []byte, previous *KVPair) (*KVPair, error) {
	configMapsClient := s.client.CoreV1().ConfigMaps(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := configMapsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of configmap: %v", err)
		}
		if !apierrors.IsNotFound(err) {
			// configmap exists

			if previous == nil {
				if result.Data != nil {
					_, ok := result.Data[k8sutil.KubeClusterDataKey]
					if ok {
						// cd exists but previous is nil
						return ErrKeyModified
					}
				}
			}

			if previous != nil {
				if result.Data == nil {
					// empty data but previous isn't nil
					return ErrKeyModified
				}
				curcd, ok := result.Data[k8sutil.KubeClusterDataKey]
				if ok {
					// check that the previous cd is the same as the current one in the
					// configmap data key
					if string(previous.Value) != curcd {
						return ErrKeyModified
					}
				} else {
					// no cd but previous isn't nil
					return ErrKeyModified
				}
			}
			if result.Data == nil {
				result.Data = map[string]string{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Data[k8sutil.KubeClusterDataKey] = string(cdj)
			_, err = configMapsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}
		// configmap does not exists

		// previous isn't nil but configmap doesn't exists
		if previous != nil {
			return ErrKeyModified
		}
		_, err = configMapsClient.Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   s.resourceName,
				Labels: s.clusterLabels(),
			},
			Data: map[string]string{k8sutil.KubeClusterDataKey: string(cdj)},
		}, metav1.CreateOptions{})
		return err
	})

	if retryErr != nil {
		return nil, fmt.Errorf("update failed: %w", retryErr)
	}

	return &KVPair{Value: cdj}, nil
}

// atomicPutSecretClusterData performs CAS-like write for secret backend.
func (s *KubeStore) atomicPutSecretClusterData(ctx context.Context, cdj []byte, previous *KVPair) (*KVPair, error) {
	secretsClient := s.client.CoreV1().Secrets(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := secretsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of secret: %v", err)
		}
		if !apierrors.IsNotFound(err) {
			if previous == nil {
				if _, ok := result.Data[k8sutil.KubeClusterDataKey]; ok {
					return ErrKeyModified
				}
			}

			if previous != nil {
				curcd, ok := result.Data[k8sutil.KubeClusterDataKey]
				if !ok || string(previous.Value) != string(curcd) {
					return ErrKeyModified
				}
			}
			if result.Data == nil {
				result.Data = map[string][]byte{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Data[k8sutil.KubeClusterDataKey] = cdj
			_, err = secretsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}

		if previous != nil {
			return ErrKeyModified
		}
		_, err = secretsClient.Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:   s.resourceName,
				Labels: s.clusterLabels(),
			},
			Type: v1.SecretTypeOpaque,
			Data: map[string][]byte{k8sutil.KubeClusterDataKey: cdj},
		}, metav1.CreateOptions{})
		return err
	})

	if retryErr != nil {
		return nil, fmt.Errorf("update failed: %w", retryErr)
	}

	return &KVPair{Value: cdj}, nil
}

// PutClusterData stores cluster data without concurrency checks.
func (s *KubeStore) PutClusterData(ctx context.Context, cd *cluster.ClusterData) error {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "put_cluster_data", opErr)
	}()

	cdj, err := json.Marshal(cd)
	if err != nil {
		opErr = err
		return err
	}

	if s.resourceKind == "secret" {
		opErr = s.putSecretClusterData(ctx, cdj)
		return opErr
	}

	opErr = s.putConfigMapClusterData(ctx, cdj)
	return opErr
}

// putConfigMapClusterData writes cluster data without previous-value checks.
func (s *KubeStore) putConfigMapClusterData(ctx context.Context, cdj []byte) error {
	configMapsClient := s.client.CoreV1().ConfigMaps(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := configMapsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of configmap: %v", err)
		}

		if !apierrors.IsNotFound(err) {
			// configmap exists
			if result.Data == nil {
				result.Data = map[string]string{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Data[k8sutil.KubeClusterDataKey] = string(cdj)
			_, err = configMapsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}

		// configmap does not exists
		_, err = configMapsClient.Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   s.resourceName,
				Labels: s.clusterLabels(),
			},
			Data: map[string]string{k8sutil.KubeClusterDataKey: string(cdj)},
		}, metav1.CreateOptions{})
		return err
	})

	if retryErr != nil {
		return fmt.Errorf("update failed: %w", retryErr)
	}

	return nil
}

// putSecretClusterData writes cluster data without previous-value checks.
func (s *KubeStore) putSecretClusterData(ctx context.Context, cdj []byte) error {
	secretsClient := s.client.CoreV1().Secrets(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := secretsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of secret: %v", err)
		}

		if !apierrors.IsNotFound(err) {
			if result.Data == nil {
				result.Data = map[string][]byte{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Data[k8sutil.KubeClusterDataKey] = cdj
			_, err = secretsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}

		_, err = secretsClient.Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:   s.resourceName,
				Labels: s.clusterLabels(),
			},
			Type: v1.SecretTypeOpaque,
			Data: map[string][]byte{k8sutil.KubeClusterDataKey: cdj},
		}, metav1.CreateOptions{})
		return err
	})

	if retryErr != nil {
		return fmt.Errorf("update failed: %w", retryErr)
	}

	return nil
}

// GetClusterData loads cluster data from Kubernetes.
func (s *KubeStore) GetClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	start := time.Now()
	var opErr error
	defer func() {
		observeDCSOperation(start, s.clusterName, "kubernetes", "get_cluster_data", opErr)
	}()

	if s.resourceKind == "secret" {
		cd, pair, err := s.getSecretClusterData(ctx)
		opErr = err
		return cd, pair, err
	}
	cd, pair, err := s.getConfigMapClusterData(ctx)
	opErr = err
	return cd, pair, err
}

// getConfigMapClusterData loads cluster data from configmap backend.
func (s *KubeStore) getConfigMapClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	configMapsClient := s.client.CoreV1().ConfigMaps(s.namespace)
	result, err := configMapsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get latest version of configmap: %v", err)
	}
	cdj, ok := result.Data[k8sutil.KubeClusterDataKey]
	if !ok {
		return nil, nil, nil
	}

	var cd *cluster.ClusterData
	if err := json.Unmarshal([]byte(cdj), &cd); err != nil {
		return nil, nil, err
	}

	return cd, &KVPair{Value: []byte(cdj)}, nil
}

// getSecretClusterData loads cluster data from secret backend.
func (s *KubeStore) getSecretClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	secretsClient := s.client.CoreV1().Secrets(s.namespace)
	result, err := secretsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get latest version of secret: %v", err)
	}
	cdj, ok := result.Data[k8sutil.KubeClusterDataKey]
	if !ok {
		return nil, nil, nil
	}

	var cd *cluster.ClusterData
	if err := json.Unmarshal(cdj, &cd); err != nil {
		return nil, nil, err
	}

	return cd, &KVPair{Value: cdj}, nil
}
