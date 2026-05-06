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

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/util"

	jsonpatch "github.com/evanphx/json-patch"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

// ComponentLabelValue is the value of the component discriminator label.
type ComponentLabelValue string

const (
	// DefaultComponentLabel is the default label key used for component type.
	DefaultComponentLabel = "component"

	// KeeperLabelValue identifies keeper pods.
	KeeperLabelValue ComponentLabelValue = "stolon-keeper"
	// SentinelLabelValue identifies sentinel pods.
	SentinelLabelValue ComponentLabelValue = "stolon-sentinel"
	// ProxyLabelValue identifies proxy pods.
	ProxyLabelValue ComponentLabelValue = "stolon-proxy"
)

// KubeStore stores cluster state in Kubernetes objects.
type KubeStore struct {
	// client is the Kubernetes API client.
	client kubernetes.Interface
	// podName is the current component pod name.
	podName string
	// namespace is Kubernetes namespace for all store objects.
	namespace string
	// clusterName is Stolon cluster name.
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

func (s *KubeStore) clusterLabels() map[string]string {
	return map[string]string{
		util.KubeClusterLabel: s.clusterName,
	}
}

func (s *KubeStore) labelSelector(componentLabel ComponentLabelValue) labels.Selector {
	selector := map[string]string{
		DefaultComponentLabel: string(componentLabel),
		util.KubeClusterLabel: s.clusterName,
	}
	return labels.SelectorFromSet(selector)
}

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
		pod.Annotations[util.KubeStatusAnnnotation] = string(annotationData)

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

// AtomicPutClusterData stores cluster data with optimistic concurrency.
func (s *KubeStore) AtomicPutClusterData(ctx context.Context, cd *cluster.ClusterData, previous *KVPair) (*KVPair, error) {
	cdj, err := json.Marshal(cd)
	if err != nil {
		return nil, err
	}
	if s.resourceKind == "secret" {
		return s.atomicPutSecretClusterData(ctx, cdj, previous)
	}
	return s.atomicPutConfigMapClusterData(ctx, cdj, previous)
}

func (s *KubeStore) atomicPutConfigMapClusterData(ctx context.Context, cdj []byte, previous *KVPair) (*KVPair, error) {
	epsClient := s.client.CoreV1().ConfigMaps(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := epsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of configmap: %v", err)
		}
		if !apierrors.IsNotFound(err) {
			// configmap exists

			if previous == nil {
				if result.Annotations != nil {
					_, ok := result.Annotations[util.KubeClusterDataAnnotation]
					if ok {
						// cd exists but previous is nil
						return ErrKeyModified
					}
				}
			}

			if previous != nil {
				if result.Annotations == nil {
					// empty annotations but previous isn't nil
					return ErrKeyModified
				}
				curcd, ok := result.Annotations[util.KubeClusterDataAnnotation]
				if ok {
					// check that the previous cd is the same as the current one in the
					// configmap annotation
					if string(previous.Value) != curcd {
						return ErrKeyModified
					}
				} else {
					// no cd but previous isn't nil
					return ErrKeyModified
				}
			}
			if result.Annotations == nil {
				result.Annotations = map[string]string{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Annotations[util.KubeClusterDataAnnotation] = string(cdj)
			_, err = epsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}
		// configmap does not exists

		// previous isn't nil but configmap doesn't exists
		if previous != nil {
			return ErrKeyModified
		}
		annotations := map[string]string{util.KubeClusterDataAnnotation: string(cdj)}
		_, err = epsClient.Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        s.resourceName,
				Annotations: annotations,
				Labels:      s.clusterLabels(),
			},
		}, metav1.CreateOptions{})
		return err
	})
	if retryErr != nil {
		return nil, fmt.Errorf("update failed: %w", retryErr)
	}
	return &KVPair{Value: cdj}, nil
}

func (s *KubeStore) atomicPutSecretClusterData(ctx context.Context, cdj []byte, previous *KVPair) (*KVPair, error) {
	secretsClient := s.client.CoreV1().Secrets(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := secretsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of secret: %v", err)
		}
		if !apierrors.IsNotFound(err) {
			if previous == nil {
				if _, ok := result.Data[util.KubeClusterDataKey]; ok {
					return ErrKeyModified
				}
			}

			if previous != nil {
				curcd, ok := result.Data[util.KubeClusterDataKey]
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
			result.Data[util.KubeClusterDataKey] = cdj
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
			Data: map[string][]byte{util.KubeClusterDataKey: cdj},
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
	cdj, err := json.Marshal(cd)
	if err != nil {
		return err
	}
	if s.resourceKind == "secret" {
		return s.putSecretClusterData(ctx, cdj)
	}
	return s.putConfigMapClusterData(ctx, cdj)
}

func (s *KubeStore) putConfigMapClusterData(ctx context.Context, cdj []byte) error {
	epsClient := s.client.CoreV1().ConfigMaps(s.namespace)

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := epsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get latest version of configmap: %v", err)
		}
		if !apierrors.IsNotFound(err) {
			// configmap exists
			if result.Annotations == nil {
				result.Annotations = map[string]string{}
			}
			if result.Labels == nil {
				result.Labels = map[string]string{}
			}
			maps.Copy(result.Labels, s.clusterLabels())
			result.Annotations[util.KubeClusterDataAnnotation] = string(cdj)
			_, err = epsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}
		// configmap does not exists
		annotations := map[string]string{util.KubeClusterDataAnnotation: string(cdj)}
		_, err = epsClient.Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        s.resourceName,
				Annotations: annotations,
				Labels:      s.clusterLabels(),
			},
		}, metav1.CreateOptions{})
		return err
	})
	if retryErr != nil {
		return fmt.Errorf("update failed: %w", retryErr)
	}
	return nil
}

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
			result.Data[util.KubeClusterDataKey] = cdj
			_, err = secretsClient.Update(ctx, result, metav1.UpdateOptions{})
			return err
		}
		_, err = secretsClient.Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:   s.resourceName,
				Labels: s.clusterLabels(),
			},
			Type: v1.SecretTypeOpaque,
			Data: map[string][]byte{util.KubeClusterDataKey: cdj},
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
	if s.resourceKind == "secret" {
		return s.getSecretClusterData(ctx)
	}
	return s.getConfigMapClusterData(ctx)
}

func (s *KubeStore) getConfigMapClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	epsClient := s.client.CoreV1().ConfigMaps(s.namespace)
	result, err := epsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get latest version of configmap: %v", err)
	}
	cdj, ok := result.Annotations[util.KubeClusterDataAnnotation]
	if !ok {
		return nil, nil, nil
	}

	var cd *cluster.ClusterData
	if err := json.Unmarshal([]byte(cdj), &cd); err != nil {
		return nil, nil, err
	}

	return cd, &KVPair{Value: []byte(cdj)}, nil
}

func (s *KubeStore) getSecretClusterData(ctx context.Context) (*cluster.ClusterData, *KVPair, error) {
	secretsClient := s.client.CoreV1().Secrets(s.namespace)
	result, err := secretsClient.Get(ctx, s.resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get latest version of secret: %v", err)
	}
	cdj, ok := result.Data[util.KubeClusterDataKey]
	if !ok {
		return nil, nil, nil
	}

	var cd *cluster.ClusterData
	if err := json.Unmarshal(cdj, &cd); err != nil {
		return nil, nil, err
	}

	return cd, &KVPair{Value: cdj}, nil
}

// SetKeeperInfo publishes keeper info.
func (s *KubeStore) SetKeeperInfo(ctx context.Context, _ string, ms *cluster.KeeperInfo, _ time.Duration) error {
	msj, err := json.Marshal(ms)
	if err != nil {
		return err
	}
	return s.patchKubeStatusAnnotation(ctx, msj)
}

// GetKeepersInfo lists published keeper info.
func (s *KubeStore) GetKeepersInfo(ctx context.Context) (cluster.KeepersInfo, error) {
	keepers := cluster.KeepersInfo{}

	podsClient := s.client.CoreV1().Pods(s.namespace)

	listOpts := metav1.ListOptions{
		LabelSelector: s.labelSelector(KeeperLabelValue).String(),
	}
	result, err := podsClient.List(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version of pod: %v", err)
	}

	pods := result.Items
	for _, pod := range pods {
		var ki cluster.KeeperInfo
		if kij, ok := pod.Annotations[util.KubeStatusAnnnotation]; ok {
			err = json.Unmarshal([]byte(kij), &ki)
			if err != nil {
				return nil, err
			}
			keepers[ki.UID] = &ki
		}
	}
	return keepers, nil
}

// SetSentinelInfo publishes sentinel info.
func (s *KubeStore) SetSentinelInfo(ctx context.Context, si *cluster.SentinelInfo, _ time.Duration) error {
	sij, err := json.Marshal(si)
	if err != nil {
		return err
	}
	return s.patchKubeStatusAnnotation(ctx, sij)
}

// GetSentinelsInfo lists published sentinel info.
func (s *KubeStore) GetSentinelsInfo(ctx context.Context) (cluster.SentinelsInfo, error) {
	ssi := cluster.SentinelsInfo{}

	podsClient := s.client.CoreV1().Pods(s.namespace)

	listOpts := metav1.ListOptions{
		LabelSelector: s.labelSelector(SentinelLabelValue).String(),
	}
	result, err := podsClient.List(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version of pod: %v", err)
	}

	pods := result.Items
	for _, pod := range pods {
		var si cluster.SentinelInfo
		if sij, ok := pod.Annotations[util.KubeStatusAnnnotation]; ok {
			err = json.Unmarshal([]byte(sij), &si)
			if err != nil {
				return nil, err
			}
		}
		ssi = append(ssi, &si)
	}
	return ssi, nil
}

// SetProxyInfo publishes proxy info.
func (s *KubeStore) SetProxyInfo(ctx context.Context, pi *cluster.ProxyInfo, _ time.Duration) error {
	pij, err := json.Marshal(pi)
	if err != nil {
		return err
	}
	return s.patchKubeStatusAnnotation(ctx, pij)
}

// GetProxiesInfo lists published proxy info.
func (s *KubeStore) GetProxiesInfo(ctx context.Context) (cluster.ProxiesInfo, error) {
	psi := cluster.ProxiesInfo{}

	podsClient := s.client.CoreV1().Pods(s.namespace)

	listOpts := metav1.ListOptions{
		LabelSelector: s.labelSelector(ProxyLabelValue).String(),
	}
	result, err := podsClient.List(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version of pod: %v", err)
	}

	pods := result.Items
	for _, pod := range pods {
		var pi cluster.ProxyInfo
		if pij, ok := pod.Annotations[util.KubeStatusAnnnotation]; ok {
			err = json.Unmarshal([]byte(pij), &pi)
			if err != nil {
				return nil, err
			}
			psi[pi.UID] = &pi
		}
	}
	return psi, nil
}

// KubeElection implements sentinel leader election via Kubernetes Lease locks.
type KubeElection struct {
	// client is the Kubernetes API client.
	client kubernetes.Interface
	// ctx is election run context.
	ctx context.Context
	// rl is the Lease resource lock.
	rl resourcelock.Interface
	// electedCh reports leadership acquisition/loss events.
	electedCh chan bool
	// errCh reports election errors.
	errCh chan error
	// cancel stops the election run context.
	cancel context.CancelFunc
	// podName is the local sentinel pod name.
	podName string
	// namespace is Kubernetes namespace containing the lease.
	namespace string
	// resourceName is lease object name.
	resourceName string
	// running reports whether campaign loop is running.
	running bool
}

// NewKubeElection creates a Kubernetes-backed election.
func NewKubeElection(kubecli kubernetes.Interface, podName, namespace, resourceName, candidateUID string) (*KubeElection, error) {
	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		namespace,
		resourceName,
		kubecli.CoreV1(),
		kubecli.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      candidateUID,
			EventRecorder: createRecorder(kubecli, "stolon-sentinel", namespace),
		})
	if err != nil {
		return nil, fmt.Errorf("error creating lock: %v", err)
	}

	return &KubeElection{
		client:       kubecli,
		podName:      podName,
		namespace:    namespace,
		resourceName: resourceName,
		rl:           rl,
	}, nil
}

// RunForElection starts campaigning and returns election and error channels.
func (e *KubeElection) RunForElection() (<-chan bool, <-chan error, error) {
	if e.running {
		return nil, nil, ErrElectionAlreadyRunning
	}

	e.electedCh = make(chan bool)
	e.errCh = make(chan error)
	e.ctx, e.cancel = context.WithCancel(context.Background())

	e.running = true
	go e.campaign()

	return e.electedCh, e.errCh, nil
}

// Stop stops election campaigning.
func (e *KubeElection) Stop() error {
	if !e.running {
		return ErrElectionNotRunning
	}
	e.cancel()
	e.running = false
	return nil
}

// Leader returns the current leader identity.
func (e *KubeElection) Leader() (string, error) {
	ler, _, err := e.rl.Get(context.Background())
	if err != nil {
		return "", fmt.Errorf("failed to get leader election record: %v", err)
	}
	if ler == nil {
		return "", nil
	}

	return ler.HolderIdentity, nil
}

func (e *KubeElection) campaign() {
	defer close(e.electedCh)
	defer close(e.errCh)

	for {
		e.electedCh <- false

		leaderelection.RunOrDie(e.ctx, leaderelection.LeaderElectionConfig{
			Lock:          e.rl,
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					e.electedCh <- true
				},
				OnStoppedLeading: func() {
					e.electedCh <- false
				},
			},
		})
	}
}

func createRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}
