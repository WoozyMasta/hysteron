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
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

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
	// clusterName is logical Hysteron cluster name used for metrics labeling.
	clusterName string
	// running reports whether campaign loop is running.
	running bool
}

// NewKubeElection creates a Kubernetes-backed election.
func NewKubeElection(
	kubecli kubernetes.Interface,
	podName, namespace, resourceName, clusterName, candidateUID string,
) (*KubeElection, error) {
	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		namespace,
		resourceName,
		kubecli.CoreV1(),
		kubecli.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      candidateUID,
			EventRecorder: createRecorder(kubecli, "hysteron-sentinel", namespace),
		})
	if err != nil {
		return nil, fmt.Errorf("error creating lock: %v", err)
	}

	return &KubeElection{
		client:       kubecli,
		podName:      podName,
		namespace:    namespace,
		resourceName: resourceName,
		clusterName:  clusterName,
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

// campaign runs the leader-election loop until context cancellation.
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
					if e.ctx.Err() == nil {
						dcsWatchResetsTotal.WithLabelValues(e.clusterName, "kubernetes", "election").Inc()
					}
					e.electedCh <- false
				},
			},
		})
	}
}

// createRecorder creates Kubernetes event recorder for election lock events.
func createRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.CoreV1().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}
