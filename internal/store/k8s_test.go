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
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/woozymasta/hysteron/internal/cluster"
	k8sutil "github.com/woozymasta/hysteron/internal/utils/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestKubeStoreConfigMapClusterDataRoundTrip(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	store, err := NewKubeStore(kubecli, "pod-01", "default", "test", "configmap", "hysteron-cluster-test")
	if err != nil {
		t.Fatalf("NewKubeStore() error = %v", err)
	}

	initMode := cluster.ClusterInitModeNew
	cd := cluster.NewClusterData(&cluster.Cluster{
		UID: "cluster-01",
		Spec: &cluster.ClusterSpec{
			InitMode: &initMode,
		},
	})

	if err := store.PutClusterData(ctx, cd); err != nil {
		t.Fatalf("PutClusterData() error = %v", err)
	}

	got, kv, err := store.GetClusterData(ctx)
	if err != nil {
		t.Fatalf("GetClusterData() error = %v", err)
	}
	if kv == nil {
		t.Fatal("GetClusterData() returned nil KVPair")
	}
	if diff := cmp.Diff(cd, got); diff != "" {
		t.Fatalf("GetClusterData() mismatch (-want +got):\n%s", diff)
	}

	cm, err := kubecli.CoreV1().ConfigMaps("default").Get(ctx, "hysteron-cluster-test", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMaps.Get() error = %v", err)
	}
	if cm.Annotations[k8sutil.KubeClusterDataAnnotation] == "" {
		t.Fatalf("ConfigMap annotation %q is empty", k8sutil.KubeClusterDataAnnotation)
	}
	if cm.Labels[k8sutil.KubeClusterLabel] != "test" {
		t.Fatalf("ConfigMap cluster label = %q, want test", cm.Labels[k8sutil.KubeClusterLabel])
	}
}

func TestKubeStoreSecretClusterDataRoundTrip(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	store, err := NewKubeStore(kubecli, "pod-01", "default", "test", "secret", "custom-resource")
	if err != nil {
		t.Fatalf("NewKubeStore() error = %v", err)
	}

	initMode := cluster.ClusterInitModeNew
	cd := cluster.NewClusterData(&cluster.Cluster{
		UID: "cluster-01",
		Spec: &cluster.ClusterSpec{
			InitMode: &initMode,
		},
	})

	if err := store.PutClusterData(ctx, cd); err != nil {
		t.Fatalf("PutClusterData() error = %v", err)
	}

	got, kv, err := store.GetClusterData(ctx)
	if err != nil {
		t.Fatalf("GetClusterData() error = %v", err)
	}
	if kv == nil {
		t.Fatal("GetClusterData() returned nil KVPair")
	}
	if diff := cmp.Diff(cd, got); diff != "" {
		t.Fatalf("GetClusterData() mismatch (-want +got):\n%s", diff)
	}

	secret, err := kubecli.CoreV1().Secrets("default").Get(ctx, "custom-resource", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Secrets.Get() error = %v", err)
	}
	if len(secret.Data[k8sutil.KubeClusterDataKey]) == 0 {
		t.Fatalf("Secret data key %q is empty", k8sutil.KubeClusterDataKey)
	}
	if secret.Labels[k8sutil.KubeClusterLabel] != "test" {
		t.Fatalf("Secret cluster label = %q, want test", secret.Labels[k8sutil.KubeClusterLabel])
	}
}

func TestKubeStoreSecretAtomicPutClusterData(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	store, err := NewKubeStore(kubecli, "pod-01", "default", "test", "secret", "hysteron-cluster-test")
	if err != nil {
		t.Fatalf("NewKubeStore() error = %v", err)
	}

	cd := cluster.NewClusterData(&cluster.Cluster{UID: "cluster-01"})
	first, err := store.AtomicPutClusterData(ctx, cd, nil)
	if err != nil {
		t.Fatalf("AtomicPutClusterData() create error = %v", err)
	}
	if first == nil {
		t.Fatal("AtomicPutClusterData() returned nil KVPair")
	}
	if _, err := store.AtomicPutClusterData(ctx, cd, nil); !errors.Is(err, ErrKeyModified) {
		t.Fatalf("AtomicPutClusterData() duplicate error = %v, want %v", err, ErrKeyModified)
	}
	if _, err := store.AtomicPutClusterData(ctx, cd, first); err != nil {
		t.Fatalf("AtomicPutClusterData() update error = %v", err)
	}
}

func TestKubeElectionUsesLeaseLock(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	election, err := NewKubeElection(kubecli, "pod-01", "default", "hysteron-cluster-test", "sentinel-01")
	if err != nil {
		t.Fatalf("NewKubeElection() error = %v", err)
	}

	now := metav1.NewTime(time.Now())
	record := resourcelock.LeaderElectionRecord{
		HolderIdentity:       "sentinel-01",
		LeaseDurationSeconds: 15,
		AcquireTime:          now,
		RenewTime:            now,
	}
	if err := election.rl.Create(ctx, record); err != nil {
		t.Fatalf("ResourceLock.Create() error = %v", err)
	}

	lease, err := kubecli.CoordinationV1().Leases("default").Get(ctx, "hysteron-cluster-test", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Leases.Get() error = %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "sentinel-01" {
		t.Fatalf("Lease holder = %v, want sentinel-01", lease.Spec.HolderIdentity)
	}
}
