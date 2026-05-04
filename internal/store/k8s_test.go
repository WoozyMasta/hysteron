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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestKubeStoreClusterDataRoundTrip(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	store, err := NewKubeStore(kubecli, "pod-01", "default", "test")
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

	cm, err := kubecli.CoreV1().ConfigMaps("default").Get(ctx, "stolon-cluster-test", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMaps.Get() error = %v", err)
	}
	if cm.Annotations[util.KubeClusterDataAnnotation] == "" {
		t.Fatalf("ConfigMap annotation %q is empty", util.KubeClusterDataAnnotation)
	}
}

func TestKubeElectionUsesLeaseLock(t *testing.T) {
	ctx := context.Background()
	kubecli := fake.NewSimpleClientset()
	election, err := NewKubeElection(kubecli, "pod-01", "default", "test", "sentinel-01")
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

	lease, err := kubecli.CoordinationV1().Leases("default").Get(ctx, "stolon-cluster-test", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Leases.Get() error = %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "sentinel-01" {
		t.Fatalf("Lease holder = %v, want sentinel-01", lease.Spec.HolderIdentity)
	}
}
