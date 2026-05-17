// Copyright 2017 Sorint.lab
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

//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/store"
)

func waitClusterPausedState(
	sm *store.KVBackedStore,
	wantPaused bool,
	timeout time.Duration,
) error {
	start := time.Now()
	for start.Add(timeout).After(time.Now()) {
		cd, _, err := sm.GetClusterData(context.TODO())
		if err != nil {
			return err
		}
		if cd != nil && cd.Cluster != nil && cd.Cluster.Status.Paused == wantPaused {
			return nil
		}
		time.Sleep(sleepInterval)
	}
	return fmt.Errorf("timeout waiting paused=%t", wantPaused)
}

func TestSentinelEnabledProxies(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	clusterName := uuid.NewString()

	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		PGParameters:       defaultPGParameters,
	}
	initialClusterSpecFile, err := writeClusterSpec(dir, initialClusterSpec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	ts, err := NewTestSentinel(t, dir, clusterName, tstore.storeBackend, storeEndpoints, fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()
	tk, err := NewTestKeeper(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	tp, err := NewTestProxy(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tp.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	waitKeeperReady(t, sm, tk)

	// the proxy should connect to the right master
	if err := tp.WaitRightMaster(tk, 3*cluster.DefaultProxyCheckInterval); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping sentinel")
	ts.Stop()

	t.Logf("starting sentinel")
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Wait until proxy routing confirms cluster state is served again.
	if err := tp.WaitRightMaster(tk, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("sentinel resumed cluster serving")

	if err := WaitClusterDataEnabledProxiesNum(sm, 1, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	enabledProxies := cd.Proxy.Spec.EnabledProxies

	// add another proxy
	tp2, err := NewTestProxy(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tp2.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// the proxy should connect to the right master
	if err := tp2.WaitRightMaster(tk, 3*cluster.DefaultProxyCheckInterval); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterDataEnabledProxiesNum(sm, 2, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cd, _, err = sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	enabledProxiesTwo := cd.Proxy.Spec.EnabledProxies
	if len(enabledProxiesTwo) != 2 {
		t.Fatalf("expected 2 enabled proxies, got %d", len(enabledProxiesTwo))
	}

	// freeze the proxy
	t.Logf("freezing proxy: %s", tp2.uid)
	if err := tp2.Freeze(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterDataEnabledProxiesNum(sm, 1, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterDataEnabledProxies(sm, enabledProxies, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// unfreeze the proxy
	t.Logf("resuming proxy: %s", tp2.uid)
	if err := tp2.Resume(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterDataEnabledProxiesNum(sm, 2, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterDataEnabledProxies(sm, enabledProxiesTwo, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestSentinelPauseGuardsAndManualSwitchover(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 2, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	masterUID, err := WaitClusterDataWithMaster(sm, 60*time.Second)
	if err != nil {
		t.Fatalf("expected a master in cluster view")
	}

	targetUID := ""
	for uid := range tks {
		if uid != masterUID {
			targetUID = uid
			break
		}
	}
	if targetUID == "" {
		t.Fatal("failed to find standby keeper for switchover")
	}

	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"pause",
		"--reason=integration-test",
		"--ttl=2m",
	); err != nil {
		t.Fatalf("pause failed: %v", err)
	}

	if err := waitClusterPausedState(sm, true, 30*time.Second); err != nil {
		t.Fatalf("pause state wasn't applied: %v", err)
	}

	if output, err := HysteronClusterOutput(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"switchover",
		fmt.Sprintf("--keeper-uid=%s", targetUID),
	); err == nil || !strings.Contains(output, "cluster is paused; resume first") {
		t.Fatalf("expected paused switchover error, got err=%v output=%q", err, output)
	}

	if output, err := HysteronFailoverOutput(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"target",
		fmt.Sprintf("--keeper-uid=%s", targetUID),
	); err == nil || !strings.Contains(output, "cluster is paused; resume first") {
		t.Fatalf("expected paused failover-target error, got err=%v output=%q", err, output)
	}

	if output, err := HysteronClusterOutput(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"reinit",
		fmt.Sprintf("--keeper-uid=%s", targetUID),
	); err == nil || !strings.Contains(output, "cluster is paused; resume first") {
		t.Fatalf("expected paused reinit error, got err=%v output=%q", err, output)
	}

	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"resume",
	); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if err := waitClusterPausedState(sm, false, 30*time.Second); err != nil {
		t.Fatalf("resume state wasn't applied: %v", err)
	}

	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"switchover",
		fmt.Sprintf("--keeper-uid=%s", targetUID),
	); err != nil {
		t.Fatalf("switchover failed after resume: %v", err)
	}

	if err := WaitClusterDataMaster(targetUID, sm, 60*time.Second); err != nil {
		t.Fatalf("expected switchover to target %s: %v", targetUID, err)
	}

	if output, err := HysteronClusterOutput(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"reinit",
		fmt.Sprintf("--keeper-uid=%s", targetUID),
	); err == nil || !strings.Contains(output, "cannot reinitialize current master database") {
		t.Fatalf("expected reinit master rejection, got err=%v output=%q", err, output)
	}
}
