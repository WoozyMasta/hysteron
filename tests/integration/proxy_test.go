// Copyright 2015 Sorint.lab
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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	pg "github.com/woozymasta/hysteron/internal/postgresql"
	"github.com/woozymasta/hysteron/internal/store"
)

func TestProxyReadOnlyRouting(t *testing.T) {
	t.Parallel()

	env := setupReadOnlyProxyCluster(t)
	defer env.shutdown()

	standby := env.standby
	if err := waitReadOnlyProxyRecovery(env.readOnlyListenAddress, env.readOnlyPort, true, 30*time.Second); err != nil {
		t.Fatalf("expected read-only proxy to route to standby: %v", err)
	}

	standby.Stop()
	if err := standby.WaitDBDown(30 * time.Second); err != nil {
		t.Fatalf("expected standby database to stop: %v", err)
	}
	if err := waitReadOnlyProxyRecovery(env.readOnlyListenAddress, env.readOnlyPort, false, 30*time.Second); err != nil {
		t.Fatalf("expected read-only proxy to fall back to primary: %v", err)
	}
}

func TestProxyReadOnlyNoFallback(t *testing.T) {
	t.Parallel()

	env := setupReadOnlyProxyCluster(t, "--read-only-no-fallback")
	defer env.shutdown()

	if err := waitReadOnlyProxyRecovery(env.readOnlyListenAddress, env.readOnlyPort, true, 30*time.Second); err != nil {
		t.Fatalf("expected read-only proxy to route to standby: %v", err)
	}

	env.standby.Stop()
	if err := env.standby.WaitDBDown(30 * time.Second); err != nil {
		t.Fatalf("expected standby database to stop: %v", err)
	}
	if err := waitReadOnlyProxyUnavailable(env.readOnlyListenAddress, env.readOnlyPort, 30*time.Second); err != nil {
		t.Fatalf("expected read-only proxy to reject traffic without fallback: %v", err)
	}
}

func TestProxyReadOnlyIncludePrimary(t *testing.T) {
	t.Parallel()

	env := setupReadOnlyProxyCluster(t, "--read-only-include-primary")
	defer env.shutdown()

	if err := waitReadOnlyProxyRecoveryValues(env.readOnlyListenAddress, env.readOnlyPort, 30*time.Second, true, false); err != nil {
		t.Fatalf("expected read-only proxy to route to standby and primary: %v", err)
	}
}

func TestProxyReadOnlyOnly(t *testing.T) {
	t.Parallel()

	env := setupReadOnlyProxyCluster(t, "--disable-writable-listener")
	defer env.shutdown()

	if err := env.proxy.WaitNotListening(10 * time.Second); err != nil {
		t.Fatalf("expected writable proxy listener to be disabled: %v", err)
	}
	if err := waitReadOnlyProxyRecovery(env.readOnlyListenAddress, env.readOnlyPort, true, 30*time.Second); err != nil {
		t.Fatalf("expected read-only-only proxy to route to standby: %v", err)
	}
}

func TestProxyListening(t *testing.T) {
	t.Parallel()

	storeWaitTimeout := 30 * time.Second

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()

	tstore, err := NewTestStore(t, dir)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	tp, err := NewTestProxy(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tp.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tp.Stop()

	t.Logf("test proxy start with store down. Should not listen")
	// tp should not listen because it cannot talk with store
	if err := tp.WaitNotListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp not listening due to failed store communication, but it's listening.")
	}

	tp.Stop()

	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(storeWaitTimeout); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}
	defer func() {
		if tstore.cmd != nil {
			tstore.Stop()
		}
	}()

	storePath := filepath.Join(common.StorePrefix, clusterName)

	sm := store.NewKVBackedStore(tstore.store, storePath)

	cd := &cluster.ClusterData{
		FormatVersion: cluster.CurrentCDFormatVersion,
		Cluster: &cluster.Cluster{
			UID:        "01",
			Generation: 1,
			Spec: &cluster.ClusterSpec{
				InitMode:     cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
				FailInterval: &cluster.Duration{Duration: 10 * time.Second},
			},
			Status: cluster.ClusterStatus{
				CurrentGeneration: 1,
				Phase:             cluster.ClusterPhaseNormal,
				Master:            "01",
			},
		},
		Keepers: cluster.Keepers{
			"01": &cluster.Keeper{
				UID:  "01",
				Spec: &cluster.KeeperSpec{},
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
		},
		DBs: cluster.DBs{
			"01": &cluster.DB{
				UID:        "01",
				Generation: 1,
				ChangeTime: time.Time{},
				Spec: &cluster.DBSpec{
					KeeperUID: "01",
					Role:      common.RoleMaster,
					Followers: []string{"02"},
				},
				Status: cluster.DBStatus{
					Healthy:           false,
					CurrentGeneration: 1,
				},
			},
		},
		Proxy: &cluster.Proxy{
			Spec: cluster.ProxySpec{
				MasterDBUID: "01",
			},
		},
	}
	pair, err := sm.AtomicPutClusterData(context.TODO(), cd, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// test proxy start with the store up
	t.Logf("test proxy start with the store up. Should listen")
	if err := tp.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	t.Logf("test proxy error communicating with store. Should stop listening")
	// Stop store
	tstore.Stop()
	if err := tstore.WaitDown(10 * time.Second); err != nil {
		t.Fatalf("error waiting on store down: %v", err)
	}

	// tp should not listen because it cannot talk with the store
	if err := tp.WaitNotListening(cluster.DefaultProxyTimeout * 2); err != nil {
		t.Fatalf("expecting tp not listening due to failed store communication, but it's listening.")
	}

	t.Logf("test proxy communication with store restored. Should start listening")
	// Start store
	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(storeWaitTimeout); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}
	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	t.Logf("test proxy error communicating with store but restored before proxy check timeout. Should continue listening")
	// Stop store
	tstore.Stop()
	if err := tstore.WaitDown(10 * time.Second); err != nil {
		t.Fatalf("error waiting on store down: %v", err)
	}
	// wait less than DefaultProxyTimeout
	time.Sleep(cluster.DefaultProxyTimeout / 3)
	// Start store
	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(storeWaitTimeout); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}
	// tp should listen
	if ok := tp.CheckListening(); !ok {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}
	// wait proxy reading again from the store
	time.Sleep(2 * cluster.DefaultProxyCheckInterval)
	// tp should listen
	if ok := tp.CheckListening(); !ok {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	t.Logf("test proxyConf removed. Should continue listening")
	// remove proxyConf
	cd.Proxy.Spec.MasterDBUID = ""
	pair, err = sm.AtomicPutClusterData(context.TODO(), cd, pair)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	t.Logf("test proxyConf restored. Should continue listening")
	// Set proxyConf again
	cd.Proxy.Spec.MasterDBUID = "01"
	pair, err = sm.AtomicPutClusterData(context.TODO(), cd, pair)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	t.Logf("test clusterView removed. Should continue listening")
	// remove whole clusterview
	_, err = sm.AtomicPutClusterData(context.TODO(), nil, pair)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}

	// simulate the store in hang by freezing its process
	t.Logf("freezing the store: %s", tstore.uid)
	if err := tstore.Freeze(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should not listen because it cannot talk with the store
	if err := tp.WaitNotListening(cluster.DefaultProxyTimeout * 2); err != nil {
		t.Fatalf("expecting tp not listening due to failed store communication, but it's listening.")
	}

	// resume the store
	t.Logf("resuming the store: %s", tstore.uid)
	if err := tstore.Resume(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// tp should listen
	if err := tp.WaitListening(10 * time.Second); err != nil {
		t.Fatalf("expecting tp listening, but it's not listening.")
	}
}

func waitReadOnlyProxyRecovery(listenAddress, port string, want bool, timeout time.Duration) error {
	start := time.Now()
	var lastErr error
	var lastRecovery bool
	for time.Now().Add(-timeout).Before(start) {
		inRecovery, err := queryReadOnlyProxyRecovery(listenAddress, port)
		if err == nil {
			if inRecovery == want {
				return nil
			}
			lastRecovery = inRecovery
		} else {
			lastErr = err
		}
		time.Sleep(sleepInterval)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for pg_is_in_recovery=%t, last error: %w", want, lastErr)
	}
	return fmt.Errorf("timeout waiting for pg_is_in_recovery=%t, last value: %t", want, lastRecovery)
}

func waitReadOnlyProxyRecoveryValues(listenAddress, port string, timeout time.Duration, wants ...bool) error {
	remaining := make(map[bool]struct{}, len(wants))
	for _, want := range wants {
		remaining[want] = struct{}{}
	}

	start := time.Now()
	var lastErr error
	for time.Now().Add(-timeout).Before(start) {
		inRecovery, err := queryReadOnlyProxyRecovery(listenAddress, port)
		if err == nil {
			delete(remaining, inRecovery)
			if len(remaining) == 0 {
				return nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(sleepInterval)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for pg_is_in_recovery values, missing: %v, last error: %w", remaining, lastErr)
	}
	return fmt.Errorf("timeout waiting for pg_is_in_recovery values, missing: %v", remaining)
}

func waitReadOnlyProxyUnavailable(listenAddress, port string, timeout time.Duration) error {
	start := time.Now()
	var lastRecovery bool
	sawUnavailable := false
	for time.Now().Add(-timeout).Before(start) {
		inRecovery, err := queryReadOnlyProxyRecovery(listenAddress, port)
		if err != nil {
			sawUnavailable = true
			time.Sleep(sleepInterval)
			continue
		}
		if !inRecovery {
			return fmt.Errorf("read-only proxy routed to primary while fallback is disabled")
		}
		lastRecovery = inRecovery
		time.Sleep(sleepInterval)
	}
	if sawUnavailable {
		return nil
	}
	return fmt.Errorf("timeout waiting for read-only proxy to become unavailable, last pg_is_in_recovery: %t", lastRecovery)
}

func queryReadOnlyProxyRecovery(listenAddress, port string) (bool, error) {
	connParams := pg.ConnParams{
		"user":     pgSUUsername,
		"password": pgSUPassword,
		"host":     listenAddress,
		"port":     port,
		"dbname":   "postgres",
		"sslmode":  "disable",
	}
	db, err := sql.Open(pg.SQLDriverName, connParams.ConnString())
	if err != nil {
		return false, err
	}
	defer db.Close()

	var inRecovery bool
	if err := db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return false, err
	}
	return inRecovery, nil
}

type readOnlyProxyCluster struct {
	t                     *testing.T
	dir                   string
	readOnlyListenAddress string
	readOnlyPort          string
	keepers               testKeepers
	sentinels             testSentinels
	proxy                 *TestProxy
	store                 *TestStore
	master                *TestKeeper
	standby               *TestKeeper
}

func setupReadOnlyProxyCluster(t *testing.T, proxyArgs ...string) *readOnlyProxyCluster {
	t.Helper()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	clusterName := uuid.NewString()
	readOnlyListenAddress, readOnlyPort, err := getFreePort(true, false)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("unexpected err: %v", err)
	}

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:               cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:          &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:         &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:           &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout:     &cluster.Duration{Duration: 30 * time.Second},
		MaxStandbyLag:          cluster.Uint32P(50 * 1024),
		SynchronousReplication: cluster.BoolP(true),
		UsePgrewind:            cluster.BoolP(false),
		PGParameters:           defaultPGParameters,
	}
	args := []string{
		fmt.Sprintf("--read-only-listen-address=%s", readOnlyListenAddress),
		fmt.Sprintf("--read-only-port=%s", readOnlyPort),
	}
	args = append(args, proxyArgs...)
	tks, tss, tp, tstore := setupServersCustomWithProxyArgs(
		t,
		clusterName,
		dir,
		2,
		1,
		initialClusterSpec,
		args...,
	)

	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	standby := standbys[0]
	if err := WaitClusterDataSynchronousStandbys([]string{standby.uid}, sm, 60*time.Second); err != nil {
		shutdown(tks, tss, tp, tstore)
		os.RemoveAll(dir)
		t.Fatalf("expected synchronous standby on keeper %q in cluster data: %v", standby.uid, err)
	}
	xLogPos, err := GetXLogPos(master)
	if err != nil {
		shutdown(tks, tss, tp, tstore)
		os.RemoveAll(dir)
		t.Fatalf("failed to get master xlog position: %v", err)
	}
	if err := WaitClusterSyncedXLogPos([]*TestKeeper{master, standby}, xLogPos, sm, 30*time.Second); err != nil {
		shutdown(tks, tss, tp, tstore)
		os.RemoveAll(dir)
		t.Fatalf("expected standby to be synced with master: %v", err)
	}

	return &readOnlyProxyCluster{
		t:                     t,
		dir:                   dir,
		readOnlyListenAddress: readOnlyListenAddress,
		readOnlyPort:          readOnlyPort,
		keepers:               tks,
		sentinels:             tss,
		proxy:                 tp,
		store:                 tstore,
		master:                master,
		standby:               standby,
	}
}

func (e *readOnlyProxyCluster) shutdown() {
	shutdown(e.keepers, e.sentinels, e.proxy, e.store)
	os.RemoveAll(e.dir)
}
