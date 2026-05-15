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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/store"
)

func TestInit(t *testing.T) {
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

	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t.Logf("database is up")
}

func TestInitNewMerge(t *testing.T) {
	t.Parallel()
	testInitNew(t, true)
}

func TestInitNewNoMerge(t *testing.T) {
	t.Parallel()
	testInitNew(t, false)
}

func testInitNew(t *testing.T, merge bool) {
	clusterName := uuid.NewString()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)

	sm := store.NewKVBackedStore(tstore.store, storePath)

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 10 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		MergePgParameters:  &merge,
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
	tk, err := NewTestKeeper(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// max_connection should be set by initdb
	_, ok := cd.Cluster.Spec.PGParameters["max_connections"]
	if merge && !ok {
		t.Fatalf("expected max_connection set in cluster data pgParameters")
	}
	if !merge && ok {
		t.Fatalf("expected no max_connection set in cluster data pgParameters")
	}

	tk.Stop()
}

func TestInitExistingMerge(t *testing.T) {
	t.Parallel()
	testInitExisting(t, true)
}

func TestInitExistingNoMerge(t *testing.T) {
	t.Parallel()
	testInitExisting(t, false)
}

func testInitExisting(t *testing.T, merge bool) {
	clusterName := uuid.NewString()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)

	sm := store.NewKVBackedStore(tstore.store, storePath)

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		PGParameters: pgParametersWithDefaults(cluster.PGParameters{
			"archive_mode": "on",
		}),
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
	tk, err := NewTestKeeper(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := populate(t, tk); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := write(t, tk, 1, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Now initialize a new cluster with the existing keeper
	initialClusterSpec = &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeExisting),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		MergePgParameters:  &merge,
		PGParameters:       defaultPGParameters,
		ExistingConfig: &cluster.ExistingConfig{
			KeeperUID: tk.uid,
		},
	}
	initialClusterSpecFile, err = writeClusterSpec(dir, initialClusterSpec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("reinitializing cluster")
	// Initialize cluster with new spec
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "initialize", "-y", "-f", initialClusterSpecFile)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	c, err := getLines(t, tk)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if c != 1 {
		t.Fatalf("wrong number of lines, want: %d, got: %d", 1, c)
	}

	pgParameters, err := tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	v, ok := pgParameters["archive_mode"]
	if merge && v != "on" {
		t.Fatalf("expected archive_mode == on got %q", v)
	}
	if !merge && ok {
		t.Fatalf("expected archive_mode empty")
	}

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// max_connection should be set by initdb
	v, ok = cd.Cluster.Spec.PGParameters["archive_mode"]
	if merge && v != "on" {
		t.Fatalf("expected archive_mode == on got %q", v)
	}
	if !merge && ok {
		t.Fatalf("expected archive_mode empty")
	}

	tk.Stop()
}

func TestInitUsers(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	// Test pg-repl-username == pg-su-username but password different
	clusterName := uuid.NewString()
	tk, err := NewTestKeeper(t, dir, clusterName, "user01", "password01", "user01", "password02", tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Wait(30 * time.Second); err == nil {
		t.Fatal("expected keeper to exit when superuser and replication user passwords differ for the same username")
	}

	// Test pg-repl-username == pg-su-username
	clusterName = uuid.NewString()
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

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 30*time.Second); err != nil {
		t.Fatal("expected cluster in initializing phase")
	}

	tk2, err := NewTestKeeper(t, dir, clusterName, "user01", "password", "user01", "password", tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk2.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk2.Stop()
	if err := tk2.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk2.waitRoleAttributes("user01", true, true, 60*time.Second); err != nil {
		t.Fatalf("expected superuser to also have replication privileges: %v", err)
	}

	// Test pg-repl-username != pg-su-username and pg-su-password defined
	clusterName = uuid.NewString()
	storePath = filepath.Join(common.StorePrefix, clusterName)

	sm = store.NewKVBackedStore(tstore.store, storePath)

	ts2, err := NewTestSentinel(t, dir, clusterName, tstore.storeBackend, storeEndpoints, fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts2.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts2.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatal("expected cluster in initializing phase")
	}

	tk3, err := NewTestKeeper(t, dir, clusterName, "user01", "password", "user02", "password", tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk3.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk3.Stop()
	if err := tk3.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk3.waitRoleSuperuser("user01", 60*time.Second); err != nil {
		t.Fatalf("expected user01 to be a superuser: %v", err)
	}
	if err := tk3.waitRoleReplication("user02", 60*time.Second); err != nil {
		t.Fatalf("expected user02 to be a replication user: %v", err)
	}
}

func TestInitUsersSCRAMSHA256(t *testing.T) {
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

	ts, err := NewTestSentinel(
		t,
		dir,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatal("expected cluster in initializing phase")
	}

	suUsername := "scramsu"
	suPassword := "scram-su-password"
	replUsername := "scramrepl"
	replPassword := "scram-repl-password"
	tk, err := NewTestKeeper(
		t,
		dir,
		clusterName,
		suUsername,
		suPassword,
		replUsername,
		replPassword,
		tstore.storeBackend,
		storeEndpoints,
		"--pg-su-auth-method=scram-sha-256",
		"--pg-repl-auth-method=scram-sha-256",
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.expectConnect(suUsername, suPassword); err != nil {
		t.Fatalf("expected superuser password auth to work: %v", err)
	}
	if err := tk.expectConnect(replUsername, replPassword); err != nil {
		t.Fatalf("expected replication user password auth to work: %v", err)
	}

	pgHBAPath := filepath.Join(tk.dataDir, "postgres", "pg_hba.conf")
	pgHBABytes, err := os.ReadFile(pgHBAPath)
	if err != nil {
		t.Fatalf("unexpected err reading pg_hba.conf: %v", err)
	}
	pgHBA := string(pgHBABytes)
	if !strings.Contains(pgHBA, "host all all 0.0.0.0/0 scram-sha-256") {
		t.Fatalf("expected default IPv4 pg_hba rule with scram-sha-256, got:\n%s", pgHBA)
	}
	if !strings.Contains(pgHBA, "host all all ::0/0 scram-sha-256") {
		t.Fatalf("expected default IPv6 pg_hba rule with scram-sha-256, got:\n%s", pgHBA)
	}

	var suHash string
	rows, err := tk.Query("select rolpassword from pg_authid where rolname = $1", suUsername)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatalf("role %q not found", suUsername)
	}
	if err := rows.Scan(&suHash); err != nil {
		rows.Close()
		t.Fatalf("unexpected err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(suHash) < len("SCRAM-SHA-256$") || suHash[:len("SCRAM-SHA-256$")] != "SCRAM-SHA-256$" {
		t.Fatalf("expected %q password hash to be SCRAM, got %q", suUsername, suHash)
	}

	var replHash string
	rows, err = tk.Query("select rolpassword from pg_authid where rolname = $1", replUsername)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatalf("role %q not found", replUsername)
	}
	if err := rows.Scan(&replHash); err != nil {
		rows.Close()
		t.Fatalf("unexpected err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(replHash) < len("SCRAM-SHA-256$") || replHash[:len("SCRAM-SHA-256$")] != "SCRAM-SHA-256$" {
		t.Fatalf("expected %q password hash to be SCRAM, got %q", replUsername, replHash)
	}
}

func TestInitWithCustomWALDir(t *testing.T) {
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

	ts, err := NewTestSentinel(
		t,
		dir,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()

	walDir := filepath.Join(dir, "wal-dir")
	tk, err := NewTestKeeper(
		t,
		dir,
		clusterName,
		pgSUUsername,
		pgSUPassword,
		pgReplUsername,
		pgReplPassword,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--pg-wal-dir=%s", walDir),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgWalPath := filepath.Join(tk.dataDir, "postgres", "pg_wal")
	target, err := os.Readlink(pgWalPath)
	if err != nil {
		t.Fatalf("expected %s to be a symlink: %v", pgWalPath, err)
	}
	if !filepath.IsAbs(target) {
		target = filepath.Clean(filepath.Join(filepath.Dir(pgWalPath), target))
	}
	walDirAbs, err := filepath.Abs(walDir)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target != walDirAbs {
		t.Fatalf("unexpected pg_wal symlink target: got %q, want %q", target, walDirAbs)
	}
	if _, err := os.Stat(walDirAbs); err != nil {
		t.Fatalf("expected wal dir to exist: %v", err)
	}
}

func TestReinitWithCustomWALDirCleansWalDir(t *testing.T) {
	t.Parallel()

	clusterName := uuid.NewString()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
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

	ts, err := NewTestSentinel(
		t,
		dir,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()

	walDir := filepath.Join(dir, "external-wal")
	tk, err := NewTestKeeper(
		t,
		dir,
		clusterName,
		pgSUUsername,
		pgSUPassword,
		pgReplUsername,
		pgReplPassword,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--pg-wal-dir=%s", walDir),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	markerPath := filepath.Join(walDir, "reinit-marker")
	if err := os.WriteFile(markerPath, []byte("stale"), 0600); err != nil {
		t.Fatalf("unexpected err creating wal marker: %v", err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("expected marker to exist before reinit: %v", err)
	}

	reinitClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		PGParameters:       defaultPGParameters,
	}
	reinitClusterSpecFile, err := writeClusterSpec(dir, reinitClusterSpec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"initialize",
		"-y",
		"-f",
		reinitClusterSpecFile,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale wal marker to be removed during reinit, got: %v", err)
	}
}

func TestReinitCleansManagedTablespaceDir(t *testing.T) {
	t.Parallel()

	clusterName := uuid.NewString()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
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

	ts, err := NewTestSentinel(
		t,
		dir,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()

	tablespaceRoot := filepath.Join(dir, "managed-tblspc")
	tablespacePath := filepath.Join(tablespaceRoot, "ts1")
	if err := os.MkdirAll(tablespacePath, 0700); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk, err := NewTestKeeper(
		t,
		dir,
		clusterName,
		pgSUUsername,
		pgSUPassword,
		pgReplUsername,
		pgReplPassword,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--pg-tablespace-dir=%s", tablespaceRoot),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tablespacePathLiteral := strings.ReplaceAll(tablespacePath, "'", "''")
	if _, err := tk.Exec(
		fmt.Sprintf("CREATE TABLESPACE ts1 LOCATION '%s'", tablespacePathLiteral),
	); err != nil {
		t.Fatalf("unexpected err creating tablespace: %v", err)
	}

	markerPath := filepath.Join(tablespacePath, "reinit-ts-marker")
	if err := os.WriteFile(markerPath, []byte("stale"), 0600); err != nil {
		t.Fatalf("unexpected err creating tablespace marker: %v", err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("expected marker to exist before reinit: %v", err)
	}

	reinitClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:      &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:     &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:       &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout: &cluster.Duration{Duration: 30 * time.Second},
		PGParameters:       defaultPGParameters,
	}
	reinitClusterSpecFile, err := writeClusterSpec(dir, reinitClusterSpec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"initialize",
		"-y",
		"-f",
		reinitClusterSpecFile,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale tablespace marker to be removed during reinit, got: %v", err)
	}
}

func TestInitialClusterSpec(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	clusterName := uuid.NewString()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)

	sm := store.NewKVBackedStore(tstore.store, storePath)

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:               cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:          &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:         &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:           &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout:     &cluster.Duration{Duration: 30 * time.Second},
		SynchronousReplication: cluster.BoolP(true),
		PGParameters:           defaultPGParameters,
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

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatal("expected cluster in initializing phase")
	}

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !*cd.Cluster.Spec.SynchronousReplication {
		t.Fatal("expected cluster spec with SynchronousReplication enabled")
	}
}

func TestSentinelMultiCluster(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore := setupStore(t, dir)
	defer tstore.Stop()

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	clusterName1 := uuid.NewString()
	clusterName2 := uuid.NewString()

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

	ts, err := NewTestSentinel(
		t,
		dir,
		clusterName1,
		tstore.storeBackend,
		storeEndpoints,
		fmt.Sprintf("--cluster-name=%s", clusterName2),
		fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer ts.Stop()

	sm1 := store.NewKVBackedStore(tstore.store, filepath.Join(common.StorePrefix, clusterName1))
	sm2 := store.NewKVBackedStore(tstore.store, filepath.Join(common.StorePrefix, clusterName2))
	if err := WaitClusterPhase(sm1, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatalf("expected first cluster in initializing phase: %v", err)
	}
	if err := WaitClusterPhase(sm2, cluster.ClusterPhaseInitializing, 60*time.Second); err != nil {
		t.Fatalf("expected second cluster in initializing phase: %v", err)
	}

	tk1, err := NewTestKeeper(t, dir, clusterName1, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk1.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk1.Stop()

	tk2, err := NewTestKeeper(t, dir, clusterName2, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk2.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk2.Stop()

	if err := WaitClusterPhase(sm1, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("expected first cluster in normal phase: %v", err)
	}
	if err := WaitClusterPhase(sm2, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("expected second cluster in normal phase: %v", err)
	}
	if err := tk1.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("expected first cluster database up: %v", err)
	}
	if err := tk2.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("expected second cluster database up: %v", err)
	}

	gotClusterNames, err := ListClustersOutput(t, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("cluster list failed: %v", err)
	}
	slices.Sort(gotClusterNames)
	wantClusterNames := []string{clusterName1, clusterName2}
	slices.Sort(wantClusterNames)
	if !slices.Equal(gotClusterNames, wantClusterNames) {
		t.Fatalf("hysteron cluster list = %v, want %v", gotClusterNames, wantClusterNames)
	}
}

func TestExclusiveLock(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore, err := NewTestStore(t, dir)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(10 * time.Second); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}
	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	defer tstore.Stop()

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

	u := uuid.New()
	id := fmt.Sprintf("%x", u[:4])

	tk1, err := NewTestKeeperWithID(t, dir, id, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk1.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk1.Stop()

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk1.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk2, err := NewTestKeeperWithID(t, dir, id, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk2.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk2.Wait(30 * time.Second); err == nil {
		t.Fatal("expected second keeper with the same data directory to exit")
	}
}

func TestPasswordTrailingNewLine(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	tstore, err := NewTestStore(t, dir)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(10 * time.Second); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}
	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	defer tstore.Stop()

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

	u := uuid.New()
	id := fmt.Sprintf("%x", u[:4])

	pgSUPassword := "hysteron_superuserpassword\n"
	pgReplPassword := "hysteron_replpassword\n"

	tk, err := NewTestKeeperWithID(t, dir, id, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.waitDBUpWithCredentials(pgSUUsername, "hysteron_superuserpassword", 60*time.Second); err != nil {
		t.Fatalf("expected superuser trimmed password to work: %v", err)
	}
	if err := tk.expectConnect(pgReplUsername, "hysteron_replpassword"); err != nil {
		t.Fatalf("expected replication user trimmed password to work: %v", err)
	}
	tk.Stop()

	pgSUPassword = "hysteron_superuserpassword\n"
	pgReplPassword = "\n"

	tk, err = NewTestKeeperWithID(t, dir, id, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Wait(30 * time.Second); err == nil {
		t.Fatal("expected keeper to exit when replication password is empty after trimming")
	}

	pgSUPassword = "\n"
	pgReplPassword = "hysteron_replpassword\n"

	tk, err = NewTestKeeperWithID(t, dir, id, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.Wait(30 * time.Second); err == nil {
		t.Fatal("expected keeper to exit when superuser password is empty after trimming")
	}
}
