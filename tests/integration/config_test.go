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

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/store"

	"github.com/google/uuid"
)

func getLogicalReplicationSlotsByName(q Querier) ([]string, error) {
	rows, err := q.Query(
		"select slot_name from pg_replication_slots where slot_type = 'logical' and temporary is false",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var slot string
		if err := rows.Scan(&slot); err != nil {
			return nil, err
		}
		out = append(out, slot)
	}
	return out, nil
}

func getLogicalSlotPluginAndDatabase(
	q Querier,
	slotName string,
) (string, string, error) {
	rows, err := q.Query(
		"select database, plugin from pg_replication_slots where slot_name = $1",
		slotName,
	)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", "", fmt.Errorf("slot %q not found", slotName)
	}
	var database, plugin string
	if err := rows.Scan(&database, &plugin); err != nil {
		return "", "", err
	}
	return database, plugin, nil
}

func getLogicalSlotFailoverFlag(q Querier, slotName string) (bool, error) {
	rows, err := q.Query(
		"select failover from pg_replication_slots where slot_name = $1 and slot_type = 'logical'",
		slotName,
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, fmt.Errorf("slot %q not found", slotName)
	}
	var failover bool
	if err := rows.Scan(&failover); err != nil {
		return false, err
	}
	return failover, nil
}

func getLogicalSlotConfirmedFlushLSN(q Querier, slotName string) (uint64, error) {
	rows, err := q.Query(
		"select coalesce(pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, 0) "+
			"from pg_replication_slots where slot_name = $1 and slot_type = 'logical'",
		slotName,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("slot %q not found", slotName)
	}
	var lsn int64
	if err := rows.Scan(&lsn); err != nil {
		return 0, err
	}
	if lsn < 0 {
		return 0, fmt.Errorf("slot %q returned negative lsn %d", slotName, lsn)
	}
	return uint64(lsn), nil
}

func waitLogicalSlotConfirmedFlushLSNGreaterThan(
	q Querier,
	slotName string,
	base uint64,
	timeout time.Duration,
) (uint64, error) {
	start := time.Now()
	var last uint64
	var lastErr error
	for time.Since(start) < timeout {
		last, lastErr = getLogicalSlotConfirmedFlushLSN(q, slotName)
		if lastErr == nil && last > base {
			return last, nil
		}
		time.Sleep(2 * time.Second)
	}
	return 0, fmt.Errorf(
		"timeout waiting logical slot %q confirmed_flush_lsn > %d, got %d, last err: %v",
		slotName,
		base,
		last,
		lastErr,
	)
}

func waitLogicalSlotConfirmedFlushLSNStable(
	q Querier,
	slotName string,
	base uint64,
	duration time.Duration,
) error {
	start := time.Now()
	for time.Since(start) < duration {
		current, err := getLogicalSlotConfirmedFlushLSN(q, slotName)
		if err != nil {
			return err
		}
		if current != base {
			return fmt.Errorf(
				"logical slot %q confirmed_flush_lsn changed unexpectedly: base=%d current=%d",
				slotName,
				base,
				current,
			)
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

func waitLogicalSlotConsumeChanges(
	q Querier,
	slotName string,
	timeout time.Duration,
) error {
	start := time.Now()
	var lastErr error
	for time.Since(start) < timeout {
		if _, err := q.Exec(
			"select count(*) from pg_logical_slot_get_changes($1, NULL, NULL)",
			slotName,
		); err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "SQLSTATE 55006") {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf(
		"timeout waiting to consume changes from logical slot %q, last err: %v",
		slotName,
		lastErr,
	)
}

func getServerVersionNum(q Querier) (int, error) {
	rows, err := q.Query("select current_setting('server_version_num')::int")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, fmt.Errorf("server_version_num not returned")
	}
	var versionNum int
	if err := rows.Scan(&versionNum); err != nil {
		return 0, err
	}
	return versionNum, nil
}

func getCurrentSetting(q Querier, name string) (string, error) {
	rows, err := q.Query("select current_setting($1)", name)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", fmt.Errorf("setting %q not returned", name)
	}
	var value string
	if err := rows.Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func waitCurrentSettingEquals(
	q Querier,
	name string,
	want string,
	timeout time.Duration,
) error {
	start := time.Now()
	var last string
	var lastErr error
	for time.Since(start) < timeout {
		last, lastErr = getCurrentSetting(q, name)
		if lastErr == nil && strings.EqualFold(strings.TrimSpace(last), want) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf(
		"timeout waiting setting %q=%q, got %q, last err: %v",
		name,
		want,
		last,
		lastErr,
	)
}

func waitLogicalReplicationSlotPresent(
	q Querier,
	slotName string,
	timeout time.Duration,
) error {
	start := time.Now()
	var last []string
	var lastErr error
	for time.Since(start) < timeout {
		last, lastErr = getLogicalReplicationSlotsByName(q)
		if lastErr == nil && slices.Contains(last, slotName) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf(
		"timeout waiting logical slot %q present, got: %v, last err: %v",
		slotName,
		last,
		lastErr,
	)
}

func waitLogicalReplicationSlotAbsent(
	q Querier,
	slotName string,
	timeout time.Duration,
) error {
	start := time.Now()
	var last []string
	var lastErr error
	for time.Since(start) < timeout {
		last, lastErr = getLogicalReplicationSlotsByName(q)
		if lastErr == nil && !slices.Contains(last, slotName) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf(
		"timeout waiting logical slot %q absent, got: %v, last err: %v",
		slotName,
		last,
		lastErr,
	)
}

func TestServerParameters(t *testing.T) {
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

	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "unexistent_parameter": "value" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.waitPostgresConfParam("unexistent_parameter", "value", 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()

	// Start tk again, postgres should fail to start due to bad parameter
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer tk.Stop()

	if err := tk.WaitDBDown(30 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Fix wrong parameters
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : null }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.WaitDBUp(30 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestWalLevel(t *testing.T) {
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

	// "archive" isn't an accepted wal_level
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_level": "archive" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err := tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walLevel := pgParameters["wal_level"]
	if walLevel != "replica" && walLevel != "hot_standby" {
		t.Fatalf("unexpected wal_level value: %q", walLevel)
	}

	// "logical" is an accepted wal_level
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_level": "logical" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.waitPostgresConfParam("wal_level", "logical", 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err = tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walLevel = pgParameters["wal_level"]
	if walLevel != "logical" {
		t.Fatalf("unexpected wal_level value: %q", walLevel)
	}
}

func TestWalKeepSegments(t *testing.T) {
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

	maj, _, err := tk.PGDataVersion()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if maj >= 13 {
		t.Skipf("skipping since postgres version %d >= 13", maj)
	}

	// "archive" isn't an accepted wal_level
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_level": "archive" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err := tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walKeepSegments := pgParameters["wal_keep_segments"]
	if walKeepSegments != "8" {
		t.Fatalf("unexpected wal_keep_segments value: %q", walKeepSegments)
	}

	// test setting a wal_keep_segments value greater than the default
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_keep_segments": "20" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.waitPostgresConfParam("wal_keep_segments", "20", 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err = tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walKeepSegments = pgParameters["wal_keep_segments"]
	if walKeepSegments != "20" {
		t.Fatalf("unexpected wal_keep_segments value: %q", walKeepSegments)
	}

	// test setting a wal_keep_segments value less than the default
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_keep_segments": "5" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tk.waitPostgresConfParam("wal_keep_segments", "5", 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err = tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walKeepSegments = pgParameters["wal_keep_segments"]
	if walKeepSegments != "8" {
		t.Fatalf("unexpected wal_keep_segments value: %q", walKeepSegments)
	}

	// test setting a bad wal_keep_segments value
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "wal_keep_segments": "badvalue" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	pgParameters, err = tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	walKeepSegments = pgParameters["wal_keep_segments"]
	if walKeepSegments != "8" {
		t.Fatalf("unexpected wal_keep_segments value: %q", walKeepSegments)
	}
}

func TestAlterSystem(t *testing.T) {
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

	expectedErr := `could not fsync file "postgresql.auto.conf": Invalid argument`
	if _, err := tk.Exec("alter system set archive_mode to on"); err != nil {
		if !strings.Contains(err.Error(), expectedErr) {
			t.Fatalf("expected err containing %q, got: %q", expectedErr, err)
		}
	} else {
		t.Fatalf("expected err: %q, got no error", expectedErr)
	}

	tk.Stop()
	if err := tk.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	pgParameters, err := tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v, ok := pgParameters["archive_mode"]; ok {
		t.Fatalf("expected archive_mode not defined, got value: %q", v)
	}
}

func TestAdditionalReplicationSlots(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()

	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 2, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	standby := standbys[0]

	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := write(t, master, 1, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	c, err := getLines(t, master)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if c != 1 {
		t.Fatalf("wrong number of lines, want: %d, got: %d", 1, c)
	}
	if err := waitLines(t, standby, 1, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var standbyDBUID string
	for _, db := range cd.DBs {
		if db.Spec.KeeperUID == standby.uid {
			standbyDBUID = db.UID
		}
	}

	// create additional replslots on master
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "additionalMasterReplicationSlots" : [ "replslot01", "replslot02" ] }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// no repl slot on standby
	if err := waitHysteronReplicationSlots(standby, []string{}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// remove replslot02
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "additionalMasterReplicationSlots" : [ "replslot01" ] }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// no repl slot on standby
	if err := waitHysteronReplicationSlots(standby, []string{}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// remove additional replslots on master
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "additionalMasterReplicationSlots" : null }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// no repl slot on standby
	if err := waitHysteronReplicationSlots(standby, []string{}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// create additional replslots on master
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "additionalMasterReplicationSlots" : [ "replslot01", "replslot02" ] }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// no repl slot on standby
	if err := waitHysteronReplicationSlots(standby, []string{}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Manually create a replication slot. It should not be dropped.
	if _, err := master.Exec("select pg_create_physical_replication_slot('manualreplslot')"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Manually create a replication slot starting with hysteron_ . It should be dropped.
	if _, err := master.Exec("select pg_create_physical_replication_slot('hysteron_manualreplslot')"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// check it here so we are sure the refresh slots function has already been called
	if err := waitNotHysteronReplicationSlots(master, []string{"manualreplslot"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Manually create a hysteron_* slot and mark it ignored; keeper must not drop it.
	if _, err := master.Exec("select pg_create_physical_replication_slot('hysteron_manualkeep')"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "ignoreMasterReplicationSlots" : [ "hysteron_manualkeep" ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02", "manualkeep"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Structured matcher ignore policy should behave like Patroni-style subset
	// matchers (name/type/database/plugin), including physical-slot name+type.
	if _, err := master.Exec("select pg_create_physical_replication_slot('hysteron_manualkeep2')"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"ignoreMasterReplicationSlots" : [ "hysteron_manualkeep" ],
			"ignoreMasterReplicationSlotMatchers" : [
				{ "name": "hysteron_manualkeep2", "type": "physical" }
			]
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02", "manualkeep", "manualkeep2"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Once ignore policy is removed, unmanaged hysteron_* slot must be dropped again.
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"ignoreMasterReplicationSlots" : null,
			"ignoreMasterReplicationSlotMatchers" : null
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID, "replslot01", "replslot02"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Stop the keeper process on master, should also stop the database
	t.Logf("Stopping current master keeper: %s", master.uid)
	master.Stop()

	// Wait for cluster data containing standby as master
	if err := WaitClusterDataMaster(standby.uid, sm, 30*time.Second); err != nil {
		t.Fatalf("expected master %q in cluster view", standby.uid)
	}
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// repl slot on standby which is the new master
	if err := waitHysteronReplicationSlots(standby, []string{"replslot01", "replslot02"}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestMemberReplicationSlotTTLGuardsXmin(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 2, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	standby := standbys[0]

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var standbyDBUID string
	for _, db := range cd.DBs {
		if db.Spec.KeeperUID == standby.uid {
			standbyDBUID = db.UID
		}
	}
	if standbyDBUID == "" {
		t.Fatal("standby db uid not found")
	}

	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "memberReplicationSlotTTL" : "8s" }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping standby keeper: %s", standby.uid)
	standby.Stop()

	// Slot should survive before TTL.
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID}, 5*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Even with TTL enabled, slot should survive in this degraded scenario and
	// must not be dropped prematurely by keeper policy.
	if err := waitHysteronReplicationSlots(master, []string{standbyDBUID}, 40*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestMemberReplicationSlotTTLCleansRemovedMemberDBSlot(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:                  cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:             &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:            &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:              &cluster.Duration{Duration: 15 * time.Second},
		ConvergenceTimeout:        &cluster.Duration{Duration: 30 * time.Second},
		DeadKeeperRemovalInterval: &cluster.Duration{Duration: 8 * time.Second},
		MaxStandbysPerSender:      cluster.Uint16P(1),
		MemberReplicationSlotTTL:  &cluster.Duration{Duration: 8 * time.Second},
		PGParameters:              defaultPGParameters,
	}

	tks, tss, tp, tstore := setupServersCustom(t, clusterName, dir, 2, 1, initialClusterSpec)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	oldStandby := standbys[0]

	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var oldStandbyDBUID string
	for _, db := range cd.DBs {
		if db.Spec.KeeperUID == oldStandby.uid {
			oldStandbyDBUID = db.UID
			break
		}
	}
	if oldStandbyDBUID == "" {
		t.Fatal("old standby db uid not found")
	}

	if err := waitHysteronReplicationSlots(master, []string{oldStandbyDBUID}, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Add spare keeper that can take over standby assignment after old standby stops.
	spare, err := NewTestKeeper(
		t,
		dir,
		clusterName,
		pgSUUsername,
		pgSUPassword,
		pgReplUsername,
		pgReplPassword,
		tstore.storeBackend,
		storeEndpoints,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	tks[spare.uid] = spare
	if err := spare.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterDataKeepers([]string{master.uid, oldStandby.uid, spare.uid}, sm, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping old standby keeper: %s", oldStandby.uid)
	oldStandby.Stop()

	waitKeeperReady(t, sm, spare)
	if err := spare.WaitDBRole(common.RoleStandby, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := WaitClusterDataKeepers([]string{master.uid, spare.uid}, sm, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	cd, _, err = sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var newStandbyDBUID string
	for _, db := range cd.DBs {
		if db.Spec.KeeperUID == spare.uid {
			newStandbyDBUID = db.UID
			break
		}
	}
	if newStandbyDBUID == "" {
		t.Fatal("new standby db uid not found")
	}

	// Old slot can survive briefly while orphan tracking starts.
	if err := waitHysteronReplicationSlots(master, []string{oldStandbyDBUID, newStandbyDBUID}, 20*time.Second); err != nil {
		t.Fatalf("unexpected err while waiting dual slots: %v", err)
	}

	// After TTL elapses, old slot must be removed while the new standby slot remains.
	if err := waitHysteronReplicationSlots(master, []string{newStandbyDBUID}, 120*time.Second); err != nil {
		t.Fatalf("expected old member slot cleanup after db removal, got err: %v", err)
	}
}

func TestAutomaticPgRestart(t *testing.T) {
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
	automaticPgRestart := true
	pgParameters := map[string]string{"max_connections": "100"}

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:           cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		AutomaticPgRestart: &automaticPgRestart,
		PGParameters:       pgParameters,
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

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "max_connections": "150" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Wait for restart to happen
	time.Sleep(20 * time.Second)

	rows, err := tk.Query("select setting from pg_settings where name = 'max_connections'")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var maxConnections int
		err = rows.Scan(&maxConnections)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if maxConnections != 150 {
			t.Errorf("expected max_connections %d is not equal to actual %d", 150, maxConnections)
		}
	}

	// Allow users to opt out
	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "automaticPgRestart" : false }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = HysteronCluster(t, clusterName, tstore.storeBackend, storeEndpoints, "update", "--patch", `{ "pgParameters" : { "max_connections": "200" } }`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Restart should not happen, but waiting in case it restarts
	time.Sleep(10 * time.Second)

	rows, err = tk.Query("select setting from pg_settings where name = 'max_connections'")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var maxConnections int
		err = rows.Scan(&maxConnections)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if maxConnections != 150 {
			t.Errorf("expected max_connections %d is not equal to actual %d", 150, maxConnections)
		}
	}
}

func TestManagedLogicalReplicationSlots(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		1,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, _ := waitMasterStandbysReady(t, sm, tks)

	slotName := "hysteron_logic01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic01", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": false, "managedLogicalReplicationSlots" : null }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotAbsent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestManagedLogicalReplicationSlotsMismatchNoDestructiveAction(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		1,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, _ := waitMasterStandbysReady(t, sm, tks)

	slotName := "hysteron_logic02"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic02", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Change desired plugin for existing slot. Keeper must not drop/recreate.
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic02", "database" : "postgres", "plugin" : "test_decoding" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Slot should still exist with old plugin (no destructive action).
	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	database, plugin, err := getLogicalSlotPluginAndDatabase(master, slotName)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if database != "postgres" || plugin != "pgoutput" {
		t.Fatalf(
			"expected existing logical slot to stay unchanged on mismatch, got database=%q plugin=%q",
			database,
			plugin,
		)
	}
}

func TestManagedLogicalReplicationSlotsIgnoreMatcher(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		1,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, _ := waitMasterStandbysReady(t, sm, tks)

	slotName := "hysteron_logic_ignore01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"managedLogicalReplicationSlots" : [
				{ "name" : "hysteron_logic_ignore01", "database" : "postgres", "plugin" : "pgoutput" }
			],
			"ignoreMasterReplicationSlotMatchers" : [
				{ "name": "hysteron_logic_ignore01", "type": "logical", "database": "postgres", "plugin": "pgoutput" }
			]
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotAbsent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("expected ignored logical slot to stay absent: %v", err)
	}

	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"managedLogicalReplicationSlots" : [
				{ "name" : "hysteron_logic_ignore01", "database" : "postgres", "plugin" : "pgoutput" }
			],
			"ignoreMasterReplicationSlotMatchers" : null
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("expected logical slot to be created after ignore removal: %v", err)
	}
}

func assertClusterUpdateFailsWith(
	t *testing.T,
	clusterName string,
	storeBackend store.Backend,
	storeEndpoints string,
	patch string,
	expectedErr string,
) {
	t.Helper()

	output, err := HysteronClusterOutput(
		t,
		clusterName,
		storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		patch,
	)
	if err == nil {
		t.Fatalf("expected cluster update to fail, got success; output=%q", output)
	}
	if !strings.Contains(output, expectedErr) {
		t.Fatalf("expected output containing %q, got: %q", expectedErr, output)
	}
}

func TestManagedLogicalReplicationSlotsRequiresLogicalWalLevel(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 1, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	_, _ = waitMasterStandbysReady(t, sm, tks)

	assertClusterUpdateFailsWith(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		`{ "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic03", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
		`managedLogicalReplicationSlots requires pgParameters.wal_level to be set to "logical"`,
	)
}

func TestLogicalSlotFailoverGateRequiresManagedSlots(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 1, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	_, _ = waitMasterStandbysReady(t, sm, tks)

	assertClusterUpdateFailsWith(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		`{ "enableLogicalSlotFailover": true }`,
		`enableLogicalSlotFailover requires managedLogicalReplicationSlots to be configured`,
	)
}

func TestLogicalSlotFailoverGateRequiresHotStandbyFeedbackEnabled(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 1, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	_, _ = waitMasterStandbysReady(t, sm, tks)

	assertClusterUpdateFailsWith(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		`{ "enableLogicalSlotFailover": true, "pgParameters": { "wal_level": "logical", "hot_standby_feedback": "off" }, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_hsf01", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
		`enableLogicalSlotFailover requires pgParameters.hot_standby_feedback to be enabled (on/true/1)`,
	)
}

func TestLogicalSlotFailoverGateForcesHotStandbyFeedbackOnWhenUnset(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 1, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	master, _ := waitMasterStandbysReady(t, sm, tks)

	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "pgParameters": { "wal_level": "logical" }, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_hsf02", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitCurrentSettingEquals(master, "hot_standby_feedback", "on", 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestFailsafeModeValidationAndUpdate(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(t, clusterName, dir, 1, 1, false, false, nil)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	_, _ = waitMasterStandbysReady(t, sm, tks)

	assertClusterUpdateFailsWith(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		`{
			"enableFailsafeMode": true,
			"failsafeProbeInterval": "1s",
			"failsafeProbeTimeout": "2s"
		}`,
		`failsafeProbeTimeout should be less than or equal to failsafeProbeInterval`,
	)

	assertClusterUpdateFailsWith(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		`{
			"enableFailsafeMode": true,
			"failsafeProbeInterval": "3s",
			"failsafeTTL": "2s"
		}`,
		`failsafeTTL should be greater than or equal to failsafeProbeInterval`,
	)

	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"enableFailsafeMode": true,
			"failsafeProbeInterval": "3s",
			"failsafeProbeTimeout": "1s",
			"failsafeMaxMissingPeers": 1,
			"failsafeTTL": "15s"
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateStandbyReadinessNoAction(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	slotName := "hysteron_logic_gate01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate01", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Under gate v1 standby path is readiness-only: no create/drop actions.
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping current master keeper: %s", master.uid)
	master.Stop()
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateCreatesNativeFailoverSlotWhenAvailable(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, _ := waitMasterStandbysReady(t, sm, tks)

	slotName := "hysteron_logic_native01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_native01", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	failoverEnabled, err := getLogicalSlotFailoverFlag(master, slotName)
	if err != nil {
		if strings.Contains(err.Error(), `column "failover" does not exist`) {
			versionNum, vErr := getServerVersionNum(master)
			if vErr != nil {
				t.Fatalf("unexpected err: %v; and failed to read server version: %v", err, vErr)
			}
			if versionNum >= 170000 {
				t.Fatalf("unexpected missing failover column on pg version %d: %v", versionNum, err)
			}
			// Legacy path (<17): native logical failover slots are unavailable.
			return
		}
		t.Fatalf("unexpected err: %v", err)
	}
	if !failoverEnabled {
		t.Fatalf("expected logical slot %q failover=true when gate is enabled", slotName)
	}
}

func TestLogicalSlotFailoverGateStandbyAdvanceWhenSlotExistsOnStandby(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	versionNum, err := getServerVersionNum(standby)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versionNum < 160000 {
		t.Skipf("requires PostgreSQL 16+ for logical slot on standby, got %d", versionNum)
	}

	slotName := "hysteron_logic_adv01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_adv01", "database" : "postgres", "plugin" : "test_decoding" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := standby.Exec(
		"select * from pg_create_logical_replication_slot($1, $2)",
		slotName,
		"test_decoding",
	); err != nil {
		t.Fatalf("failed to create standby logical slot: %v", err)
	}
	standbyBefore, err := getLogicalSlotConfirmedFlushLSN(standby, slotName)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := write(t, master, 1, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalSlotConsumeChanges(master, slotName, 20*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, err := waitLogicalSlotConfirmedFlushLSNGreaterThan(
		standby,
		slotName,
		standbyBefore,
		60*time.Second,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateStandbyAdvanceUnavailableOnLegacyPG(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	versionNum, err := getServerVersionNum(standby)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versionNum >= 160000 {
		t.Skipf("legacy-only test (PG<16), got %d", versionNum)
	}

	slotName := "hysteron_logic_adv_legacy01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_adv_legacy01", "database" : "postgres", "plugin" : "test_decoding" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// PostgreSQL <16 cannot create logical slots on standby. Validate that
	// standby slot remains absent when failover gate is enabled.
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 20*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := write(t, master, 1, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalSlotConsumeChanges(master, slotName, 20*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 15*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateStandbyAdvanceDisabledByNoStream(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	versionNum, err := getServerVersionNum(standby)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versionNum < 160000 {
		t.Skipf("requires PostgreSQL 16+ for standby logical slot creation, got %d", versionNum)
	}

	slotName := "hysteron_logic_adv_nostream01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{
			"enableLogicalSlotFailover": true,
			"standbyConfig": { "noStream": true },
			"managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_adv_nostream01", "database" : "postgres", "plugin" : "test_decoding" } ]
		}`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := standby.Exec(
		"select * from pg_create_logical_replication_slot($1, $2)",
		slotName,
		"test_decoding",
	); err != nil {
		t.Fatalf("failed to create standby logical slot: %v", err)
	}
	standbyBefore, err := getLogicalSlotConfirmedFlushLSN(standby, slotName)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := write(t, master, 1, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalSlotConsumeChanges(master, slotName, 20*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalSlotConfirmedFlushLSNStable(
		standby,
		slotName,
		standbyBefore,
		20*time.Second,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateStandbyAdvanceRecoversAfterStoreOutage(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	versionNum, err := getServerVersionNum(standby)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versionNum < 160000 {
		t.Skipf("requires PostgreSQL 16+ for logical slot on standby, got %d", versionNum)
	}

	slotName := "hysteron_logic_adv_store01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_adv_store01", "database" : "postgres", "plugin" : "test_decoding" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := standby.Exec(
		"select * from pg_create_logical_replication_slot($1, $2)",
		slotName,
		"test_decoding",
	); err != nil {
		t.Fatalf("failed to create standby logical slot: %v", err)
	}

	standbyBefore, err := getLogicalSlotConfirmedFlushLSN(standby, slotName)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	standbyAfterBaseline, err := driveAndWaitStandbySlotAdvance(
		t,
		master,
		standby,
		slotName,
		standbyBefore,
		1,
		60*time.Second,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tstore.Stop()
	if err := tstore.WaitDown(15 * time.Second); err != nil {
		t.Fatalf("error waiting on store down: %v", err)
	}

	if err := write(t, master, 2, 2); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalSlotConsumeChanges(master, slotName, 20*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := tstore.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := tstore.WaitUp(30 * time.Second); err != nil {
		t.Fatalf("error waiting on store up: %v", err)
	}

	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := master.WaitDBUp(30 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := standby.WaitDBUp(30 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, err := driveAndWaitStandbySlotAdvance(
		t,
		master,
		standby,
		slotName,
		standbyAfterBaseline,
		3,
		90*time.Second,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func driveAndWaitStandbySlotAdvance(
	t *testing.T,
	master *TestKeeper,
	standby Querier,
	slotName string,
	base uint64,
	writeID int,
	timeout time.Duration,
) (uint64, error) {
	t.Helper()

	if err := write(t, master, writeID, writeID); err != nil {
		return 0, err
	}
	if err := waitLogicalSlotConsumeChanges(master, slotName, 20*time.Second); err != nil {
		return 0, err
	}
	return waitLogicalSlotConfirmedFlushLSNGreaterThan(standby, slotName, base, timeout)
}

func TestManagedLogicalSlotsMasterOnlyWhenGateDisabled(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	slotName := "hysteron_logic_gate00"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate00", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping current master keeper: %s", master.uid)
	master.Stop()
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotPresent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateDisableTransition(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	slotName := "hysteron_logic_gate02"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate02", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Disable failover gate, but keep managed slots configured.
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": false, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate02", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping current master keeper: %s", master.uid)
	master.Stop()
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotPresent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateEnableTransition(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		2,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, standbys := waitMasterStandbysReady(t, sm, tks)
	if len(standbys) == 0 {
		t.Fatalf("expected at least one standby keeper")
	}
	standby := standbys[0]

	slotName := "hysteron_logic_gate03"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": false, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate03", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Enable failover gate while keeping managed slots configured.
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_gate03", "database" : "postgres", "plugin" : "pgoutput" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Gate-enabled v1 is still standby readiness-only.
	if err := waitLogicalReplicationSlotAbsent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	t.Logf("stopping current master keeper: %s", master.uid)
	master.Stop()
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitLogicalReplicationSlotPresent(standby, slotName, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestLogicalSlotFailoverGateRepeatedFailoverCycles(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		3,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.PGParameters = cluster.PGParameters{
				"wal_level": "logical",
			}
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)

	master, _ := waitMasterStandbysReady(t, sm, tks)

	versionNum, err := getServerVersionNum(master)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versionNum < 170000 {
		t.Skipf("requires PostgreSQL 17+ for native failover logical slots, got %d", versionNum)
	}

	slotName := "hysteron_logic_cycle01"
	err = HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "enableLogicalSlotFailover": true, "managedLogicalReplicationSlots" : [ { "name" : "hysteron_logic_cycle01", "database" : "postgres", "plugin" : "test_decoding" } ] }`,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if err := waitLogicalReplicationSlotPresent(master, slotName, 30*time.Second); err != nil {
		t.Fatalf("expected logical slot present on initial master: %v", err)
	}
	failoverEnabled, err := getLogicalSlotFailoverFlag(master, slotName)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !failoverEnabled {
		t.Fatalf("expected logical slot %q failover=true on initial master", slotName)
	}
	if err := populate(t, master); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	currentMaster := master
	seenMasters := map[string]struct{}{currentMaster.uid: {}}
	writeID := 10
	const cycles = 2

	for cycle := 0; cycle < cycles; cycle++ {
		if err := write(t, currentMaster, writeID, writeID); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := waitLogicalSlotConsumeChanges(currentMaster, slotName, 20*time.Second); err != nil {
			t.Fatalf("expected logical slot consume before failover: %v", err)
		}
		writeID++

		oldMasterUID := currentMaster.uid
		promotedUID := ""
		for attempt := 0; attempt < 3; attempt++ {
			err = HysteronFailover(
				t,
				clusterName,
				tstore.storeBackend,
				storeEndpoints,
				"keeper",
				"--keeper-uid",
				oldMasterUID,
			)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			start := time.Now()
			for time.Since(start) < 35*time.Second {
				cd, _, getErr := sm.GetClusterData(context.TODO())
				if getErr == nil && cd != nil && cd.Cluster.Status.Phase == cluster.ClusterPhaseNormal && cd.Cluster.Status.Master != "" {
					newMasterUID := cd.DBs[cd.Cluster.Status.Master].Spec.KeeperUID
					if newMasterUID != oldMasterUID {
						promotedUID = newMasterUID
						break
					}
				}
				time.Sleep(sleepInterval)
			}
			if promotedUID != "" {
				break
			}
		}
		if promotedUID == "" {
			t.Fatalf("expected master switch from %q", oldMasterUID)
		}

		currentMaster = tks[promotedUID]
		if err := currentMaster.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := waitLogicalReplicationSlotPresent(currentMaster, slotName, 40*time.Second); err != nil {
			t.Fatalf("expected logical slot present on promoted master: %v", err)
		}
		failoverEnabled, err = getLogicalSlotFailoverFlag(currentMaster, slotName)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !failoverEnabled {
			t.Fatalf("expected logical slot %q failover=true on promoted master", slotName)
		}

		if err := write(t, currentMaster, writeID, writeID); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := waitLogicalSlotConsumeChanges(currentMaster, slotName, 20*time.Second); err != nil {
			t.Fatalf("expected logical slot consume after failover: %v", err)
		}
		writeID++
		seenMasters[currentMaster.uid] = struct{}{}
	}

	if len(seenMasters) < 2 {
		t.Fatalf("expected failover to involve at least two masters, got %d", len(seenMasters))
	}
}

func TestBeforeStopCommandHook(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	hookMarkerPath := filepath.Join(dir, "before-stop-hook-ran")
	clusterName := uuid.NewString()
	automaticPgRestart := true
	tks, tss, tp, tstore := setupServers(
		t,
		clusterName,
		dir,
		1,
		1,
		false,
		false,
		nil,
		func(spec *cluster.ClusterSpec) {
			spec.AutomaticPgRestart = &automaticPgRestart
			spec.PGParameters = cluster.PGParameters{
				"max_connections": "100",
			}
			spec.BeforeStopCommand = fmt.Sprintf(
				"date +%%s%%N > %s",
				hookMarkerPath,
			)
		},
	)
	defer shutdown(tks, tss, tp, tstore)

	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	if err := WaitClusterPhase(sm, cluster.ClusterPhaseNormal, 60*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tk, _ := waitMasterStandbysReady(t, sm, tks)
	if err := tk.WaitDBUp(60 * time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	var markerBefore string
	start := time.Now()
	for {
		content, readErr := os.ReadFile(hookMarkerPath)
		if readErr == nil {
			markerBefore = strings.TrimSpace(string(content))
			if markerBefore != "" {
				break
			}
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("beforeStopCommand marker wasn't written before stop: %v", readErr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)
	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		storeEndpoints,
		"update",
		"--patch",
		`{ "pgParameters" : { "max_connections": "150" } }`,
	); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	start = time.Now()
	for {
		content, readErr := os.ReadFile(hookMarkerPath)
		if readErr == nil {
			markerAfter := strings.TrimSpace(string(content))
			if markerAfter == "" {
				t.Fatalf("beforeStopCommand marker is empty after stop")
			}
			if markerAfter != markerBefore {
				break
			}
		}
		if time.Since(start) > 40*time.Second {
			t.Fatalf(
				"beforeStopCommand marker did not change after restart-trigger update: before=%q err=%v",
				markerBefore,
				readErr,
			)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestAdvertise(t *testing.T) {
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

	// Start keeper with advertise config
	advertiseConfig := []string{"--pg-advertise-address=6.6.6.6", "--pg-advertise-port=6666"}
	tk, err := NewTestKeeper(t, dir, clusterName, pgSUUsername, pgSUPassword, pgReplUsername, pgReplPassword, tstore.storeBackend, storeEndpoints, advertiseConfig...)
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

	// Check actual postgres listen address and port
	pgParameters, err := tk.GetPGParameters()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if tk.pgListenAddress != pgParameters["listen_addresses"] || tk.pgPort != pgParameters["port"] {
		t.Fatalf("Expected postgres listen address and port to be %s and %s. Got %s and %s", tk.pgListenAddress, tk.pgPort, pgParameters["listen_addresses"], pgParameters["port"])
	}

	// Check advertised listen address and port
	cd, _, err := sm.GetClusterData(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	for _, db := range cd.DBs {
		if db.Status.ListenAddress != "6.6.6.6" || db.Status.Port != "6666" {
			t.Fatalf("Expected advertised address and port to be 6.6.6.6 and 6666. Got %s and %s", db.Status.ListenAddress, db.Status.Port)
		}
	}
}
