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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package integration

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/store"
)

func TestMetricsFailoverDCSAndPendingRestart(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "hysteron")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)

	clusterName := uuid.NewString()
	tks, keeperMetrics, tss, sentinelMetrics, tp, tstore := setupServersWithMetrics(
		t,
		clusterName,
		dir,
		2,
		1,
	)
	defer shutdown(tks, tss, tp, tstore)

	storePath := filepath.Join(common.StorePrefix, clusterName)
	sm := store.NewKVBackedStore(tstore.store, storePath)
	master, standbys := waitMasterStandbysReady(t, sm, tks)
	standby := standbys[0]

	// Trigger failover and verify sentinel failover metric increments.
	master.Stop()
	if err := WaitClusterDataMaster(standby.uid, sm, 30*time.Second); err != nil {
		t.Fatalf("expected master %q in cluster view", standby.uid)
	}
	if err := standby.WaitDBRole(common.RoleMaster, nil, 30*time.Second); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := waitMetricSumAtLeast(
		sentinelMetrics,
		"hysteron_sentinel_failovers_total",
		map[string]string{"cluster_name": clusterName},
		1,
		30*time.Second,
	); err != nil {
		t.Fatalf("expected sentinel failover metric increment: %v", err)
	}

	// Force pending-restart on current master and verify keeper PG metric.
	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port),
		"update",
		"--patch",
		`{ "automaticPgRestart": false }`,
	); err != nil {
		t.Fatalf("cluster patch failed: %v", err)
	}
	if err := HysteronCluster(
		t,
		clusterName,
		tstore.storeBackend,
		fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port),
		"update",
		"--patch",
		`{ "pgParameters": { "max_connections": "150" } }`,
	); err != nil {
		t.Fatalf("cluster patch failed: %v", err)
	}
	if err := waitMetricAtLeast(
		keeperMetrics[standby.uid],
		"hysteron_pg_pending_restart",
		nil,
		1,
		30*time.Second,
	); err != nil {
		t.Fatalf("expected pending-restart metric on keeper %s: %v", standby.uid, err)
	}

	// Stop DCS, verify keeper degraded metric, then recover and verify reset.
	tstore.Stop()
	if err := tstore.WaitDown(20 * time.Second); err != nil {
		t.Fatalf("store down wait failed: %v", err)
	}
	if err := waitMetricAtLeast(
		keeperMetrics[standby.uid],
		"hysteron_keeper_dcs_degraded",
		nil,
		1,
		30*time.Second,
	); err != nil {
		t.Fatalf("expected keeper dcs degraded metric: %v", err)
	}

	if err := tstore.Start(); err != nil {
		t.Fatalf("store restart failed: %v", err)
	}
	if err := tstore.WaitUp(30 * time.Second); err != nil {
		t.Fatalf("store up wait failed: %v", err)
	}
	if err := waitMetricEquals(
		keeperMetrics[standby.uid],
		"hysteron_keeper_dcs_degraded",
		nil,
		0,
		30*time.Second,
	); err != nil {
		t.Fatalf("expected keeper dcs degraded reset: %v", err)
	}
}

func setupServersWithMetrics(
	t *testing.T,
	clusterName, dir string,
	numKeepers, numSentinels uint8,
) (
	testKeepers,
	map[string]string,
	testSentinels,
	string,
	*TestProxy,
	*TestStore,
) {
	tstore := setupStore(t, dir)
	storeEndpoints := fmt.Sprintf("%s:%s", tstore.listenAddress, tstore.port)

	initialClusterSpec := &cluster.ClusterSpec{
		InitMode:               cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
		SleepInterval:          &cluster.Duration{Duration: 2 * time.Second},
		RequestTimeout:         &cluster.Duration{Duration: 1 * time.Second},
		FailInterval:           &cluster.Duration{Duration: 5 * time.Second},
		ConvergenceTimeout:     &cluster.Duration{Duration: 30 * time.Second},
		MaxStandbyLag:          cluster.Uint32P(50 * 1024),
		SynchronousReplication: cluster.BoolP(false),
		PGParameters:           defaultPGParameters,
	}
	initialClusterSpecFile, err := writeClusterSpec(dir, initialClusterSpec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	tks := map[string]*TestKeeper{}
	keeperMetrics := map[string]string{}
	tss := map[string]*TestSentinel{}
	sentinelMetrics := ""

	for i := uint8(0); i < numSentinels; i++ {
		host, port, err := getFreePort(true, false)
		if err != nil {
			t.Fatalf("metrics port allocation failed: %v", err)
		}
		metricsAddr := net.JoinHostPort(host, port)
		ts, err := NewTestSentinel(
			t,
			dir,
			clusterName,
			tstore.storeBackend,
			storeEndpoints,
			fmt.Sprintf("--initial-cluster-spec=%s", initialClusterSpecFile),
			fmt.Sprintf("--metrics-listen-address=%s", metricsAddr),
		)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := ts.Start(); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		tss[ts.uid] = ts
		if sentinelMetrics == "" {
			sentinelMetrics = metricsAddr
		}
	}

	for i := uint8(0); i < numKeepers; i++ {
		host, port, err := getFreePort(true, false)
		if err != nil {
			t.Fatalf("metrics port allocation failed: %v", err)
		}
		metricsAddr := net.JoinHostPort(host, port)
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
			fmt.Sprintf("--metrics-listen-address=%s", metricsAddr),
		)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := tk.Start(); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		tks[tk.uid] = tk
		keeperMetrics[tk.uid] = metricsAddr
	}

	tp, err := NewTestProxy(
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
	if err := tp.Start(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	return tks, keeperMetrics, tss, sentinelMetrics, tp, tstore
}

func waitMetricAtLeast(
	metricsAddr, metricName string,
	labelFilter map[string]string,
	minValue float64,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, found, err := metricValue(metricsAddr, metricName, labelFilter)
		if err == nil && found && value >= minValue {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for metric %s >= %v", metricName, minValue)
}

func waitMetricEquals(
	metricsAddr, metricName string,
	labelFilter map[string]string,
	want float64,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, found, err := metricValue(metricsAddr, metricName, labelFilter)
		if err == nil && found && value == want {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for metric %s == %v", metricName, want)
}

func waitMetricSumAtLeast(
	metricsAddr, metricName string,
	labelFilter map[string]string,
	minValue float64,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, err := metricSum(metricsAddr, metricName, labelFilter)
		if err == nil && value >= minValue {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for metric sum %s >= %v", metricName, minValue)
}

func metricValue(metricsAddr, metricName string, labelFilter map[string]string) (float64, bool, error) {
	text, err := fetchMetrics(metricsAddr)
	if err != nil {
		return 0, false, err
	}
	for _, line := range strings.Split(text, "\n") {
		value, ok, err := parseMetricLine(line, metricName, labelFilter)
		if err != nil {
			return 0, false, err
		}
		if ok {
			return value, true, nil
		}
	}
	return 0, false, nil
}

func metricSum(metricsAddr, metricName string, labelFilter map[string]string) (float64, error) {
	text, err := fetchMetrics(metricsAddr)
	if err != nil {
		return 0, err
	}
	var sum float64
	for _, line := range strings.Split(text, "\n") {
		value, ok, err := parseMetricLine(line, metricName, labelFilter)
		if err != nil {
			return 0, err
		}
		if ok {
			sum += value
		}
	}
	return sum, nil
}

func fetchMetrics(metricsAddr string) (string, error) {
	url := "http://" + metricsAddr + "/metrics"
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func parseMetricLine(
	line, metricName string,
	labelFilter map[string]string,
) (float64, bool, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return 0, false, nil
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false, nil
	}

	nameAndLabels := fields[0]
	valueStr := fields[1]

	name := nameAndLabels
	labels := map[string]string{}
	if idx := strings.Index(nameAndLabels, "{"); idx >= 0 {
		name = nameAndLabels[:idx]
		end := strings.LastIndex(nameAndLabels, "}")
		if end <= idx {
			return 0, false, fmt.Errorf("invalid metric labels: %s", nameAndLabels)
		}
		labels = parseLabels(nameAndLabels[idx+1 : end])
	}
	if name != metricName {
		return 0, false, nil
	}
	for key, want := range labelFilter {
		if labels[key] != want {
			return 0, false, nil
		}
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func parseLabels(raw string) map[string]string {
	res := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return res
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		eq := strings.Index(part, "=")
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(part[:eq])
		val := strings.Trim(strings.TrimSpace(part[eq+1:]), `"`)
		res[key] = val
	}
	return res
}
