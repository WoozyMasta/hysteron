// Copyright 2015 Sorint.lab
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

package sentinel

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/store"
)

// Sentinel computes and writes cluster state from observed keepers and proxies.
type Sentinel struct {
	// External cluster store client.
	e store.Store

	// Leader election backend.
	election store.Election

	// Optional Kubernetes Service publisher.
	kubeServicePublisher *kubeServicePublisher

	// Cluster name served by this sentinel runner.
	clusterName string

	// Parsed sentinel command configuration.
	cfg *config

	// Per-cluster sentinel logger.
	log zerolog.Logger

	// Completion channel for sentinel run loop.
	end chan bool

	// Optional bootstrap spec used for first cluster initialization.
	initialClusterSpec *cluster.ClusterSpec

	// Injectable UID generator for deterministic tests.
	UIDFn func() string

	// Injectable random chooser for deterministic tests.
	RandFn func(int) int

	// Keeper unhealthy timers keyed by keeper UID.
	keeperErrorTimers map[string]time.Time

	// DB unhealthy timers keyed by DB UID.
	dbErrorTimers map[string]time.Time

	// Timers for DBs not advancing WAL position.
	dbNotIncreasingXLogPos map[string]int64

	// Last observed timestamps of DB WAL position increases.
	dbIncreasingXLogPosObservedAt map[string]time.Time

	// Cached convergence tracking keyed by DB UID.
	dbConvergenceInfos map[string]*DBConvergenceInfo

	// Backoff timers for delayed leader race keyed by failed master DB UID.
	leaderRaceBackoffTimers map[string]time.Time

	// Keepers force-failed in the current reconciliation cycle.
	forceFailedKeeperUIDs map[string]struct{}

	// History of keeper heartbeats and state transitions.
	keeperInfoHistories KeeperInfoHistories

	// History of proxy heartbeats and state transitions.
	proxyInfoHistories ProxyInfoHistories

	// Sentinel instance UID.
	uid string

	// Previously observed leadership epoch counter.
	lastLeadershipCount uint

	// Current leadership epoch counter.
	leadershipCount uint

	// Main reconciliation loop sleep interval.
	sleepInterval time.Duration

	// Timeout for store and component requests.
	requestTimeout time.Duration

	// Guards cluster update/reconciliation execution.
	updateMutex sync.Mutex

	// Guards leader state and leadership counters.
	leaderMutex sync.Mutex

	// Current local leadership flag.
	leader bool

	// Marks whether DCS retrieval failures happened in current leadership epoch.
	dcsDegradedSeen bool
}

// KeeperInfoHistory tracks the latest keeper info observed by the sentinel.
type KeeperInfoHistory struct {
	// KeeperInfo is last keeper info snapshot.
	KeeperInfo *cluster.KeeperInfo
	// Seen reports whether keeper was seen in current loop.
	Seen bool
	// Timer is monotonic timestamp used for failure tracking.
	Timer time.Time
}

// KeeperInfoHistories maps keeper UID to keeper info history.
type KeeperInfoHistories map[string]*KeeperInfoHistory

// DeepCopy returns an independent copy of keeper info histories.
func (k KeeperInfoHistories) DeepCopy() (KeeperInfoHistories, error) {
	if k == nil {
		return nil, nil
	}

	out := make(KeeperInfoHistories, len(k))
	for uid, history := range k {
		if history == nil {
			out[uid] = nil
			continue
		}
		hCopy := *history
		hCopy.KeeperInfo = history.KeeperInfo.DeepCopy()
		out[uid] = &hCopy
	}

	return out, nil
}

// DBConvergenceInfo tracks convergence timing for a database generation.
type DBConvergenceInfo struct {
	// Generation is DB generation being tracked.
	Generation int64
	// Timer is monotonic timestamp when convergence tracking started.
	Timer time.Time
}

// ProxyInfoHistory tracks the latest proxy info observed by the sentinel.
type ProxyInfoHistory struct {
	// ProxyInfo is last proxy info snapshot.
	ProxyInfo *cluster.ProxyInfo
	// Timer is monotonic timestamp for proxy liveness tracking.
	Timer time.Time
}

// ProxyInfoHistories maps proxy UID to proxy info history.
type ProxyInfoHistories map[string]*ProxyInfoHistory

// DeepCopy returns an independent copy of proxy info histories.
func (p ProxyInfoHistories) DeepCopy() (ProxyInfoHistories, error) {
	if p == nil {
		return nil, nil
	}

	out := make(ProxyInfoHistories, len(p))
	for uid, history := range p {
		if history == nil {
			out[uid] = nil
			continue
		}

		hCopy := *history
		if history.ProxyInfo != nil {
			piCopy := *history.ProxyInfo
			hCopy.ProxyInfo = &piCopy
		}
		out[uid] = &hCopy
	}

	return out, nil
}
