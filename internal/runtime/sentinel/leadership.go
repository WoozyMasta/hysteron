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
	"slices"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
	slicesutil "github.com/woozymasta/hysteron/internal/utils/slices"
)

// runLeadershipSanitySweep resets in-memory leader-loop caches after
// leadership pauses and rebuilds convergence tracking from current cluster data.
func (s *Sentinel) runLeadershipSanitySweep(cd *cluster.ClusterData) {
	s.keeperErrorTimers = make(map[string]time.Time)
	s.dbErrorTimers = make(map[string]time.Time)
	s.dbNotIncreasingXLogPos = make(map[string]int64)
	s.dbIncreasingXLogPosObservedAt = make(map[string]time.Time)
	s.keeperHealthySince = make(map[string]time.Time)
	s.autoFailbackLastSwitchAt = time.Time{}
	s.keeperInfoHistories = make(KeeperInfoHistories)
	s.dbConvergenceInfos = make(map[string]*DBConvergenceInfo)
	s.leaderRaceBackoffTimers = make(map[string]time.Time)
	s.forceFailedKeeperUIDs = make(map[string]struct{})
	s.proxyInfoHistories = make(ProxyInfoHistories)

	// Rebuild convergence tracking from current cluster data so post-pause
	// reconcile starts from a consistent in-memory state.
	s.updateDBConvergenceInfos(cd)
}

// shouldDelayLeaderRace applies a short backoff window before failover while
// candidate standbys still show recent WAL progress.
func (s *Sentinel) shouldDelayLeaderRace(
	failedMaster *cluster.DB,
	candidates []*cluster.DB,
	window time.Duration,
) bool {
	if s.leaderRaceBackoffTimers == nil {
		s.leaderRaceBackoffTimers = make(map[string]time.Time)
	}
	if s.dbIncreasingXLogPosObservedAt == nil {
		s.dbIncreasingXLogPosObservedAt = make(map[string]time.Time)
	}

	if failedMaster == nil || failedMaster.UID == "" {
		return false
	}
	if window <= 0 || len(candidates) == 0 {
		delete(s.leaderRaceBackoffTimers, failedMaster.UID)
		return false
	}

	for _, candidate := range candidates {
		lastIncreaseAt, ok := s.dbIncreasingXLogPosObservedAt[candidate.UID]
		if !ok {
			continue
		}
		if !s.isDBIncreasingXLogPos(candidate) {
			continue
		}
		if time.Since(lastIncreaseAt) >= window {
			continue
		}

		startedAt, ok := s.leaderRaceBackoffTimers[failedMaster.UID]
		if !ok {
			s.leaderRaceBackoffTimers[failedMaster.UID] = time.Now()
			return true
		}
		if time.Since(startedAt) < window {
			return true
		}

		delete(s.leaderRaceBackoffTimers, failedMaster.UID)
		return false
	}

	delete(s.leaderRaceBackoffTimers, failedMaster.UID)
	return false
}

// chooseBestNewMaster picks a failover target according to health, lag, and
// synchronous replication constraints.
func (s *Sentinel) chooseBestNewMaster(
	newcd *cluster.ClusterData,
	curMasterDB *cluster.DB,
	failInterval time.Duration,
) (*cluster.DB, bool) {
	bestNewMasters := s.findBestNewMasters(newcd, curMasterDB)
	if len(bestNewMasters) == 0 {
		s.log.Error().Msg("no eligible masters")
		return nil, true
	}

	_, forceFailRequested := s.forceFailedKeeperUIDs[curMasterDB.Spec.KeeperUID]
	if !forceFailRequested &&
		s.shouldDelayLeaderRace(curMasterDB, bestNewMasters, failInterval) {
		s.log.Warn().
			Str(slog.FieldDBUID, curMasterDB.UID).
			Dur("leader_race_backoff_window", failInterval).
			Msg("deferring leader race because standby WAL position is still advancing")
		return nil, false
	}

	synchronousReplicationEnabled := curMasterDB.Spec.SynchronousReplication
	if newcd != nil && newcd.Cluster != nil && newcd.Cluster.Spec != nil {
		clusterSpec := newcd.Cluster.DefSpec()
		if clusterSpec.SynchronousReplication != nil {
			synchronousReplicationEnabled = synchronousReplicationEnabled &&
				*clusterSpec.SynchronousReplication
		}
	}
	if !synchronousReplicationEnabled {
		return bestNewMasters[0], true
	}

	commonSyncStandbys := slicesutil.CommonElements(
		curMasterDB.Status.SynchronousStandbys,
		curMasterDB.Spec.SynchronousStandbys,
	)
	if len(commonSyncStandbys) == 0 {
		s.log.Warn().
			Strs("reported_sync_standbys", curMasterDB.Status.SynchronousStandbys).
			Strs("spec_sync_standbys", curMasterDB.Spec.SynchronousStandbys).
			Msg(
				"cannot choose synchronous standby since there are no " +
					"common elements between the latest master reported " +
					"synchronous standbys and the db spec ones",
			)
		s.log.Error().Msg("no eligible masters")
		return nil, false
	}

	for _, nm := range bestNewMasters {
		if slices.Contains(commonSyncStandbys, nm.UID) {
			return nm, true
		}
	}

	s.log.Warn().
		Strs("reported_sync_standbys", curMasterDB.Status.SynchronousStandbys).
		Strs("spec_sync_standbys", curMasterDB.Spec.SynchronousStandbys).
		Strs("common_sync_standbys", commonSyncStandbys).
		Interface("possible_masters", cluster.LogSummaryDBList(bestNewMasters)).
		Msg(
			"cannot choose synchronous standby: no match between " +
				"possible masters and usable synchronous standbys",
		)

	s.log.Error().Msg("no eligible masters")
	return nil, false
}
