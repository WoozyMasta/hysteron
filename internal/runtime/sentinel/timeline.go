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
	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
)

// isDifferentTimelineBranch reports whether db timeline diverged from followed
// DB timeline history in a way that makes it ineligible for failover.
func (s *Sentinel) isDifferentTimelineBranch(followedDB *cluster.DB, db *cluster.DB) bool {
	res := cluster.DetectTimelineBranchDivergence(
		followedDB.Status.TimelineID,
		followedDB.Status.TimelinesHistory,
		followedDB.Status.XLogPos,
		db.Status.TimelineID,
		db.Status.TimelinesHistory,
		db.Status.XLogPos,
	)

	if !res.Different {
		return false
	}

	switch res.Reason {
	case cluster.TimelineDivergenceFollowedTimelineOlder:
		s.log.Info().
			Uint64("followed_timeline", followedDB.Status.TimelineID).
			Uint64("db_timeline", db.Status.TimelineID).
			Msg("followed instance timeline < than our timeline")

	case cluster.TimelineDivergenceSameTimelineDifferentSwitchPoint:
		s.log.Info().
			Uint64("followed_timeline", followedDB.Status.TimelineID).
			Uint64("followed_xlog_pos", res.FollowedSwitchPoint).
			Uint64("db_timeline", db.Status.TimelineID).
			Uint64("db_xlog_pos", res.CurrentSwitchPoint).
			Msg("followed instance timeline forked at a different xlog pos than our timeline")

	case cluster.TimelineDivergenceFollowedForkedBeforeCurrentPosition:
		s.log.Info().
			Uint64("followed_timeline", followedDB.Status.TimelineID).
			Uint64("followed_xlog_pos", res.FollowedSwitchPoint).
			Uint64("db_timeline", db.Status.TimelineID).
			Uint64("db_xlog_pos", res.CurrentSwitchPoint).
			Msg("followed instance timeline forked before our current state")
	}

	return true
}

// isLagBelowMax reports whether standby WAL lag is within configured
// maxStandbyLag threshold.
func (s *Sentinel) isLagBelowMax(cd *cluster.ClusterData, curMasterDB, db *cluster.DB) bool {
	var lag uint64
	if curMasterDB.Status.XLogPos > db.Status.XLogPos {
		lag = curMasterDB.Status.XLogPos - db.Status.XLogPos
	}

	s.log.Debug().
		Uint64("master_xlog_pos", curMasterDB.Status.XLogPos).
		Uint64("db_xlog_pos", db.Status.XLogPos).
		Uint64("lag", lag).
		Msg("standby lag calculated")

	if lag > uint64(*cd.Cluster.DefSpec().MaxStandbyLag) {
		s.log.Info().
			Str(slog.FieldDBUID, db.UID).
			Uint64("db_xlog_pos", db.Status.XLogPos).
			Uint64("master_xlog_pos", curMasterDB.Status.XLogPos).
			Uint64("max_standby_lag", uint64(*cd.Cluster.DefSpec().MaxStandbyLag)).
			Msg("ignoring keeper because standby lag exceeds maximum")
		return false
	}

	return true
}
