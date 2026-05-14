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

package keeper

import (
	"fmt"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// pgrewindDecision captures whether pg_rewind path should be attempted and why.
type pgrewindDecision struct {
	walCheckErr error  // Non-fatal WAL availability check error.
	reason      string // Decision reason constant.
	requiredWal string // WAL file required by local position.
	olderWal    string // Oldest WAL file available on source.
	try         bool   // Whether keeper should attempt pg_rewind.
}

const (
	pgrewindReasonNotInitialized = "not_initialized"
	pgrewindReasonSystemIDDiff   = "system_id_mismatch"
	pgrewindReasonNoMaster       = "no_master"
	pgrewindReasonWalCheckErr    = "wal_check_error"
	pgrewindReasonWalMissing     = "required_wal_missing"
	pgrewindReasonAllowed        = "allowed"
)

// usePgrewind reports whether keeper has credentials and config required to
// attempt pg_rewind.
func (p *PostgresKeeper) usePgrewind(db *cluster.DB) bool {
	return p.pgSUUsername != "" && p.pgSUPassword != "" &&
		db.Spec.UsePgrewind
}

// evaluatePgrewindDecision decides whether pg_rewind is safe to attempt for
// current local/followed state.
func evaluatePgrewindDecision(
	initialized bool,
	localSystemID string,
	followedSystemID string,
	hasMaster bool,
	dbXLogPos uint64,
	masterOlderWal string,
) pgrewindDecision {
	if !initialized {
		return pgrewindDecision{reason: pgrewindReasonNotInitialized}
	}
	if localSystemID != followedSystemID {
		return pgrewindDecision{reason: pgrewindReasonSystemIDDiff}
	}
	if !hasMaster {
		return pgrewindDecision{reason: pgrewindReasonNoMaster}
	}

	walAvailable, walErr := postgresql.IsRequiredWalAvailable(
		dbXLogPos,
		masterOlderWal,
		postgresql.WalSegSize,
	)
	if walErr != nil {
		// Keep warning-only behavior: inability to verify WAL availability
		// should not disable pg_rewind path.
		return pgrewindDecision{
			try:         true,
			reason:      pgrewindReasonWalCheckErr,
			walCheckErr: walErr,
		}
	}
	if !walAvailable {
		requiredWal := postgresql.XlogPosToWalFileNameNoTimeline(dbXLogPos, postgresql.WalSegSize)
		olderWal, _ := postgresql.WalFileNameNoTimeLine(masterOlderWal)
		return pgrewindDecision{
			try:         false,
			reason:      pgrewindReasonWalMissing,
			requiredWal: requiredWal,
			olderWal:    olderWal,
		}
	}

	return pgrewindDecision{try: true, reason: pgrewindReasonAllowed}
}

// resync converges local data directory from followed/master node using
// pg_rewind when possible, otherwise pg_basebackup.
func (p *PostgresKeeper) resync(
	db, masterDB, followedDB *cluster.DB,
	tryPgrewind bool,
) error {
	pgManager := p.pgm
	replConnParams := p.getReplConnParams(db, followedDB)
	standbySettings := &cluster.StandbySettings{
		PrimaryConninfo: replConnParams.ConnString(),
		PrimarySlotName: common.HysteronName(db.UID),
	}

	// We intentionally do not hard-fail on pg_rewind capability checks here.
	// If pg_rewind is missing or unusable, SyncFromFollowedPGRewind returns an
	// error and we fall back to pg_basebackup.
	if tryPgrewind && p.usePgrewind(db) {
		// pg_rewind doesn't support running against a database that is in recovery, as it
		// builds temporary tables and this is not supported on a hot-standby. Hysteron doesn't
		// currently support cascading replication, but we should be clear when issuing a
		// rewind that it targets the current primary, rather than whatever database we
		// follow.
		connParams := p.getSUConnParams(db, masterDB)
		p.baseLog().Info().
			Str(log.FieldDBUID, masterDB.UID).
			Str(log.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Msg("attempting pg_rewind against current primary to sync data directory")
		pgrewindStart := time.Now()
		if err := pgManager.SyncFromFollowedPGRewind(connParams, p.pgSUPassword); err != nil {
			pgrewindDurationSeconds.Observe(time.Since(pgrewindStart).Seconds())
			pgrewindTotal.WithLabelValues("error").Inc()
			// log pg_rewind error and fallback to pg_basebackup
			p.baseLog().
				Error().
				Err(err).
				Msg("error syncing with pg_rewind")
		} else {
			pgrewindDurationSeconds.Observe(time.Since(pgrewindStart).Seconds())
			pgrewindTotal.WithLabelValues("success").Inc()
			pgManager.SetRecoveryOptions(p.createRecoveryOptions(postgresql.RecoveryModeStandby, standbySettings, nil, nil))
			return nil
		}
	}

	_, _, err := p.binaryVersion()
	if err != nil {
		// in case we fail to parse the binary version then log it and just don't use replSlot
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
	}
	replSlot := common.HysteronName(db.UID)

	if err := pgManager.RemoveAll(); err != nil {
		return fmt.Errorf(
			"failed to remove the postgres data dir: %v",
			err,
		)
	}
	if log.IsDebug() {
		p.baseLog().Debug().
			Str(log.FieldDBUID, followedDB.UID).
			Str(log.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Str("repl_conn_params", fmt.Sprintf("%v", replConnParams)).
			Msg("starting base backup / clone from followed PostgreSQL instance")
	} else {
		p.baseLog().Info().
			Str(log.FieldDBUID, followedDB.UID).
			Str(log.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Msg("starting base backup / clone from followed PostgreSQL instance")
	}

	basebackupStart := time.Now()
	if err := pgManager.SyncFromFollowed(replConnParams, replSlot); err != nil {
		basebackupDurationSeconds.Observe(time.Since(basebackupStart).Seconds())
		basebackupTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("sync error: %v", err)
	}

	basebackupDurationSeconds.Observe(time.Since(basebackupStart).Seconds())
	basebackupTotal.WithLabelValues("success").Inc()
	p.baseLog().
		Info().
		Str(log.FieldDBUID, followedDB.UID).
		Msg("successfully cloned data directory from followed instance")

	return nil
}

// isDifferentTimelineBranch reports timeline-branch divergence against
// followed DB state and logs divergence reason.
func (p *PostgresKeeper) isDifferentTimelineBranch(
	followedDB *cluster.DB,
	pgState *cluster.PostgresState,
) bool {
	divergence := cluster.DetectTimelineBranchDivergence(
		followedDB.Status.TimelineID,
		followedDB.Status.TimelinesHistory,
		followedDB.Status.XLogPos,
		pgState.TimelineID,
		pgState.TimelinesHistory,
		pgState.XLogPos,
	)
	if !divergence.Different {
		return false
	}

	switch divergence.Reason {
	case cluster.TimelineDivergenceFollowedTimelineOlder:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("timeline", pgState.TimelineID).
			Msg("followed instance timeline < than our timeline")
	case cluster.TimelineDivergenceSameTimelineDifferentSwitchPoint:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("followedXlogpos", divergence.FollowedSwitchPoint).
			Interface("timeline", pgState.TimelineID).
			Interface("xlogpos", divergence.CurrentSwitchPoint).
			Msg("followed instance timeline forked at a different xlog pos than our timeline")
	case cluster.TimelineDivergenceFollowedForkedBeforeCurrentPosition:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("followedXlogpos", divergence.FollowedSwitchPoint).
			Interface("timeline", pgState.TimelineID).
			Interface("xlogpos", divergence.CurrentSwitchPoint).
			Msg("followed instance timeline forked before our current state")
	}

	return true
}
