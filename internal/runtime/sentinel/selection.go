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
	"fmt"
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	slog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

type dbType int
type dbValidity int
type dbStatus int

const (
	// dbTypePrimaryLine is a writer lineage root in sentinel topology logic.
	// It may map to role=master or to an external-follow standby root.
	dbTypePrimaryLine dbType = iota
	// dbTypeReplicaLine is an internal-follow replica in sentinel topology logic.
	dbTypeReplicaLine

	dbValidityValid dbValidity = iota
	dbValidityInvalid
	dbValidityUnknown

	dbStatusGood dbStatus = iota
	dbStatusFailed
	dbStatusConverging
)

// dbType classifies DB as primary line (master/external-follow) or replica line.
func (s *Sentinel) dbType(cd *cluster.ClusterData, dbUID string) (dbType, error) {
	db, ok := cd.DBs[dbUID]
	if !ok {
		return 0, fmt.Errorf("unknown db uid %q", dbUID)
	}

	switch db.Spec.Role {
	case common.RoleMaster:
		return dbTypePrimaryLine, nil

	case common.RoleStandby:
		if db.Spec.FollowConfig != nil &&
			db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
			return dbTypePrimaryLine, nil
		}
		return dbTypeReplicaLine, nil

	default:
		return 0, fmt.Errorf("invalid db role in spec for db %q", dbUID)
	}
}

// dbValidity reports whether DB can be considered a valid failover candidate.
func (s *Sentinel) dbValidity(cd *cluster.ClusterData, dbUID string) (dbValidity, error) {
	db, ok := cd.DBs[dbUID]
	if !ok {
		return 0, fmt.Errorf("unknown db uid %q", dbUID)
	}

	if db.Status.CurrentGeneration == cluster.NoGeneration {
		return dbValidityUnknown, nil
	}

	masterDB := cd.DBs[cd.Cluster.Status.Master]
	if masterDB == nil {
		return dbValidityUnknown, nil
	}

	// ignore empty (not provided) systemid
	if db.Status.SystemID != "" {
		// if with a different postgres systemID it's invalid
		if db.Status.SystemID != masterDB.Status.SystemID {
			s.log.Info().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Str("db_system_id", db.Status.SystemID).
				Str("master_system_id", masterDB.Status.SystemID).
				Msg(
					"invalid db since the postgres systemdID is different " +
						"than the master one",
				)
			return dbValidityInvalid, nil
		}
	}

	// If on a different timeline branch it's invalid.
	if s.isDifferentTimelineBranch(masterDB, db) {
		return dbValidityInvalid, nil
	}

	return dbValidityValid, nil
}

// dbCanSync reports whether DB can still catch up with WAL available on master.
func (s *Sentinel) dbCanSync(cd *cluster.ClusterData, dbUID string) (bool, error) {
	db, ok := cd.DBs[dbUID]
	if !ok {
		return false, fmt.Errorf("unknown db uid %q", dbUID)
	}
	masterDB := cd.DBs[cd.Cluster.Status.Master]
	if masterDB == nil {
		return true, nil
	}

	// ignore if master doesn't provide the older wal file
	if masterDB.Status.OlderWalFile == "" {
		return true, nil
	}

	// skip current master
	if dbUID == masterDB.UID {
		return true, nil
	}

	// skip the standbys
	dt, err := s.dbType(cd, db.UID)
	if err != nil {
		return false, err
	}
	if dt != dbTypeReplicaLine {
		return true, nil
	}

	// only check when db isn't initializing
	if db.Generation == cluster.InitialGeneration {
		return true, nil
	}

	// check only if the db isn't healty.
	if !db.Status.Healthy {
		return true, nil
	}

	if db.Status.XLogPos == masterDB.Status.XLogPos {
		return true, nil
	}

	// check only if the xlogpos isn't increasing for some time. This can also
	// happen when no writes are happening on the master but the standby should
	// be, if syncing at the same xlogpos.
	if s.isDBIncreasingXLogPos(db) {
		return true, nil
	}

	required := postgresql.XlogPosToWalFileNameNoTimeline(db.Status.XLogPos, postgresql.WalSegSize)
	older, err := postgresql.WalFileNameNoTimeLine(masterDB.Status.OlderWalFile)
	if err != nil {
		// warn on wrong file name (shouldn't happen...)
		s.log.Warn().
			Str("filename", masterDB.Status.OlderWalFile).
			Msg("wrong wal file name")
	}
	s.log.Debug().
		Str(slog.FieldDBUID, db.UID).
		Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
		Str("required_wal", required).
		Str("older_master_wal", older).
		Msg(
			"xlog pos isn't advancing on standby, checking if the master " +
				"has the required wals",
		)
	walAvailable, walErr := postgresql.IsRequiredWalAvailable(
		db.Status.XLogPos,
		masterDB.Status.OlderWalFile,
		postgresql.WalSegSize,
	)
	if walErr != nil {
		return false, walErr
	}
	if walAvailable {
		return true, nil
	}

	s.log.Info().
		Str(slog.FieldDBUID, db.UID).
		Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
		Str("required_wal", required).
		Str("older_master_wal", older).
		Msg("db won't be able to sync due to missing required wals on master")
	return false, nil
}

// dbStatus classifies DB health considering keeper health and convergence.
func (s *Sentinel) dbStatus(cd *cluster.ClusterData, dbUID string) (dbStatus, error) {
	db, ok := cd.DBs[dbUID]
	if !ok {
		return 0, fmt.Errorf("unknown db uid %q", dbUID)
	}

	// if keeper failed then mark as failed
	keeper := cd.Keepers[db.Spec.KeeperUID]
	if !keeper.Status.Healthy {
		return dbStatusFailed, nil
	}

	convergenceTimeout := cd.Cluster.DefSpec().ConvergenceTimeout.Duration
	// check if db should be in init mode and adjust convergence timeout
	if db.Generation == cluster.InitialGeneration {
		if db.Spec.InitMode == cluster.DBInitModeResync {
			convergenceTimeout = cd.Cluster.DefSpec().SyncTimeout.Duration
		}
	}
	convergenceState := s.dbConvergenceState(db, convergenceTimeout)
	switch convergenceState {
	case ConvergenceFailed:
		return dbStatusFailed, nil
	case Converging:
		return dbStatusConverging, nil
	}

	if !db.Status.Healthy {
		return dbStatusFailed, nil
	}

	return dbStatusGood, nil
}

func (s *Sentinel) validMastersByStatus(cd *cluster.ClusterData) (
	map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB,
) {
	return s.validDBsByStatus(cd, dbTypePrimaryLine, "master")
}

func (s *Sentinel) validStandbysByStatus(cd *cluster.ClusterData) (
	map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB,
) {
	return s.validDBsByStatus(cd, dbTypeReplicaLine, "standby")
}

func (s *Sentinel) validDBsByStatus(cd *cluster.ClusterData, wantType dbType, logRole string) (
	map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB,
) {
	goodDBs := map[string]*cluster.DB{}
	failedDBs := map[string]*cluster.DB{}
	convergingDBs := map[string]*cluster.DB{}

	for _, db := range cd.DBs {
		valid, err := s.dbValidity(cd, db.UID)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str(slog.FieldDBUID, db.UID).
				Str("role", logRole).
				Msg("skipping database classification because validity cannot be determined")
			continue
		}

		dt, err := s.dbType(cd, db.UID)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str(slog.FieldDBUID, db.UID).
				Str("role", logRole).
				Msg("skipping database classification because database type cannot be determined")
			continue
		}

		if valid != dbValidityValid || dt != wantType {
			continue
		}

		status, err := s.dbStatus(cd, db.UID)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str(slog.FieldDBUID, db.UID).
				Str("role", logRole).
				Msg("skipping database classification because database status cannot be determined")
			continue
		}

		switch status {
		case dbStatusGood:
			goodDBs[db.UID] = db
		case dbStatusFailed:
			failedDBs[db.UID] = db
		case dbStatusConverging:
			convergingDBs[db.UID] = db
		}
	}

	return goodDBs, failedDBs, convergingDBs
}

// dbSlice implements sort interface to sort by XLogPos.
type dbSlice []*cluster.DB

func (p dbSlice) Len() int {
	return len(p)
}

func (p dbSlice) Less(i, j int) bool {
	return p[i].Status.XLogPos < p[j].Status.XLogPos
}

func (p dbSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func keeperMasterPriority(cd *cluster.ClusterData, db *cluster.DB) int {
	if cd == nil || db == nil || db.Spec == nil {
		return 0
	}
	keeper := cd.Keepers[db.Spec.KeeperUID]
	if keeper == nil {
		return 0
	}
	return keeper.Status.MasterPriority
}

func sortCandidateMasters(cd *cluster.ClusterData, candidates []*cluster.DB) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Status.XLogPos != candidates[j].Status.XLogPos {
			return candidates[i].Status.XLogPos < candidates[j].Status.XLogPos
		}
		pi := keeperMasterPriority(cd, candidates[i])
		pj := keeperMasterPriority(cd, candidates[j])
		if pi != pj {
			return pi > pj
		}
		return candidates[i].UID < candidates[j].UID
	})
}

func (s *Sentinel) findBestStandbys(cd *cluster.ClusterData, masterDB *cluster.DB) []*cluster.DB {
	goodStandbys, _, _ := s.validStandbysByStatus(cd)
	bestDBs := []*cluster.DB{}
	for _, db := range goodStandbys {
		if db.Status.TimelineID != masterDB.Status.TimelineID {
			s.log.Debug().
				Str(slog.FieldDBUID, db.UID).
				Uint64("db_timeline", db.Status.TimelineID).
				Uint64("master_timeline", masterDB.Status.TimelineID).
				Msg(msgPGTimelineDiffersFromMaster)
			continue
		}

		// do this only when not using synchronous replication since in sync repl we
		// have to ignore the last reported xlogpos or valid sync standby will be
		// skipped
		if !s.syncRepl(cd.Cluster.DefSpec()) {
			if !s.isLagBelowMax(cd, masterDB, db) {
				s.log.Debug().
					Str(slog.FieldDBUID, db.UID).
					Uint64("db_xlog_pos", db.Status.XLogPos).
					Uint64("master_xlog_pos", masterDB.Status.XLogPos).
					Msg(msgStandbyLagAboveMax)
				continue
			}
		}
		bestDBs = append(bestDBs, db)
	}

	// Sort by XLogPos
	sort.Sort(dbSlice(bestDBs))
	return bestDBs
}

// findBestNewMasters identifies DBs eligible to become new master.
func (s *Sentinel) findBestNewMasters(
	cd *cluster.ClusterData,
	masterDB *cluster.DB,
) []*cluster.DB {
	bestNewMasters := []*cluster.DB{}
	for _, db := range s.findBestStandbys(cd, masterDB) {
		if k, ok := cd.Keepers[db.Spec.KeeperUID]; ok &&
			(k.Status.CanBeMaster != nil && !*k.Status.CanBeMaster) {
			s.log.Info().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("ignoring keeper since it cannot be master (--can-be-master=false)")
			continue
		}

		bestNewMasters = append(bestNewMasters, db)
	}

	// Add previous masters to best standbys (if valid and in good state).
	validMastersByStatus, _, _ := s.validMastersByStatus(cd)
	s.log.Debug().
		Interface("valid_masters", cluster.LogSummaryDBMap(validMastersByStatus)).
		Msg("master databases currently considered valid by status")
	for _, db := range validMastersByStatus {
		if db.UID == masterDB.UID {
			s.log.Debug().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("ignoring db since it's the current master")
			continue
		}

		if db.Status.TimelineID != masterDB.Status.TimelineID {
			s.log.Debug().
				Str(slog.FieldDBUID, db.UID).
				Uint64("db_timeline", db.Status.TimelineID).
				Uint64("master_timeline", masterDB.Status.TimelineID).
				Msg(msgPGTimelineDiffersFromMaster)
			continue
		}

		// do this only when not using synchronous replication since in sync repl we
		// have to ignore the last reported xlogpos or valid sync standby will be
		// skipped
		if !s.syncRepl(cd.Cluster.DefSpec()) {
			if !s.isLagBelowMax(cd, masterDB, db) {
				s.log.Debug().
					Str(slog.FieldDBUID, db.UID).
					Uint64("db_xlog_pos", db.Status.XLogPos).
					Uint64("master_xlog_pos", masterDB.Status.XLogPos).
					Msg(msgStandbyLagAboveMax)
				continue
			}
		}

		bestNewMasters = append(bestNewMasters, db)
	}

	sortCandidateMasters(cd, bestNewMasters)
	s.log.Debug().
		Interface("candidate_masters", cluster.LogSummaryDBList(bestNewMasters)).
		Msg("candidate databases for new master, ordered by XLog position and keeper priority tie-break")

	return bestNewMasters
}
