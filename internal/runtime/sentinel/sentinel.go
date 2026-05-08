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

package sentinel

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/configfile"
	slog "github.com/woozymasta/hysteron/internal/log"
	pg "github.com/woozymasta/hysteron/internal/postgresql"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/id"
	slicesutil "github.com/woozymasta/hysteron/internal/utils/slices"
	"github.com/woozymasta/hysteron/internal/utils/timer"

	"github.com/mitchellh/copystructure"
	"github.com/rs/zerolog"
	"github.com/woozymasta/flags"
)

// log is the sentinel component logger; refreshed after logging is configured.
var log zerolog.Logger

func init() {
	log = slog.WithComponent("sentinel")
}

const (
	fakeStandbyName = "hysteronfakestandby"

	msgPGTimelineDiffersFromMaster = "ignoring keeper since its pg timeline " +
		"is different than master timeline"
	msgStandbyLagAboveMax = "ignoring keeper since its lag is above the " +
		"max configured lag"
)

type config struct {
	InitialClusterSpecFile string                       `short:"f" long:"initial-cluster-spec" env:"INITIAL_CLUSTER_SPEC" description:"a file providing the initial cluster specification, used only at cluster initialization, ignored if cluster is already initialized"`
	ClusterSpecFiles       []string                     `long:"cluster-spec" env:"CLUSTER_SPEC" description:"per-cluster initial cluster specification override as <cluster-name>=<path>; can be repeated"`
	KubeService            kubeServicePublishingOptions `group:"Kubernetes Service Publishing"`
	runtimecommon.CommonConfig
}

var cfg config

func (s *Sentinel) electionLoop(ctx context.Context) {
	for {
		s.log.Info().Msg("trying to acquire sentinel leadership")
		electedCh, errCh, err := s.election.RunForElection()
		if err != nil {
			s.log.Error().Err(err).Msg("failed to start sentinel election")
			select {
			case <-ctx.Done():
				s.log.Debug().Msg("stopping election loop")
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}
	inner:
		for {
			select {
			case elected := <-electedCh:
				s.leaderMutex.Lock()
				if elected {
					s.log.Info().Msg("sentinel leadership acquired")
					s.leader = true
					s.leadershipCount++
					isLeaderGauge.WithLabelValues(s.clusterName).Set(1)
					leaderCountGauge.WithLabelValues(s.clusterName).Set(float64(s.leadershipCount))
				} else {
					if s.leader {
						s.log.Info().Msg("sentinel leadership lost")
					}
					s.leader = false
					isLeaderGauge.WithLabelValues(s.clusterName).Set(0)
				}
				s.leaderMutex.Unlock()

			case err := <-errCh:
				if err != nil {
					s.log.Error().Err(err).Msg("sentinel election loop failed")
					if err := s.election.Stop(); err != nil {
						s.log.Error().Err(err).Msg("failed to stop sentinel election")
					}
				}
				break inner
			case <-ctx.Done():
				s.log.Debug().Msg("stopping election loop")
				if err := s.election.Stop(); err != nil {
					s.log.Error().Err(err).Msg("failed to stop sentinel election")
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

// syncRepl return whether to use synchronous replication based on the current
// cluster spec.
func (s *Sentinel) syncRepl(spec *cluster.ClusterSpec) bool {
	// a cluster standby role means our "master" will act as a cascading standby to
	// the other keepers, in this case we can't use synchronous replication
	return *spec.SynchronousReplication &&
		*spec.Role == cluster.ClusterRoleMaster
}

func (s *Sentinel) setSentinelInfo(
	ctx context.Context,
	ttl time.Duration,
) error {
	sentinelInfo := &cluster.SentinelInfo{
		UID: s.uid,
	}
	s.log.Debug().
		Str("sentinel_uid", sentinelInfo.UID).
		Msg("sentinel registration payload before write to store")

	if err := s.e.SetSentinelInfo(ctx, sentinelInfo, ttl); err != nil {
		return err
	}
	return nil
}

// SetKeeperError marks a keeper as having a recent error.
func (s *Sentinel) SetKeeperError(uid string) {
	if _, ok := s.keeperErrorTimers[uid]; !ok {
		s.keeperErrorTimers[uid] = timer.Now()
	}
}

// CleanKeeperError clears the recent error marker for a keeper.
func (s *Sentinel) CleanKeeperError(uid string) {
	delete(s.keeperErrorTimers, uid)
}

// SetDBError marks a database as having a recent error.
func (s *Sentinel) SetDBError(uid string) {
	if _, ok := s.dbErrorTimers[uid]; !ok {
		s.dbErrorTimers[uid] = timer.Now()
	}
}

// CleanDBError clears the recent error marker for a database.
func (s *Sentinel) CleanDBError(uid string) {
	delete(s.dbErrorTimers, uid)
}

// SetDBNotIncreasingXLogPos records a database check with no WAL progress.
func (s *Sentinel) SetDBNotIncreasingXLogPos(uid string) {
	if _, ok := s.dbNotIncreasingXLogPos[uid]; !ok {
		s.dbNotIncreasingXLogPos[uid] = 1
	} else {
		s.dbNotIncreasingXLogPos[uid]++
	}
}

// CleanDBNotIncreasingXLogPos clears the WAL progress stall counter.
func (s *Sentinel) CleanDBNotIncreasingXLogPos(uid string) {
	delete(s.dbNotIncreasingXLogPos, uid)
}

func (s *Sentinel) updateKeepersStatus(
	cd *cluster.ClusterData,
	keepersInfo cluster.KeepersInfo,
	firstRun bool,
) (*cluster.ClusterData, KeeperInfoHistories, error) {
	// Create a copy of cd
	cd = cd.DeepCopy()

	kihs, err := s.keeperInfoHistories.DeepCopy()
	if err != nil {
		return nil, nil, err
	}

	// Remove keepers with wrong cluster UID
	tmpKeepersInfo := keepersInfo.DeepCopy()
	for _, ki := range keepersInfo {
		if ki.ClusterUID != cd.Cluster.UID {
			delete(tmpKeepersInfo, ki.UID)
		}
	}
	keepersInfo = tmpKeepersInfo

	// On first run just insert keepers info in the history with Seen set
	// to false and don't do any change to the keepers' state
	if firstRun {
		for keeperUID, ki := range keepersInfo {
			kihs[keeperUID] = &KeeperInfoHistory{
				KeeperInfo: ki,
				Seen:       false,
			}
		}
		return cd, kihs, nil
	}

	tmpKeepersInfo = keepersInfo.DeepCopy()
	// keep only updated keepers info
	for keeperUID, ki := range keepersInfo {
		if kih, ok := kihs[keeperUID]; ok {
			if kih.KeeperInfo.InfoUID == ki.InfoUID {
				if !kih.Seen {
					// Remove since it was already there and wasn't updated
					delete(tmpKeepersInfo, ki.UID)
				} else if kih.Seen && timer.Since(kih.Timer) > s.sleepInterval {
					// Remove since it wasn't updated
					delete(tmpKeepersInfo, ki.UID)
				}
			}
			if kih.KeeperInfo.InfoUID != ki.InfoUID {
				kihs[keeperUID] = &KeeperInfoHistory{
					KeeperInfo: ki,
					Seen:       true,
					Timer:      timer.Now(),
				}
			}
		} else {
			kihs[keeperUID] = &KeeperInfoHistory{KeeperInfo: ki, Seen: true, Timer: timer.Now()}
		}
	}
	keepersInfo = tmpKeepersInfo

	// Create new keepers from keepersInfo
	for keeperUID, ki := range keepersInfo {
		if _, ok := cd.Keepers[keeperUID]; !ok {
			k := cluster.NewKeeperFromKeeperInfo(ki)
			cd.Keepers[k.UID] = k
		}
	}

	// Keepers support several command line arguments that should be populated in the
	// KeeperStatus by the sentinel. This allows us to make decisions about how to arrange
	// the cluster that take into consideration the configuration of each keeper.
	for keeperUID, k := range cd.Keepers {
		if ki, ok := keepersInfo[keeperUID]; ok {
			k.Status.CanBeMaster = ki.CanBeMaster
			k.Status.CanBeSynchronousReplica = ki.CanBeSynchronousReplica
		}
	}

	// Mark keepers without a keeperInfo (cleaned up above from not updated
	// ones) as in error
	for keeperUID, k := range cd.Keepers {
		if ki, ok := keepersInfo[keeperUID]; !ok {
			s.SetKeeperError(keeperUID)
		} else {
			s.CleanKeeperError(keeperUID)
			// Update keeper status infos
			k.Status.BootUUID = ki.BootUUID
			k.Status.PostgresBinaryVersion.Maj = ki.PostgresBinaryVersion.Maj
			k.Status.PostgresBinaryVersion.Min = ki.PostgresBinaryVersion.Min
		}
	}

	// Update keepers' healthy states
	for _, k := range cd.Keepers {
		healthy := s.isKeeperHealthy(cd, k)
		if k.Status.ForceFail {
			healthy = false
			// reset ForceFail
			k.Status.ForceFail = false
		}
		// set zero LastHealthyTime to time.Now() to avoid the keeper being
		// removed since previous versions don't have it set
		if k.Status.LastHealthyTime.IsZero() {
			k.Status.LastHealthyTime = time.Now()
		}
		if healthy {
			k.Status.LastHealthyTime = time.Now()
		}
		k.Status.Healthy = healthy
	}

	// Update dbs' states
	for _, db := range cd.DBs {
		// Mark not found DBs in DBstates in error
		k, ok := keepersInfo[db.Spec.KeeperUID]
		if !ok {
			s.log.Warn().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("no keeper info available")
			s.SetDBError(db.UID)
			continue
		}
		dbs := k.PostgresState
		if dbs == nil {
			s.log.Warn().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("no db state available")
			s.SetDBError(db.UID)
			continue
		}
		if dbs.UID != db.UID {
			s.log.Warn().
				Str("received_db_uid", dbs.UID).
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("received db state for unexpected db uid")
			s.SetDBError(db.UID)
			continue
		}
		s.log.Debug().
			Str(slog.FieldDBUID, db.UID).
			Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
			Msg("received db state")

		if db.Status.XLogPos == dbs.XLogPos {
			s.SetDBNotIncreasingXLogPos(db.UID)
		} else {
			s.CleanDBNotIncreasingXLogPos(db.UID)
		}

		db.Status.ListenAddress = dbs.ListenAddress
		db.Status.Port = dbs.Port
		db.Status.CurrentGeneration = dbs.Generation
		if dbs.Healthy {
			s.CleanDBError(db.UID)
			db.Status.SystemID = dbs.SystemID
			db.Status.TimelineID = dbs.TimelineID
			db.Status.XLogPos = dbs.XLogPos
			db.Status.TimelinesHistory = dbs.TimelinesHistory
			db.Status.PGParameters = cluster.PGParameters(
				dbs.PGParameters,
			)

			db.Status.CurSynchronousStandbys = dbs.SynchronousStandbys

			db.Status.OlderWalFile = dbs.OlderWalFile
		} else {
			s.SetDBError(db.UID)
		}
	}

	// Update dbs' healthy state
	for _, db := range cd.DBs {
		db.Status.Healthy = s.isDBHealthy(cd, db)
		// if keeper is unhealthy then mark also the db ad unhealthy
		keeper := cd.Keepers[db.Spec.KeeperUID]
		if !keeper.Status.Healthy {
			db.Status.Healthy = false
		}
	}

	return cd, kihs, nil
}

// activeProxiesInfos takes the provided proxyInfo list and returns a list of
// proxiesInfo considered active. We also consider as active the proxies not yet
// in the proxyInfoHistories since only after some time we'll know if they are
// really active (updating their proxyInfo) or stale. This is needed to not
// exclude any possible active proxy from the checks in updateCluster and not
// remove them from the enabled proxies list. At worst a stale proxy will be
// added to the enabled proxies list.
func (s *Sentinel) activeProxiesInfos(
	proxiesInfo cluster.ProxiesInfo,
) (cluster.ProxiesInfo, error) {
	pihs, err := s.proxyInfoHistories.DeepCopy()
	if err != nil {
		return nil, err
	}

	// remove missing proxyInfos from the history
	for proxyUID := range pihs {
		if _, ok := proxiesInfo[proxyUID]; !ok {
			delete(pihs, proxyUID)
		}
	}

	activeProxiesInfo := proxiesInfo.DeepCopy()
	// keep only updated proxies info
	for _, pi := range proxiesInfo {
		if pih, ok := pihs[pi.UID]; ok {
			if pih.ProxyInfo.InfoUID == pi.InfoUID {
				if timer.Since(pih.Timer) > 2*pi.ProxyTimeout {
					delete(activeProxiesInfo, pi.UID)
				}
			} else {
				pihs[pi.UID] = &ProxyInfoHistory{ProxyInfo: pi, Timer: timer.Now()}
			}
		} else {
			// add proxyInfo if not in the history
			pihs[pi.UID] = &ProxyInfoHistory{ProxyInfo: pi, Timer: timer.Now()}
		}
	}

	s.proxyInfoHistories = pihs

	return activeProxiesInfo, nil
}

func (s *Sentinel) findInitialKeeper(
	cd *cluster.ClusterData,
) (*cluster.Keeper, error) {
	if len(cd.Keepers) < 1 {
		return nil, errors.New("no keepers registered")
	}
	r := s.RandFn(len(cd.Keepers))
	keys := []string{}
	for k := range cd.Keepers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return cd.Keepers[keys[r]], nil
}

// setDBSpecFromClusterSpec updates dbSpec values with the related clusterSpec ones
func (s *Sentinel) setDBSpecFromClusterSpec(cd *cluster.ClusterData) {
	clusterSpec := cd.Cluster.DefSpec()
	for _, db := range cd.DBs {
		db.Spec.RequestTimeout = *clusterSpec.RequestTimeout
		db.Spec.MaxStandbys = *clusterSpec.MaxStandbys
		db.Spec.UsePgrewind = *clusterSpec.UsePgrewind
		db.Spec.PGParameters = clusterSpec.PGParameters
		db.Spec.PGHBA = clusterSpec.PGHBA
		if db.Spec.FollowConfig != nil &&
			db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
			db.Spec.FollowConfig.StandbySettings = clusterSpec.StandbyConfig.StandbySettings
			db.Spec.FollowConfig.ArchiveRecoverySettings = clusterSpec.StandbyConfig.ArchiveRecoverySettings
		}
		db.Spec.AdditionalWalSenders = *clusterSpec.AdditionalWalSenders
		dt, err := s.dbType(cd, db.UID)
		if err != nil {
			s.log.Warn().
				Err(err).
				Str(slog.FieldDBUID, db.UID).
				Msg("skipping database spec update because database type cannot be determined")
			continue
		}
		switch dt {
		case dbTypePrimaryLine:
			db.Spec.AdditionalReplicationSlots = clusterSpec.AdditionalMasterReplicationSlots
			db.Spec.IgnoreReplicationSlots = clusterSpec.IgnoreMasterReplicationSlots
		case dbTypeReplicaLine:
			db.Spec.AdditionalReplicationSlots = nil
			db.Spec.IgnoreReplicationSlots = nil
			// Standby additional slot policy is currently master-only.
			// See plan.md: "TODO: Replication Slots On Failover".
		}
	}
}

func (s *Sentinel) isDifferentTimelineBranch(
	followedDB *cluster.DB,
	db *cluster.DB,
) bool {
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
			Msg(
				"followed instance timeline forked at a different xlog pos " +
					"than our timeline",
			)
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

// isLagBelowMax checks if the db reported lag is below MaxStandbyLag from the
// master reported lag
func (s *Sentinel) isLagBelowMax(
	cd *cluster.ClusterData,
	curMasterDB, db *cluster.DB,
) bool {
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

func (s *Sentinel) freeKeepers(
	cd *cluster.ClusterData,
) []*cluster.Keeper {
	freeKeepers := []*cluster.Keeper{}
K:
	for _, keeper := range cd.Keepers {
		if !keeper.Status.Healthy {
			continue
		}
		for _, db := range cd.DBs {
			if db.Spec.KeeperUID == keeper.UID {
				continue K
			}
		}
		freeKeepers = append(freeKeepers, keeper)
	}
	return freeKeepers
}

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

// dbType returns the db type
// A master is a db that:
// * Has a master db role or a standby db role with followtype external
// A standby is a db that:
// * Has a standby db role with followtype internal
func (s *Sentinel) dbType(
	cd *cluster.ClusterData,
	dbUID string,
) (dbType, error) {
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

// dbValidity return the validity of a db
// a db isn't valid when it has a different postgres systemdID or is on a
// different timeline branch
// dbs with CurrentGeneration == NoGeneration (0) are reported as
// dbValidityUnknown since the db status is empty.
func (s *Sentinel) dbValidity(
	cd *cluster.ClusterData,
	dbUID string,
) (dbValidity, error) {
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

	// If on a different timeline branch it's invalid
	if s.isDifferentTimelineBranch(masterDB, db) {
		return dbValidityInvalid, nil
	}

	// db is valid
	return dbValidityValid, nil
}

func (s *Sentinel) dbCanSync(
	cd *cluster.ClusterData,
	dbUID string,
) (bool, error) {
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

	required := pg.XlogPosToWalFileNameNoTimeline(db.Status.XLogPos, pg.WalSegSize)
	older, err := pg.WalFileNameNoTimeLine(masterDB.Status.OlderWalFile)
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
	walAvailable, walErr := pg.IsRequiredWalAvailable(
		db.Status.XLogPos,
		masterDB.Status.OlderWalFile,
		pg.WalSegSize,
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

func (s *Sentinel) dbStatus(
	cd *cluster.ClusterData,
	dbUID string,
) (dbStatus, error) {
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
	// if convergence failed then mark as failed
	case ConvergenceFailed:
		return dbStatusFailed, nil
	// if converging then it's not failed (it can also be not healthy since it could be resyncing)
	case Converging:
		return dbStatusConverging, nil
	}
	// if converged but not healthy mark as failed
	if !db.Status.Healthy {
		return dbStatusFailed, nil
	}

	// db is good
	return dbStatusGood, nil
}

func (s *Sentinel) validMastersByStatus(
	cd *cluster.ClusterData,
) (map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB) {
	return s.validDBsByStatus(cd, dbTypePrimaryLine, "master")
}

func (s *Sentinel) validStandbysByStatus(
	cd *cluster.ClusterData,
) (map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB) {
	return s.validDBsByStatus(cd, dbTypeReplicaLine, "standby")
}

func (s *Sentinel) validDBsByStatus(
	cd *cluster.ClusterData,
	wantType dbType,
	logRole string,
) (map[string]*cluster.DB, map[string]*cluster.DB, map[string]*cluster.DB) {
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

// dbSlice implements sort interface to sort by XLogPos
type dbSlice []*cluster.DB

func (p dbSlice) Len() int { return len(p) }

func (p dbSlice) Less(
	i, j int,
) bool {
	return p[i].Status.XLogPos < p[j].Status.XLogPos
}
func (p dbSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (s *Sentinel) findBestStandbys(
	cd *cluster.ClusterData,
	masterDB *cluster.DB,
) []*cluster.DB {
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

// findBestNewMasters identifies the DBs that are elegible to become a new master. We do
// this by selecting from valid standbys (those keepers that follow the same timeline as
// our master, and have an acceptable replication lag) and also selecting from those nodes
// that are valid to become master by their status.
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

	// Add the previous masters to the best standbys (if valid and in good state)
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

	// Sort by XLogPos
	sort.Sort(dbSlice(bestNewMasters))
	s.log.Debug().
		Interface("candidate_masters", cluster.LogSummaryDBList(bestNewMasters)).
		Msg("candidate databases for new master, ordered by XLog position")
	return bestNewMasters
}

func (s *Sentinel) applyInitialDBConvergence(
	newcd *cluster.ClusterData,
	clusterSpec *cluster.ClusterSpec,
	db *cluster.DB,
	state ConvergenceState,
	waitMsg string,
) {
	switch state {
	case Converged:
		if db.Status.Healthy {
			s.log.Info().
				Str(slog.FieldDBUID, db.UID).
				Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
				Msg("db initialized")
			// Set db initMode to none, not needed but just a security measure
			db.Spec.InitMode = cluster.DBInitModeNone
			// Don't include previous config anymore
			db.Spec.IncludeConfig = false

			// Replace reported pg parameters in cluster spec
			if *clusterSpec.MergePgParameters {
				newcd.Cluster.Spec.PGParameters = db.Status.PGParameters
			}
			// Cluster initialized, switch to Normal state
			newcd.Cluster.Status.Phase = cluster.ClusterPhaseNormal
		}
	case Converging:
		s.log.Info().
			Str(slog.FieldDBUID, db.UID).
			Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
			Msg(waitMsg)
	case ConvergenceFailed:
		s.log.Info().
			Str(slog.FieldDBUID, db.UID).
			Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
			Msg("db failed to initialize")
		// Empty DBs
		newcd.DBs = cluster.DBs{}
		// Unset master so another keeper can be chosen
		newcd.Cluster.Status.Master = ""
	}
}

func (s *Sentinel) updateCluster(
	cd *cluster.ClusterData,
	pis cluster.ProxiesInfo,
) (*cluster.ClusterData, error) {
	// take a cd deepCopy to check that the code isn't changing it (it'll be a bug)
	origcd := cd.DeepCopy()
	newcd := cd.DeepCopy()
	clusterSpec := cd.Cluster.DefSpec()

	switch cd.Cluster.Status.Phase {
	case cluster.ClusterPhaseInitializing:
		switch *clusterSpec.InitMode {
		case cluster.ClusterInitModeNew:
			// Is there already a keeper choosed to be the new master?
			if cd.Cluster.Status.Master == "" {
				s.log.Info().Msg("trying to find initial master")
				k, err := s.findInitialKeeper(newcd)
				if err != nil {
					return nil, fmt.Errorf(
						"cannot choose initial master: %v",
						err,
					)
				}
				s.log.Info().
					Str(slog.FieldKeeperUID, k.UID).
					Msg("initializing cluster")
				db := &cluster.DB{
					UID:        s.UIDFn(),
					Generation: cluster.InitialGeneration,
					Spec: &cluster.DBSpec{
						KeeperUID:     k.UID,
						InitMode:      cluster.DBInitModeNew,
						NewConfig:     clusterSpec.NewConfig,
						Role:          common.RoleMaster,
						Followers:     []string{},
						IncludeConfig: *clusterSpec.MergePgParameters,
					},
				}
				newcd.DBs[db.UID] = db
				newcd.Cluster.Status.Master = db.UID
				s.log.Debug().
					Fields(cluster.LogSummaryClusterData(newcd)).
					Msg("cluster data after creating initial master DB (new cluster init)")
			} else {
				db, ok := newcd.DBs[cd.Cluster.Status.Master]
				if !ok {
					return nil, fmt.Errorf("cluster status references missing db %q", cd.Cluster.Status.Master)
				}
				// Check that the choosed db for being the master has correctly initialized
				s.applyInitialDBConvergence(
					newcd,
					clusterSpec,
					db,
					s.dbConvergenceState(db, clusterSpec.InitTimeout.Duration),
					"waiting for db",
				)
			}
		case cluster.ClusterInitModeExisting:
			if cd.Cluster.Status.Master == "" {
				wantedKeeper := clusterSpec.ExistingConfig.KeeperUID
				s.log.Info().
					Str(slog.FieldKeeperUID, wantedKeeper).
					Msg("trying to use keeper as initial master")

				k, ok := newcd.Keepers[wantedKeeper]
				if !ok {
					return nil, fmt.Errorf(
						"keeper %q state not available",
						wantedKeeper,
					)
				}

				s.log.Info().
					Str(slog.FieldKeeperUID, k.UID).
					Msg("initializing cluster using selected keeper as master db owner")

				db := &cluster.DB{
					UID:        s.UIDFn(),
					Generation: cluster.InitialGeneration,
					Spec: &cluster.DBSpec{
						KeeperUID:     k.UID,
						InitMode:      cluster.DBInitModeExisting,
						Role:          common.RoleMaster,
						Followers:     []string{},
						IncludeConfig: *clusterSpec.MergePgParameters,
					},
				}
				newcd.DBs[db.UID] = db
				newcd.Cluster.Status.Master = db.UID
				s.log.Debug().
					Fields(cluster.LogSummaryClusterData(newcd)).
					Msg("cluster data after creating initial master DB (existing data init)")
			} else {
				db, ok := newcd.DBs[cd.Cluster.Status.Master]
				if !ok {
					return nil, fmt.Errorf("cluster status references missing db %q", cd.Cluster.Status.Master)
				}
				// Check that the choosed db for being the master has correctly initialized
				if db.Status.Healthy && s.dbConvergenceState(db, clusterSpec.ConvergenceTimeout.Duration) == Converged {
					s.log.Info().
						Str(slog.FieldDBUID, db.UID).
						Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
						Msg("db initialized")
					// Set db initMode to none, not needed but just a security measure
					db.Spec.InitMode = cluster.DBInitModeNone
					// Don't include previous config anymore
					db.Spec.IncludeConfig = false
					// Replace reported pg parameters in cluster spec
					if *clusterSpec.MergePgParameters {
						newcd.Cluster.Spec.PGParameters = db.Status.PGParameters
					}
					// Cluster initialized, switch to Normal state
					newcd.Cluster.Status.Phase = cluster.ClusterPhaseNormal
				}
			}
		case cluster.ClusterInitModePITR:
			// Is there already a keeper choosed to be the new master?
			if cd.Cluster.Status.Master == "" {
				s.log.Info().Msg("trying to find initial master")
				k, err := s.findInitialKeeper(cd)
				if err != nil {
					return nil, fmt.Errorf(
						"cannot choose initial master: %v",
						err,
					)
				}
				s.log.Info().
					Str(slog.FieldKeeperUID, k.UID).
					Msg("initializing cluster using selected keeper as master db owner")
				role := common.RoleMaster
				var followConfig *cluster.FollowConfig
				if *clusterSpec.Role == cluster.ClusterRoleStandby {
					role = common.RoleStandby
					followConfig = &cluster.FollowConfig{
						Type:                    cluster.FollowTypeExternal,
						StandbySettings:         clusterSpec.StandbyConfig.StandbySettings,
						ArchiveRecoverySettings: clusterSpec.StandbyConfig.ArchiveRecoverySettings,
					}
				}
				db := &cluster.DB{
					UID:        s.UIDFn(),
					Generation: cluster.InitialGeneration,
					Spec: &cluster.DBSpec{
						KeeperUID:     k.UID,
						InitMode:      cluster.DBInitModePITR,
						PITRConfig:    clusterSpec.PITRConfig,
						Role:          role,
						FollowConfig:  followConfig,
						Followers:     []string{},
						IncludeConfig: *clusterSpec.MergePgParameters,
					},
				}
				newcd.DBs[db.UID] = db
				newcd.Cluster.Status.Master = db.UID
				s.log.Debug().
					Fields(cluster.LogSummaryClusterData(newcd)).
					Msg("cluster data after creating initial master DB (PITR init)")
			} else {
				db, ok := newcd.DBs[cd.Cluster.Status.Master]
				if !ok {
					return nil, fmt.Errorf("cluster status references missing db %q", cd.Cluster.Status.Master)
				}
				// Check that the chosen db for being the master has correctly initialized.
				// PITR initialization is bounded by init timeout.
				s.applyInitialDBConvergence(
					newcd,
					clusterSpec,
					db,
					s.dbConvergenceState(db, clusterSpec.InitTimeout.Duration),
					"waiting for db to converge",
				)
			}
		default:
			return nil, fmt.Errorf(
				"unknown init mode %s",
				*cd.Cluster.DefSpec().InitMode,
			)
		}

	case cluster.ClusterPhaseNormal:
		// Remove old keepers
		keepersToRemove := []*cluster.Keeper{}
		for _, k := range newcd.Keepers {
			// get db associated to the keeper
			db := cd.FindDB(k)
			if db != nil {
				// skip keepers with an assigned db
				continue
			}
			if time.Now().
				After(k.Status.LastHealthyTime.Add(cd.Cluster.DefSpec().DeadKeeperRemovalInterval.Duration)) {
				s.log.Info().
					Str(slog.FieldKeeperUID, k.UID).
					Msg("removing old dead keeper")
				keepersToRemove = append(keepersToRemove, k)
			}
		}
		for _, k := range keepersToRemove {
			delete(newcd.Keepers, k.UID)
		}

		// Calculate current master status
		curMasterDBUID := cd.Cluster.Status.Master
		wantedMasterDBUID := curMasterDBUID

		masterOK := true
		curMasterDB := cd.DBs[curMasterDBUID]
		if curMasterDB == nil {
			return nil, fmt.Errorf(
				"db for keeper %q not available, this shouldn't happen",
				curMasterDBUID,
			)
		}
		s.log.Debug().
			Fields(cluster.LogSummaryDBBrief(curMasterDB)).
			Msg("current master database record from cluster data")

		if !curMasterDB.Status.Healthy {
			s.log.Info().
				Str(slog.FieldDBUID, curMasterDB.UID).
				Str(slog.FieldKeeperUID, curMasterDB.Spec.KeeperUID).
				Msg("master db is failed")
			masterOK = false
		}

		// Check that the wanted master is in master state (i.e. check that promotion from standby to master happened)
		if s.dbConvergenceState(
			curMasterDB,
			clusterSpec.ConvergenceTimeout.Duration,
		) == ConvergenceFailed {
			s.log.Info().
				Str(slog.FieldDBUID, curMasterDB.UID).
				Str(slog.FieldKeeperUID, curMasterDB.Spec.KeeperUID).
				Msg("db not converged")
			masterOK = false
		}

		if !masterOK {
			s.log.Info().
				Msg("trying to find a new master to replace failed master")
			bestNewMasters := s.findBestNewMasters(newcd, curMasterDB)
			if len(bestNewMasters) == 0 {
				s.log.Error().Msg("no eligible masters")
			} else {
				// if synchronous replication is enabled, only choose new master in the synchronous replication standbys.
				var bestNewMasterDB *cluster.DB
				if curMasterDB.Spec.SynchronousReplication {
					commonSyncStandbys := slicesutil.CommonElements(curMasterDB.Status.SynchronousStandbys, curMasterDB.Spec.SynchronousStandbys)
					if len(commonSyncStandbys) == 0 {
						s.log.Warn().
							Strs(
								"reported_sync_standbys",
								curMasterDB.Status.SynchronousStandbys,
							).
							Strs(
								"spec_sync_standbys",
								curMasterDB.Spec.SynchronousStandbys,
							).
							Msg(
								"cannot choose synchronous standby since there are no " +
									"common elements between the latest master reported " +
									"synchronous standbys and the db spec ones",
							)
					} else {
						for _, nm := range bestNewMasters {
							if slices.Contains(commonSyncStandbys, nm.UID) {
								bestNewMasterDB = nm
								break
							}
						}
						if bestNewMasterDB == nil {
							s.log.Warn().
								Strs(
									"reported_sync_standbys",
									curMasterDB.Status.SynchronousStandbys,
								).
								Strs(
									"spec_sync_standbys",
									curMasterDB.Spec.SynchronousStandbys,
								).
								Strs("common_sync_standbys", commonSyncStandbys).
								Interface(
									"possible_masters",
									cluster.LogSummaryDBList(bestNewMasters),
								).
								Msg(
									"cannot choose synchronous standby: no match between " +
										"possible masters and usable synchronous standbys",
								)
						}
					}
				} else {
					bestNewMasterDB = bestNewMasters[0]
				}
				if bestNewMasterDB != nil {
					s.log.Info().
						Str(slog.FieldDBUID, bestNewMasterDB.UID).
						Str(slog.FieldKeeperUID, bestNewMasterDB.Spec.KeeperUID).
						Msg("electing db as the new master")
					wantedMasterDBUID = bestNewMasterDB.UID
				} else {
					s.log.Error().Msg("no eligible masters")
				}
			}
		}

		// New master elected
		if curMasterDBUID != wantedMasterDBUID {
			// maintain the current role of the old master and just remove followers
			oldMasterdb := newcd.DBs[curMasterDBUID]
			oldMasterdb.Spec.Followers = []string{}

			masterDBRole := common.RoleMaster
			var followConfig *cluster.FollowConfig
			if *clusterSpec.Role == cluster.ClusterRoleStandby {
				masterDBRole = common.RoleStandby
				followConfig = &cluster.FollowConfig{
					Type:                    cluster.FollowTypeExternal,
					StandbySettings:         clusterSpec.StandbyConfig.StandbySettings,
					ArchiveRecoverySettings: clusterSpec.StandbyConfig.ArchiveRecoverySettings,
				}
			}

			newcd.Cluster.Status.Master = wantedMasterDBUID
			newMasterDB := newcd.DBs[wantedMasterDBUID]
			newMasterDB.Spec.Role = masterDBRole
			newMasterDB.Spec.FollowConfig = followConfig

			// Tell proxy that there's currently no active master
			if newcd.Proxy.Spec.MasterDBUID != "" {
				// Tell proxies that there's currently no active master
				newcd.Proxy.Spec.MasterDBUID = ""
				newcd.Proxy.Generation++
			}

			// Setup synchronous standbys to the one of the previous master (replacing ourself with the previous master)
			if s.syncRepl(clusterSpec) {
				newMasterDB.Spec.SynchronousReplication = true
				newMasterDB.Spec.SynchronousStandbys = []string{}
				newMasterDB.Spec.ExternalSynchronousStandbys = []string{}
				for _, dbUID := range oldMasterdb.Spec.SynchronousStandbys {
					if dbUID != newMasterDB.UID {
						newMasterDB.Spec.SynchronousStandbys = append(
							newMasterDB.Spec.SynchronousStandbys,
							dbUID,
						)
					} else {
						newMasterDB.Spec.SynchronousStandbys = append(newMasterDB.Spec.SynchronousStandbys, oldMasterdb.UID)
					}
				}
				if len(newMasterDB.Spec.SynchronousStandbys) == 0 &&
					*clusterSpec.MinSynchronousStandbys > 0 {
					newMasterDB.Spec.ExternalSynchronousStandbys = []string{
						fakeStandbyName,
					}
				}

				// Just sort to always have them in the same order and avoid
				// unneeded updates to synchronous_standby_names by the keeper.
				sort.Strings(newMasterDB.Spec.SynchronousStandbys)
				sort.Strings(
					newMasterDB.Spec.ExternalSynchronousStandbys,
				)
			} else {
				newMasterDB.Spec.SynchronousReplication = false
				newMasterDB.Spec.SynchronousStandbys = nil
				newMasterDB.Spec.ExternalSynchronousStandbys = nil
			}
		}

		if curMasterDBUID == wantedMasterDBUID {
			masterDB := newcd.DBs[curMasterDBUID]

			if newcd.Proxy.Spec.MasterDBUID == "" {
				// if the Proxy.Spec.MasterDBUID is empty we have to wait for all
				// the proxies to have converged to be sure they closed connections
				// to previous master or disappear (in this case we assume that they
				// have closed connections to previous master)
				unconvergedProxiesUIDs := []string{}
				for _, pi := range pis {
					if pi.Generation != newcd.Proxy.Generation {
						unconvergedProxiesUIDs = append(
							unconvergedProxiesUIDs,
							pi.UID,
						)
					}
				}
				if len(unconvergedProxiesUIDs) > 0 {
					s.log.Info().
						Strs("unconverged_proxy_uids", unconvergedProxiesUIDs).
						Msg("waiting for proxies to be converged to the current generation")
				} else {
					// Tell proxy that there's a new active master
					newcd.Proxy.Spec.MasterDBUID = wantedMasterDBUID
					newcd.Proxy.Generation++
				}
			} else {
				// if we have Proxy.Spec.MasterDBUID != "" then we have waited for
				// proxies to have converged and we can set enabled proxies to
				// the currently available proxies in proxyInfo.
				enabledProxies := []string{}
				for _, pi := range pis {
					enabledProxies = append(enabledProxies, pi.UID)
				}
				sort.Strings(enabledProxies)
				if !reflect.DeepEqual(newcd.Proxy.Spec.EnabledProxies, enabledProxies) {
					newcd.Proxy.Spec.EnabledProxies = enabledProxies
					newcd.Proxy.Generation++
				}
			}

			// change master db role to "master" if the cluster role has been changed in the spec
			if *clusterSpec.Role == cluster.ClusterRoleMaster {
				masterDB.Spec.Role = common.RoleMaster
				masterDB.Spec.FollowConfig = nil
			}

			// Set standbys to follow master only if it's healthy and converged
			if masterDB.Status.Healthy &&
				s.dbConvergenceState(
					masterDB,
					clusterSpec.ConvergenceTimeout.Duration,
				) == Converged {
				// Remove old masters
				toRemove := []*cluster.DB{}
				for _, db := range newcd.DBs {
					if db.UID == wantedMasterDBUID {
						continue
					}
					dt, err := s.dbType(newcd, db.UID)
					if err != nil {
						s.log.Warn().
							Err(err).
							Str(slog.FieldDBUID, db.UID).
							Msg("skipping old master cleanup because database type cannot be determined")
						continue
					}
					if dt != dbTypePrimaryLine {
						continue
					}
					s.log.Info().
						Str(slog.FieldDBUID, db.UID).
						Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
						Msg("removing old master db")
					toRemove = append(toRemove, db)
				}
				for _, db := range toRemove {
					delete(newcd.DBs, db.UID)
				}

				// Remove invalid dbs
				toRemove = []*cluster.DB{}
				for _, db := range newcd.DBs {
					if db.UID == wantedMasterDBUID {
						continue
					}
					v, err := s.dbValidity(newcd, db.UID)
					if err != nil {
						s.log.Warn().
							Err(err).
							Str(slog.FieldDBUID, db.UID).
							Msg("skipping invalid database cleanup because validity cannot be determined")
						continue
					}
					if v != dbValidityInvalid {
						continue
					}
					s.log.Info().
						Str(slog.FieldDBUID, db.UID).
						Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
						Msg("removing invalid db")
					toRemove = append(toRemove, db)
				}
				for _, db := range toRemove {
					delete(newcd.DBs, db.UID)
				}

				// Remove dbs that won't sync due to missing wals on current master
				toRemove = []*cluster.DB{}
				for _, db := range newcd.DBs {
					canSync, err := s.dbCanSync(cd, db.UID)
					if err != nil {
						s.log.Warn().
							Err(err).
							Str(slog.FieldDBUID, db.UID).
							Msg("skipping database sync check")
						continue
					}
					if canSync {
						continue
					}
					s.log.Info().
						Str(slog.FieldDBUID, db.UID).
						Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
						Msg(
							"removing db that won't be able to sync due to " +
								"missing wals on current master",
						)
					toRemove = append(toRemove, db)
				}
				for _, db := range toRemove {
					delete(newcd.DBs, db.UID)
				}

				goodStandbys, failedStandbys, convergingStandbys := s.validStandbysByStatus(
					newcd,
				)
				goodStandbysCount := len(goodStandbys)
				failedStandbysCount := len(failedStandbys)
				convergingStandbysCount := len(convergingStandbys)
				s.log.Debug().
					Int("good_standbys", goodStandbysCount).
					Int("failed_standbys", failedStandbysCount).
					Int("converging_standbys", convergingStandbysCount).
					Msg("standby convergence counts")

				// Clean InitMode for goodStandbys
				for _, db := range goodStandbys {
					db.Spec.InitMode = cluster.DBInitModeNone
				}

				// Setup synchronous standbys
				if s.syncRepl(clusterSpec) {
					minSynchronousStandbys := int(
						*clusterSpec.MinSynchronousStandbys,
					)
					maxSynchronousStandbys := int(
						*clusterSpec.MaxSynchronousStandbys,
					)
					merge := true

					// if the current known in sync syncstandbys are different than the required ones wait for them and remove non good ones
					if !slicesutil.CompareStringSliceNoOrder(
						masterDB.Status.SynchronousStandbys,
						masterDB.Spec.SynchronousStandbys,
					) {
						// remove old syncstandbys from current status
						masterDB.Status.SynchronousStandbys = slicesutil.CommonElements(
							masterDB.Status.SynchronousStandbys,
							masterDB.Spec.SynchronousStandbys,
						)

						// add reported in sync syncstandbys to the current status
						curSyncStandbys := slicesutil.CommonElements(
							masterDB.Status.CurSynchronousStandbys,
							masterDB.Spec.SynchronousStandbys,
						)
						toAddSyncStandbys := slicesutil.Difference(
							curSyncStandbys,
							masterDB.Status.SynchronousStandbys,
						)
						masterDB.Status.SynchronousStandbys = append(
							masterDB.Status.SynchronousStandbys,
							toAddSyncStandbys...)

						// if some of the non yet in sync syncstandbys are failed, set Spec.SynchronousStandbys to the current in sync ones, se other could be added.
						notInSyncSyncStandbys := slicesutil.Difference(
							masterDB.Spec.SynchronousStandbys,
							masterDB.Status.SynchronousStandbys,
						)
						update := false
						for _, dbUID := range notInSyncSyncStandbys {
							if _, ok := newcd.DBs[dbUID]; !ok {
								s.log.Info().
									Str(slog.FieldDBUID, dbUID).
									Strs("in_sync_standbys", masterDB.Status.SynchronousStandbys).
									Strs("spec_sync_standbys", masterDB.Spec.SynchronousStandbys).
									Msg("one of the new synchronousStandbys has been removed")
								update = true
								continue
							}
							if _, ok := goodStandbys[dbUID]; !ok {
								s.log.Info().
									Str(slog.FieldDBUID, dbUID).
									Strs("in_sync_standbys", masterDB.Status.SynchronousStandbys).
									Strs("spec_sync_standbys", masterDB.Spec.SynchronousStandbys).
									Msg("one of the new synchronousStandbys is not in good state")
								update = true
								continue
							}
						}
						if update {
							// Use the current known in sync syncStandbys as Spec.SynchronousStandbys
							s.log.Info().
								Strs(
									"in_sync_standbys",
									masterDB.Status.SynchronousStandbys,
								).
								Strs(
									"spec_sync_standbys",
									masterDB.Spec.SynchronousStandbys,
								).
								Msg(
									"setting expected sync standbys to current " +
										"in-sync standbys",
								)
							masterDB.Spec.SynchronousStandbys = masterDB.Status.SynchronousStandbys

							// Just sort to always have them in the same order and avoid
							// unneeded updates to synchronous_standby_names by the keeper.
							sort.Strings(
								masterDB.Spec.SynchronousStandbys,
							)
						}
					}

					// update synchronousStandbys only if the reported
					// SynchronousStandbys are the same as the required ones. In
					// this way, when we have to choose a new master we are sure
					// that there're no intermediate changes between the
					// reported standbys and the required ones.
					if !slicesutil.CompareStringSliceNoOrder(
						masterDB.Status.SynchronousStandbys,
						masterDB.Spec.SynchronousStandbys,
					) {
						s.log.Info().
							Strs("in_sync_standbys", curMasterDB.Status.SynchronousStandbys).
							Strs("spec_sync_standbys", curMasterDB.Spec.SynchronousStandbys).
							Msg("waiting for new defined synchronous standbys to be in sync")
					} else {
						addFakeStandby := false
						externalSynchronousStandbys := map[string]struct{}{}

						// make a map of synchronous standbys starting from the current ones
						prevSynchronousStandbys := map[string]struct{}{}
						synchronousStandbys := map[string]struct{}{}

						for _, dbUID := range masterDB.Spec.SynchronousStandbys {
							prevSynchronousStandbys[dbUID] = struct{}{}
							synchronousStandbys[dbUID] = struct{}{}
						}

						// Remove not existing dbs (removed above)
						toRemove := map[string]struct{}{}
						for dbUID := range synchronousStandbys {
							if _, ok := newcd.DBs[dbUID]; !ok {
								s.log.Info().
									Str(slog.FieldDBUID, masterDB.UID).
									Str("standby_db_uid", dbUID).
									Msg(
										"removing non existent db from " +
											"synchronousStandbys",
									)
								toRemove[dbUID] = struct{}{}
							}
						}
						for dbUID := range toRemove {
							delete(synchronousStandbys, dbUID)
						}

						// Check if the current synchronous standbys are healthy or remove them
						toRemove = map[string]struct{}{}
						for dbUID := range synchronousStandbys {
							if _, ok := goodStandbys[dbUID]; !ok {
								s.log.Info().
									Str(slog.FieldDBUID, masterDB.UID).
									Str("standby_db_uid", dbUID).
									Msg("removing failed synchronous standby")
								toRemove[dbUID] = struct{}{}
							}
						}
						for dbUID := range toRemove {
							delete(synchronousStandbys, dbUID)
						}

						// Remove synchronous standbys in excess
						if len(synchronousStandbys) > maxSynchronousStandbys {
							rc := len(synchronousStandbys) - maxSynchronousStandbys
							removedCount := 0
							toRemove = map[string]struct{}{}
							for dbUID := range synchronousStandbys {
								if removedCount >= rc {
									break
								}
								s.log.Info().
									Str(slog.FieldDBUID, masterDB.UID).
									Str("standby_db_uid", dbUID).
									Msg("removing synchronous standby in excess")
								toRemove[dbUID] = struct{}{}
								removedCount++
							}
							for dbUID := range toRemove {
								delete(synchronousStandbys, dbUID)
							}
						}

						// try to add missing standbys up to MaxSynchronousStandbys
						bestStandbys := s.findBestStandbys(newcd, curMasterDB)

						ac := maxSynchronousStandbys - len(synchronousStandbys)
						addedCount := 0
						for _, bestStandby := range bestStandbys {
							if addedCount >= ac {
								break
							}
							if _, ok := synchronousStandbys[bestStandby.UID]; ok {
								continue
							}

							// ignore standbys that cannot be synchronous standbys
							if db, ok := newcd.DBs[bestStandby.UID]; ok {
								if keeper, ok := newcd.Keepers[db.Spec.KeeperUID]; ok && (keeper.Status.CanBeSynchronousReplica != nil && !*keeper.Status.CanBeSynchronousReplica) {
									s.log.Info().
										Str(slog.FieldDBUID, db.UID).
										Str(slog.FieldKeeperUID, keeper.UID).
										Msg(
											"cannot choose standby as synchronous " +
												"(--can-be-synchronous-replica=false)",
										)
									continue
								}
							}

							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Str("standby_db_uid", bestStandby.UID).
								Str(slog.FieldKeeperUID, bestStandby.Spec.KeeperUID).
								Msg(
									"adding new synchronous standby in good state " +
										"trying to reach MaxSynchronousStandbys",
								)
							synchronousStandbys[bestStandby.UID] = struct{}{}
							addedCount++
						}

						// If there're some missing standbys to reach
						// MinSynchronousStandbys, keep previous sync standbys,
						// also if not in a good state. In this way we have more
						// possibilities to choose a sync standby to replace a
						// failed master if they becoe healthy again
						ac = minSynchronousStandbys - len(synchronousStandbys)
						addedCount = 0
						for _, db := range newcd.DBs {
							if addedCount >= ac {
								break
							}
							if _, ok := synchronousStandbys[db.UID]; ok {
								continue
							}
							if _, ok := prevSynchronousStandbys[db.UID]; ok {
								s.log.Info().
									Str(slog.FieldDBUID, masterDB.UID).
									Str("standby_db_uid", db.UID).
									Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
									Msg(
										"adding previous synchronous standby to " +
											"reach MinSynchronousStandbys",
									)
								synchronousStandbys[db.UID] = struct{}{}
								addedCount++
							}
						}

						if merge {
							// if some of the new synchronousStandbys are not inside
							// the prevSynchronousStandbys then also add all
							// the prevSynchronousStandbys. In this way when there's
							// a synchronousStandbys change we'll have, in a first
							// step, both the old and the new standbys, then in the
							// second step the old will be removed (since the new
							// standbys are all inside prevSynchronousStandbys), so
							// we'll always be able to choose a sync standby that we
							// know was defined in the primary and in sync if the
							// primary fails.
							allInPrev := true
							for k := range synchronousStandbys {
								if _, ok := prevSynchronousStandbys[k]; !ok {
									allInPrev = false
								}
							}
							if !allInPrev {
								s.log.Info().
									Str(slog.FieldDBUID, masterDB.UID).
									Strs(
										"prev_sync_standby_uids",
										sortedStringSetKeys(prevSynchronousStandbys),
									).
									Strs(
										"target_sync_standby_uids",
										sortedStringSetKeys(synchronousStandbys),
									).
									Msg("merging current and previous synchronous standbys")
								// use only existing dbs
								for _, db := range newcd.DBs {
									if _, ok := prevSynchronousStandbys[db.UID]; ok {
										s.log.Info().
											Str(slog.FieldDBUID, masterDB.UID).
											Str("standby_db_uid", db.UID).
											Str(
												slog.FieldKeeperUID,
												db.Spec.KeeperUID,
											).
											Msg("adding previous synchronous standby")
										synchronousStandbys[db.UID] = struct{}{}
									}
								}
							}
						}

						if !reflect.DeepEqual(synchronousStandbys, prevSynchronousStandbys) {
							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Strs(
									"prev_sync_standby_uids",
									sortedStringSetKeys(prevSynchronousStandbys),
								).
								Strs(
									"target_sync_standby_uids",
									sortedStringSetKeys(synchronousStandbys),
								).
								Msg("synchronousStandbys changed")
						} else {
							s.log.Debug().
								Str(slog.FieldDBUID, masterDB.UID).
								Strs(
									"prev_sync_standby_uids",
									sortedStringSetKeys(prevSynchronousStandbys),
								).
								Strs(
									"target_sync_standby_uids",
									sortedStringSetKeys(synchronousStandbys),
								).
								Msg("synchronous standbys unchanged")
						}

						// If there're not enough real synchronous standbys add a fake synchronous standby because we have to be strict and make the master block transactions until MinSynchronousStandbys real standbys are available
						if len(synchronousStandbys)+len(externalSynchronousStandbys) < minSynchronousStandbys {
							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Int("min_sync_standbys_required", minSynchronousStandbys).
								Msg(
									"using a fake synchronous standby since there are " +
										"not enough real standbys available",
								)
							addFakeStandby = true
						}

						masterDB.Spec.SynchronousReplication = true
						masterDB.Spec.SynchronousStandbys = []string{}
						masterDB.Spec.ExternalSynchronousStandbys = []string{}
						for dbUID := range synchronousStandbys {
							masterDB.Spec.SynchronousStandbys = append(masterDB.Spec.SynchronousStandbys, dbUID)
						}

						for dbUID := range externalSynchronousStandbys {
							masterDB.Spec.ExternalSynchronousStandbys = append(masterDB.Spec.ExternalSynchronousStandbys, dbUID)
						}

						if addFakeStandby {
							masterDB.Spec.ExternalSynchronousStandbys = append(masterDB.Spec.ExternalSynchronousStandbys, fakeStandbyName)
						}

						// remove old syncstandbys from current status
						masterDB.Status.SynchronousStandbys = slicesutil.CommonElements(masterDB.Status.SynchronousStandbys, masterDB.Spec.SynchronousStandbys)

						// Just sort to always have them in the same order and avoid
						// unneeded updates to synchronous_standby_names by the keeper.
						sort.Strings(masterDB.Spec.SynchronousStandbys)
						sort.Strings(masterDB.Spec.ExternalSynchronousStandbys)
					}
				} else {
					masterDB.Spec.SynchronousReplication = false
					masterDB.Spec.SynchronousStandbys = nil
					masterDB.Spec.ExternalSynchronousStandbys = nil

					masterDB.Status.SynchronousStandbys = nil
				}

				// NotFailed != Good since there can be some dbs that are converging
				// it's the total number of standbys - the failed standbys
				// or the sum of good + converging standbys
				notFailedStandbysCount := goodStandbysCount + convergingStandbysCount

				// Remove dbs in excess if we have a good number >= MaxStandbysPerSender
				// We don't remove failed db until the number of good db is >= MaxStandbysPerSender since they can come back
				maxStandbysPerSender := int(
					*clusterSpec.MaxStandbysPerSender,
				)
				if goodStandbysCount >= maxStandbysPerSender {
					toRemove := []*cluster.DB{}
					// Remove all non good standbys
					for _, db := range newcd.DBs {
						dt, err := s.dbType(newcd, db.UID)
						if err != nil {
							s.log.Warn().
								Err(err).
								Str(slog.FieldDBUID, db.UID).
								Msg("skipping standby cleanup because database type cannot be determined")
							continue
						}
						if dt != dbTypeReplicaLine {
							continue
						}
						// Don't remove standbys marked as synchronous standbys
						if slices.Contains(
							masterDB.Spec.SynchronousStandbys,
							db.UID,
						) {
							continue
						}
						if _, ok := goodStandbys[db.UID]; !ok {
							s.log.Info().
								Str(slog.FieldDBUID, db.UID).
								Msg("removing non good standby")
							toRemove = append(toRemove, db)
						}
					}
					// Remove good standbys in excess
					nr := goodStandbysCount - maxStandbysPerSender
					i := 0
					for _, db := range goodStandbys {
						if i >= nr {
							break
						}
						// Don't remove standbys marked as synchronous standbys
						if slices.Contains(
							masterDB.Spec.SynchronousStandbys,
							db.UID,
						) {
							continue
						}
						s.log.Info().
							Str(slog.FieldDBUID, db.UID).
							Msg("removing good standby in excess")
						toRemove = append(toRemove, db)
						i++
					}
					for _, db := range toRemove {
						delete(newcd.DBs, db.UID)
					}
				} else {
					// Add new dbs to substitute failed dbs, if there're available keepers.

					// nc can be negative if MaxStandbysPerSender has been lowered
					nc := maxStandbysPerSender - notFailedStandbysCount
					// Add missing DBs until MaxStandbysPerSender
					freeKeepers := s.freeKeepers(newcd)
					nf := len(freeKeepers)
					for i := 0; i < nc && i < nf; i++ {
						freeKeeper := freeKeepers[i]
						db := &cluster.DB{
							UID:        s.UIDFn(),
							Generation: cluster.InitialGeneration,
							Spec: &cluster.DBSpec{
								KeeperUID:    freeKeeper.UID,
								InitMode:     cluster.DBInitModeResync,
								Role:         common.RoleStandby,
								Followers:    []string{},
								FollowConfig: &cluster.FollowConfig{Type: cluster.FollowTypeInternal, DBUID: wantedMasterDBUID},
							},
						}
						newcd.DBs[db.UID] = db
						s.log.Info().
							Str(slog.FieldDBUID, db.UID).
							Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
							Msg("added new standby db")
					}
				}

				// Reconfigure all standbys as followers of the current master
				for _, db := range newcd.DBs {
					dt, err := s.dbType(newcd, db.UID)
					if err != nil {
						s.log.Warn().
							Err(err).
							Str(slog.FieldDBUID, db.UID).
							Msg("skipping standby reconfiguration because database type cannot be determined")
						continue
					}
					if dt != dbTypeReplicaLine {
						continue
					}

					db.Spec.Role = common.RoleStandby
					// Remove followers
					db.Spec.Followers = []string{}
					db.Spec.FollowConfig = &cluster.FollowConfig{
						Type:  cluster.FollowTypeInternal,
						DBUID: wantedMasterDBUID,
					}

					db.Spec.SynchronousReplication = false
					db.Spec.SynchronousStandbys = nil
					db.Spec.ExternalSynchronousStandbys = nil
				}
			}
		}

		// Update followers for master DB
		// Always do this since, in future, keepers and related db could be
		// removed (currently only dead keepers without an assigned db are
		// removed)
		masterDB := newcd.DBs[curMasterDBUID]
		masterDB.Spec.Followers = []string{}
		for _, db := range newcd.DBs {
			if masterDB.UID == db.UID {
				continue
			}
			fc := db.Spec.FollowConfig
			if fc != nil {
				if fc.Type == cluster.FollowTypeInternal &&
					fc.DBUID == wantedMasterDBUID {
					masterDB.Spec.Followers = append(
						masterDB.Spec.Followers,
						db.UID,
					)
				}
			}
		}
		// Sort followers so the slice won't be considered changed due to different order of the same entries.
		sort.Strings(masterDB.Spec.Followers)

	default:
		return nil, fmt.Errorf(
			"unknown cluster phase %s",
			cd.Cluster.Status.Phase,
		)
	}

	// Copy the clusterSpec parameters to the dbSpec
	s.setDBSpecFromClusterSpec(newcd)

	// Update generation on DBs if they have changed
	for dbUID, db := range newcd.DBs {
		prevDB, ok := cd.DBs[dbUID]
		if !ok {
			continue
		}
		if !reflect.DeepEqual(db.Spec, prevDB.Spec) {
			s.log.Debug().
				Str("db_uid", dbUID).
				Int64("generation_before_bump", db.Generation).
				Int64("generation_after_bump", db.Generation+1).
				Msg("database specification changed; bumping generation")
			db.Generation++
		}
	}

	// check that we haven't changed the current cd or there's a bug somewhere
	if !reflect.DeepEqual(origcd, cd) {
		return nil, errors.New(
			"cd was changed in updateCluster, this shouldn't happen",
		)
	}
	return newcd, nil
}

func (s *Sentinel) updateChangeTimes(cd, newcd *cluster.ClusterData) {
	newcd.ChangeTime = time.Now()

	if !reflect.DeepEqual(newcd.Cluster, cd.Cluster) {
		newcd.Cluster.ChangeTime = time.Now()
	}

	for dbUID, db := range newcd.DBs {
		prevDB, ok := cd.DBs[dbUID]
		if !ok {
			db.ChangeTime = time.Now()
			continue
		}
		if !reflect.DeepEqual(db, prevDB) {
			db.ChangeTime = time.Now()
		}
	}

	for keeperUID, keeper := range newcd.Keepers {
		prevKeeper, ok := cd.Keepers[keeperUID]
		if !ok {
			keeper.ChangeTime = time.Now()
			continue
		}
		if !reflect.DeepEqual(keeper, prevKeeper) {
			keeper.ChangeTime = time.Now()
		}
	}

	if !reflect.DeepEqual(newcd.Proxy, cd.Proxy) {
		newcd.Proxy.ChangeTime = time.Now()
	}
}

// ConvergenceState describes database convergence progress.
type ConvergenceState uint

const (
	// Converging means the database has not yet reached the wanted generation.
	Converging ConvergenceState = iota
	// Converged means the database reached the wanted generation.
	Converged
	// ConvergenceFailed means the database did not converge before timeout.
	ConvergenceFailed
)

func (s *Sentinel) isKeeperHealthy(
	cd *cluster.ClusterData,
	keeper *cluster.Keeper,
) bool {
	t, ok := s.keeperErrorTimers[keeper.UID]
	if !ok {
		return true
	}
	if timer.Since(t) > cd.Cluster.DefSpec().FailInterval.Duration {
		return false
	}
	return true
}

func (s *Sentinel) isDBHealthy(
	cd *cluster.ClusterData,
	db *cluster.DB,
) bool {
	t, ok := s.dbErrorTimers[db.UID]
	if !ok {
		return true
	}
	if timer.Since(t) > cd.Cluster.DefSpec().FailInterval.Duration {
		return false
	}
	return true
}

func (s *Sentinel) isDBIncreasingXLogPos(db *cluster.DB) bool {
	t, ok := s.dbNotIncreasingXLogPos[db.UID]
	if !ok {
		return true
	}
	if t > cluster.DefaultDBNotIncreasingXLogPosTimes {
		return false
	}
	return true
}

func (s *Sentinel) updateDBConvergenceInfos(cd *cluster.ClusterData) {
	for _, db := range cd.DBs {
		if db.Status.CurrentGeneration == db.Generation {
			delete(s.dbConvergenceInfos, db.UID)
			continue
		}
		nd := &DBConvergenceInfo{
			Generation: db.Generation,
			Timer:      timer.Now(),
		}
		d, ok := s.dbConvergenceInfos[db.UID]
		if !ok {
			s.dbConvergenceInfos[db.UID] = nd
		} else if d.Generation != db.Generation {
			s.dbConvergenceInfos[db.UID] = nd
		}
	}
}

func (s *Sentinel) runLeadershipSanitySweep(cd *cluster.ClusterData) {
	s.keeperErrorTimers = make(map[string]int64)
	s.dbErrorTimers = make(map[string]int64)
	s.dbNotIncreasingXLogPos = make(map[string]int64)
	s.keeperInfoHistories = make(KeeperInfoHistories)
	s.dbConvergenceInfos = make(map[string]*DBConvergenceInfo)
	s.proxyInfoHistories = make(ProxyInfoHistories)

	// Rebuild convergence tracking from current cluster data so post-pause
	// reconcile starts from a consistent in-memory state.
	s.updateDBConvergenceInfos(cd)
}

func (s *Sentinel) dbConvergenceState(
	db *cluster.DB,
	timeout time.Duration,
) ConvergenceState {
	if db.Status.CurrentGeneration == db.Generation {
		return Converged
	}
	if timeout != 0 {
		d, ok := s.dbConvergenceInfos[db.UID]
		if !ok {
			d = &DBConvergenceInfo{
				Generation: db.Generation,
				Timer:      timer.Now(),
			}
			s.dbConvergenceInfos[db.UID] = d
			s.log.Debug().
				Str(slog.FieldDBUID, db.UID).
				Msg("database convergence tracking initialized")
		}
		if timer.Since(d.Timer) > timeout {
			return ConvergenceFailed
		}
	}
	return Converging
}

// KeeperInfoHistory tracks the latest keeper info observed by the sentinel.
type KeeperInfoHistory struct {
	// KeeperInfo is last keeper info snapshot.
	KeeperInfo *cluster.KeeperInfo
	// Seen reports whether keeper was seen in current loop.
	Seen bool
	// Timer is monotonic timestamp used for failure tracking.
	Timer int64
}

// KeeperInfoHistories maps keeper UID to keeper info history.
type KeeperInfoHistories map[string]*KeeperInfoHistory

// DeepCopy returns an independent copy of keeper info histories.
func (k KeeperInfoHistories) DeepCopy() (KeeperInfoHistories, error) {
	if k == nil {
		return nil, nil
	}
	nk, err := copystructure.Copy(k)
	if err != nil {
		return nil, fmt.Errorf(
			"deep copy keeper info histories: %w",
			err,
		)
	}
	out := nk.(KeeperInfoHistories)
	if !reflect.DeepEqual(k, out) {
		return nil, errors.New(
			"deep copy keeper info histories: result not equal to source",
		)
	}
	return out, nil
}

// DBConvergenceInfo tracks convergence timing for a database generation.
type DBConvergenceInfo struct {
	// Generation is DB generation being tracked.
	Generation int64
	// Timer is monotonic timestamp when convergence tracking started.
	Timer int64
}

// ProxyInfoHistory tracks the latest proxy info observed by the sentinel.
type ProxyInfoHistory struct {
	// ProxyInfo is last proxy info snapshot.
	ProxyInfo *cluster.ProxyInfo
	// Timer is monotonic timestamp for proxy liveness tracking.
	Timer int64
}

// ProxyInfoHistories maps proxy UID to proxy info history.
type ProxyInfoHistories map[string]*ProxyInfoHistory

// DeepCopy returns an independent copy of proxy info histories.
func (p ProxyInfoHistories) DeepCopy() (ProxyInfoHistories, error) {
	if p == nil {
		return nil, nil
	}
	np, err := copystructure.Copy(p)
	if err != nil {
		return nil, fmt.Errorf(
			"deep copy proxy info histories: %w",
			err,
		)
	}
	out := np.(ProxyInfoHistories)
	if !reflect.DeepEqual(p, out) {
		return nil, errors.New(
			"deep copy proxy info histories: result not equal to source",
		)
	}
	return out, nil
}

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
	keeperErrorTimers map[string]int64
	// DB unhealthy timers keyed by DB UID.
	dbErrorTimers map[string]int64
	// Timers for DBs not advancing WAL position.
	dbNotIncreasingXLogPos map[string]int64
	// Cached convergence tracking keyed by DB UID.
	dbConvergenceInfos map[string]*DBConvergenceInfo

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
}

func sortedStringSetKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// NewSentinel creates a sentinel from command configuration.
func NewSentinel(
	uid string,
	cfg *config,
	clusterName string,
	initialClusterSpecFile string,
	end chan bool,
) (*Sentinel, error) {
	logger := slog.WithComponent("sentinel").With().
		Str(slog.FieldClusterName, clusterName).
		Logger()
	var initialClusterSpec *cluster.ClusterSpec
	if initialClusterSpecFile != "" {
		configData, err := os.ReadFile(initialClusterSpecFile)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot read provided initial cluster config file: %v",
				err,
			)
		}
		initialClusterSpec, err = configfile.ClusterSpec(configData)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot parse provided initial cluster config: %v",
				err,
			)
		}
		logger.Debug().
			Fields(cluster.LogSummaryClusterSpec(initialClusterSpec)).
			Msg("initial cluster specification loaded from file")
		if err := initialClusterSpec.Validate(); err != nil {
			return nil, fmt.Errorf("invalid initial cluster: %v", err)
		}
	}

	e, err := runtimecommon.NewStoreForCluster(&cfg.CommonConfig, clusterName, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	election, err := runtimecommon.NewElectionForCluster(&cfg.CommonConfig, clusterName, uid)
	if err != nil {
		return nil, fmt.Errorf("cannot create election: %v", err)
	}
	publisher, err := newKubeServicePublisher(cfg, clusterName, logger)
	if err != nil {
		return nil, err
	}

	return &Sentinel{
		uid:                  uid,
		cfg:                  cfg,
		e:                    e,
		election:             election,
		kubeServicePublisher: publisher,
		clusterName:          clusterName,
		log:                  logger,
		leader:               false,
		initialClusterSpec:   initialClusterSpec,
		end:                  end,
		UIDFn:                id.UID,
		// This is just to choose a pseudo random keeper so
		// use math.rand (no need for crypto.rand) without an
		// initial seed.
		RandFn: rand.Intn,

		sleepInterval:  cluster.DefaultSleepInterval,
		requestTimeout: cluster.DefaultRequestTimeout,
	}, nil
}

// Start runs sentinel leader election and cluster reconciliation loops.
func (s *Sentinel) Start(ctx context.Context) {
	endCh := make(chan struct{})

	timerCh := time.NewTimer(0).C

	go s.electionLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			s.log.Info().Msg("stopping hysteron sentinel")
			if s.end != nil {
				s.end <- true
			}
			return
		case <-timerCh:
			go func() {
				s.clusterSentinelCheck(ctx)
				endCh <- struct{}{}
			}()
		case <-endCh:
			timerCh = time.NewTimer(s.sleepInterval).C
		}
	}
}

func (s *Sentinel) leaderInfo() (bool, uint) {
	s.leaderMutex.Lock()
	defer s.leaderMutex.Unlock()
	return s.leader, s.leadershipCount
}

func (s *Sentinel) clusterSentinelCheck(pctx context.Context) {
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	e := s.e

	cd, prevCDPair, err := e.GetClusterData(pctx)
	if err != nil {
		s.log.Error().Err(err).Msg("error retrieving cluster data")
		return
	}
	if cd != nil {
		if cd.FormatVersion != cluster.CurrentCDFormatVersion {
			s.log.Error().
				Uint64("format_version", cd.FormatVersion).
				Msg("unsupported cluster data format version")
			return
		}
		if err = cd.Cluster.Spec.Validate(); err != nil {
			s.log.Error().Err(err).Msg("cluster data validation failed")
			return
		}
		if cd.Cluster != nil {
			s.sleepInterval = cd.Cluster.DefSpec().SleepInterval.Duration
			s.requestTimeout = cd.Cluster.DefSpec().RequestTimeout.Duration
		}
	}

	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data at start of sentinel reconciliation")

	if cd == nil {
		// Cluster first initialization
		if s.initialClusterSpec == nil {
			s.log.Info().
				Msg("no cluster data available, waiting for it to appear")
			return
		}
		c := cluster.NewCluster(s.UIDFn(), s.initialClusterSpec)
		s.log.Info().Msg("writing initial cluster data")
		newcd := cluster.NewClusterData(c)
		s.log.Debug().
			Fields(cluster.LogSummaryClusterData(newcd)).
			Msg("cluster data to persist on first cluster initialization")
		if _, err = e.AtomicPutClusterData(pctx, newcd, nil); err != nil {
			s.log.Error().Err(err).Msg("error saving cluster data")
		}
		return
	}

	if err = s.setSentinelInfo(pctx, 2*s.sleepInterval); err != nil {
		s.log.Error().Err(err).Msg("cannot update sentinel info")
		return
	}

	keepersInfo, err := s.e.GetKeepersInfo(pctx)
	if err != nil {
		s.log.Error().Err(err).Msg("cannot get keepers info")
		return
	}
	s.log.Debug().
		Interface("keepers_info", cluster.LogSummaryKeepersInfo(keepersInfo)).
		Msg("keeper info map from store")

	proxiesInfo, err := s.e.GetProxiesInfo(pctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to get proxies info")
		return
	}

	isLeader, leadershipCount := s.leaderInfo()
	if !isLeader {
		if slog.IsTrace() {
			s.log.Trace().
				Uint("leadership_epoch", leadershipCount).
				Msg("skipping cluster reconciliation: not sentinel leader")
		}
		return
	}

	// detect if this is the first check after (re)gaining leadership
	firstRun := false
	if s.lastLeadershipCount != leadershipCount {
		firstRun = true
		s.lastLeadershipCount = leadershipCount
	}

	// if this is the first check after (re)gaining leadership reset all
	// the internal timers
	if firstRun {
		s.log.Info().
			Uint("leadership_epoch", leadershipCount).
			Msg("running post-leadership sanity sweep")
		s.runLeadershipSanitySweep(cd)
	}

	newcd, newKeeperInfoHistories, err := s.updateKeepersStatus(
		cd,
		keepersInfo,
		firstRun,
	)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to update keeper status")
		return
	}
	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(newcd)).
		Msg("cluster data after merging keeper health and reported state")

	activeProxiesInfos, err := s.activeProxiesInfos(proxiesInfo)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to compute active proxy info")
		return
	}

	newcd, err = s.updateCluster(newcd, activeProxiesInfos)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to update cluster data")
		return
	}
	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(newcd)).
		Msg("cluster data after sentinel failover and convergence logic")

	if newcd != nil {
		s.updateChangeTimes(cd, newcd)
		if _, err := e.AtomicPutClusterData(pctx, newcd, prevCDPair); err != nil {
			s.log.Error().Err(err).Msg("error saving cluster data")
			return
		}
	}
	if s.kubeServicePublisher != nil {
		if err := s.kubeServicePublisher.Publish(pctx, newcd); err != nil {
			s.log.Error().Err(err).Msg("failed to publish Kubernetes Services")
			return
		}
	}

	// Save the new keeperInfoHistories only on successful cluster data
	// update or in the next run we'll think that the saved keeperInfo was
	// already applied.
	s.keeperInfoHistories = newKeeperInfoHistories

	// Update db convergence timers using the new cluster data
	s.updateDBConvergenceInfos(newcd)

	// We only update this metric when we've completed all actions in this method
	// successfully. That enables us to alert on when Sentinels are failing to
	// correctly sync.
	lastCheckSuccessSeconds.WithLabelValues(s.clusterName).SetToCurrentTime()
}

func sigHandler(sigs chan os.Signal, cancel context.CancelFunc) {
	s := <-sigs
	log.Debug().
		Str("signal", s.String()).
		Msg("received shutdown signal")
	cancel()
}

func clusterSpecFiles(defaultSpec string, overrides []string, clusterNames []string) (map[string]string, error) {
	clusterSet := map[string]struct{}{}
	for _, name := range clusterNames {
		clusterSet[name] = struct{}{}
	}

	specs := map[string]string{}
	for _, name := range clusterNames {
		if defaultSpec != "" {
			specs[name] = defaultSpec
		}
	}

	for _, override := range overrides {
		name, path, ok := strings.Cut(override, "=")
		name = strings.TrimSpace(name)
		path = strings.TrimSpace(path)
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("invalid cluster spec override %q, expected <cluster-name>=<path>", override)
		}
		if _, ok := clusterSet[name]; !ok {
			return nil, fmt.Errorf("cluster spec override references unknown cluster %q", name)
		}
		if _, ok := specs[name]; ok && specs[name] != defaultSpec {
			return nil, fmt.Errorf("duplicate cluster spec override for cluster %q", name)
		}
		specs[name] = path
	}
	return specs, nil
}

func runSentinelCluster(ctx context.Context, uid string, cfg *config, clusterName, initialSpecFile string) {
	logger := slog.WithComponent("sentinel").With().
		Str(slog.FieldClusterName, clusterName).
		Logger()
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		s, err := NewSentinel(uid, cfg, clusterName, initialSpecFile, nil)
		if err != nil {
			logger.Error().
				Err(err).
				Dur("retry_after", backoff).
				Msg("failed to create sentinel cluster runner")
			if !waitForSentinelRetry(ctx, backoff) {
				return
			}
			backoff = nextSentinelRetryBackoff(backoff, maxBackoff)
			continue
		}

		runtimecommon.SetMetricsForCluster(clusterName, "sentinel")
		if err := runSentinelOnce(ctx, s); err != nil {
			s.log.Error().
				Err(err).
				Dur("retry_after", backoff).
				Msg("sentinel cluster runner stopped unexpectedly")
			if !waitForSentinelRetry(ctx, backoff) {
				return
			}
			backoff = nextSentinelRetryBackoff(backoff, maxBackoff)
			continue
		}
		return
	}
}

func waitForSentinelRetry(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextSentinelRetryBackoff(current, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func runSentinelOnce(ctx context.Context, s *Sentinel) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in sentinel cluster runner: %v", r)
		}
	}()
	s.Start(ctx)
	if ctx.Err() != nil {
		return nil
	}
	return errors.New("sentinel cluster runner returned without cancellation")
}

// newParser creates a parser for runtime sentinel options. Built-in helper
// commands remain available; subcommands are optional because the
// sentinel is a daemon.
func newParser() *flags.Parser {
	parser := runtimecommon.NewParser("hysteron sentinel", "HYSTERON", &cfg, 0)
	parser.SubcommandsOptional = true
	return parser
}

// Run starts sentinel with externally prepared common config and optional
// sentinel-specific CLI arguments.
func Run(commonConfig stconfig.CommonConfig, args []string) error {
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	parser := newParser()
	if _, err := parser.ParseArgs(args); err != nil {
		return err
	}
	if parser.Active != nil {
		return nil
	}
	return runSentinel()
}

func runSentinel() error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	log = slog.WithComponent("sentinel")
	pg.SetLogger(slog.L())
	defer runtimecommon.CloseLogging(closer, &log)

	clusterNames, err := runtimecommon.CheckClusterNames(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("invalid cluster names: %w", err)
	}
	if err := runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}
	if err := checkSentinelConfig(&cfg); err != nil {
		return fmt.Errorf("invalid sentinel configuration: %w", err)
	}

	specFiles, err := clusterSpecFiles(cfg.InitialClusterSpecFile, cfg.ClusterSpecFiles, clusterNames)
	if err != nil {
		return fmt.Errorf("invalid cluster spec configuration: %w", err)
	}

	uid := id.UID()
	log.Info().Str(slog.FieldSentinelUID, uid).Msg("sentinel UID assigned")

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go sigHandler(sigs, cancel)

	if cfg.Metrics.ListenAddress != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsServer := http.Server{
			Addr:              cfg.Metrics.ListenAddress,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			err := metricsServer.ListenAndServe()
			if err != nil {
				log.Error().Err(err).Msg("metrics http server error")
				cancel()
			}
		}()
	}

	var wg sync.WaitGroup
	for _, clusterName := range clusterNames {
		wg.Go(func() {
			runSentinelCluster(ctx, uid, &cfg, clusterName, specFiles[clusterName])
		})
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

func checkSentinelConfig(cfg *config) error {
	if (cfg.KubeService.Enabled || cfg.KubeService.ReadOnlyEnabled) &&
		stconfig.NormalizeStoreBackend(cfg.Store.Backend) != "kubernetes" {
		return errors.New("kubernetes service publishing requires --store-backend=kubernetes")
	}
	if cfg.KubeService.Enabled &&
		cfg.KubeService.ReadOnlyEnabled &&
		cfg.KubeService.ServiceName == cfg.KubeService.ReadOnlyServiceName &&
		cfg.KubeService.ServicePort == cfg.KubeService.ReadOnlyServicePort {
		return errors.New("kubernetes writable and read-only services cannot use the same name and port")
	}
	return nil
}
