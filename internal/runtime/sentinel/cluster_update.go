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
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	slog "github.com/woozymasta/hysteron/internal/log"
	slicesutil "github.com/woozymasta/hysteron/internal/utils/slices"
)

// updateCluster computes next cluster data state from current cluster view and
// active proxies, applying initialization, failover, and standby management
// policies.
func (s *Sentinel) updateCluster(cd *cluster.ClusterData, pis cluster.ProxiesInfo) (*cluster.ClusterData, error) {
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
		if cd.Cluster.Status.PauseActive(time.Now().UTC()) {
			s.log.Info().
				Msg("cluster pause is active; skipping automatic HA state mutations")
			return newcd, nil
		}

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
			bestNewMasterDB, clearBackoff := s.chooseBestNewMaster(
				newcd,
				curMasterDB,
				clusterSpec.FailInterval.Duration,
			)
			if clearBackoff {
				delete(s.leaderRaceBackoffTimers, curMasterDBUID)
			}
			if bestNewMasterDB != nil {
				s.log.Info().
					Str(slog.FieldDBUID, bestNewMasterDB.UID).
					Str(slog.FieldKeeperUID, bestNewMasterDB.Spec.KeeperUID).
					Msg("electing db as the new master")
				wantedMasterDBUID = bestNewMasterDB.UID
			}
		} else {
			delete(s.leaderRaceBackoffTimers, curMasterDBUID)
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

						// if some of the non yet in sync syncstandbys are failed,
						// set Spec.SynchronousStandbys to the current in sync ones, se other could be added.
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
								Strs("in_sync_standbys", masterDB.Status.SynchronousStandbys).
								Strs("spec_sync_standbys", masterDB.Spec.SynchronousStandbys).
								Msg("setting expected sync standbys to current in-sync standbys")
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
									Msg("removing non existent db from synchronousStandbys")
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
										Msg("cannot choose standby as synchronous (--can-be-synchronous-replica=false)")
									continue
								}
							}

							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Str("standby_db_uid", bestStandby.UID).
								Str(slog.FieldKeeperUID, bestStandby.Spec.KeeperUID).
								Msg("adding new synchronous standby in good state trying to reach MaxSynchronousStandbys")
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
									Msg("adding previous synchronous standby to reach MinSynchronousStandbys")
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
									Strs("prev_sync_standby_uids", sortedStringSetKeys(prevSynchronousStandbys)).
									Strs("target_sync_standby_uids", sortedStringSetKeys(synchronousStandbys)).
									Msg("merging current and previous synchronous standbys")
								// use only existing dbs
								for _, db := range newcd.DBs {
									if _, ok := prevSynchronousStandbys[db.UID]; ok {
										s.log.Info().
											Str(slog.FieldDBUID, masterDB.UID).
											Str("standby_db_uid", db.UID).
											Str(slog.FieldKeeperUID, db.Spec.KeeperUID).
											Msg("adding previous synchronous standby")
										synchronousStandbys[db.UID] = struct{}{}
									}
								}
							}
						}

						if !reflect.DeepEqual(synchronousStandbys, prevSynchronousStandbys) {
							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Strs("prev_sync_standby_uids", sortedStringSetKeys(prevSynchronousStandbys)).
								Strs("target_sync_standby_uids", sortedStringSetKeys(synchronousStandbys)).
								Msg("synchronousStandbys changed")
						} else {
							s.log.Debug().
								Str(slog.FieldDBUID, masterDB.UID).
								Strs("prev_sync_standby_uids", sortedStringSetKeys(prevSynchronousStandbys)).
								Strs("target_sync_standby_uids", sortedStringSetKeys(synchronousStandbys)).
								Msg("synchronous standbys unchanged")
						}

						// If there're not enough real synchronous standbys add a fake synchronous standby
						// because we have to be strict and make the master block transactions
						// until MinSynchronousStandbys real standbys are available
						if len(synchronousStandbys)+len(externalSynchronousStandbys) < minSynchronousStandbys {
							s.log.Info().
								Str(slog.FieldDBUID, masterDB.UID).
								Int("min_sync_standbys_required", minSynchronousStandbys).
								Msg("using a fake synchronous standby since there are not enough real standbys available")
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
						masterDB.Status.SynchronousStandbys = slicesutil.CommonElements(
							masterDB.Status.SynchronousStandbys, masterDB.Spec.SynchronousStandbys)

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

	if masterUID := newcd.Cluster.Status.Master; masterUID != "" {
		if masterDB, ok := newcd.DBs[masterUID]; ok && masterDB.Spec != nil {
			ttl := newcd.Cluster.DefSpec().MemberReplicationSlotTTL
			if ttl != nil && ttl.Duration > 0 {
				prevFollowers := []string{}
				if prevMasterDB, ok := cd.DBs[masterUID]; ok && prevMasterDB.Spec != nil {
					prevFollowers = prevMasterDB.Spec.Followers
				}
				masterChanged := cd.Cluster.Status.Master != "" &&
					cd.Cluster.Status.Master != masterUID
				masterDB.Status.OrphanMemberSlots = computeOrphanMemberSlots(
					prevFollowers,
					masterDB.Spec.Followers,
					masterDB.Status.OrphanMemberSlots,
					masterChanged,
					time.Now(),
				)
			} else {
				masterDB.Status.OrphanMemberSlots = nil
			}
		}
	}

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

// computeOrphanMemberSlots updates tracked orphaned member replication slots
// after follower set changes, preserving previously tracked orphan timestamps.
func computeOrphanMemberSlots(
	prevFollowers, currentFollowers []string,
	prevOrphans map[string]time.Time,
	masterChanged bool,
	now time.Time,
) map[string]time.Time {
	if masterChanged {
		return nil
	}

	out := map[string]time.Time{}
	maps.Copy(out, prevOrphans)

	current := map[string]struct{}{}
	for _, followerUID := range currentFollowers {
		slot := common.HysteronName(followerUID)
		current[slot] = struct{}{}
		delete(out, slot)
	}

	for _, followerUID := range prevFollowers {
		slot := common.HysteronName(followerUID)
		if _, managedNow := current[slot]; managedNow {
			continue
		}
		if _, alreadyTracked := out[slot]; alreadyTracked {
			continue
		}
		out[slot] = now
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
