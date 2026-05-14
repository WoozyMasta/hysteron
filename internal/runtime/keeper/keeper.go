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
	"context"
	"reflect"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
	slicesutil "github.com/woozymasta/hysteron/internal/utils/slices"
)

// Start runs keeper reconciliation loops until the context is canceled.
func (p *PostgresKeeper) Start(ctx context.Context) {
	cd := p.loadInitialClusterData()
	p.logInitialClusterData(cd)

	if err := p.setupPostgresManager(); err != nil {
		p.end <- err
		return
	}

	_ = p.pgm.StopIfStarted(true)
	go p.standbyLogicalSlotAdvanceWorker(ctx)
	p.runStartLoops(ctx)
}

func (p *PostgresKeeper) postgresKeeperSM(pctx context.Context) {
	start := time.Now()
	const reconcilePhase = "postgres_keeper_sm"
	defer func() {
		reconcileDurationSeconds.WithLabelValues(reconcilePhase).Observe(time.Since(start).Seconds())
	}()

	clusterStore := p.e
	pgManager := p.pgm

	cd, _, err := clusterStore.GetClusterData(pctx)
	if err != nil {
		reconcileErrorsTotal.WithLabelValues(reconcilePhase, "get_cluster_data").Inc()
		p.handleDCSDegraded(time.Now(), err)
		p.baseLog().
			Error().
			Err(err).
			Msg("error retrieving cluster data")
		return
	}
	p.handleDCSRecovered()
	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data snapshot before state machine step")

	if cd == nil {
		p.baseLog().
			Info().
			Str("store_backend", p.cfg.Store.Backend).
			Msg("cluster data not in store yet; waiting before managing PostgreSQL")
		return
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		reconcileErrorsTotal.WithLabelValues(reconcilePhase, "unsupported_clusterdata_format").Inc()
		p.baseLog().
			Error().
			Uint64("version", cd.FormatVersion).
			Msg("unsupported clusterdata format version")
		return
	}
	if err = cd.Cluster.Spec.Validate(); err != nil {
		reconcileErrorsTotal.WithLabelValues(reconcilePhase, "invalid_cluster_spec").Inc()
		p.baseLog().
			Error().
			Err(err).
			Msg("clusterdata validation failed")
		return
	}

	// Mark that the clusterdata we've received is valid. We'll use this metric to detect
	// when our store is failing to serve a valid clusterdata, so it's important we only
	// update the metric here.
	clusterdataLastValidUpdateSeconds.SetToCurrentTime()

	if cd.Cluster != nil {
		p.applyRuntimeConfigFromClusterData(cd)

		if p.keeperLocalState.ClusterUID != cd.Cluster.UID {
			p.keeperLocalState.ClusterUID = cd.Cluster.UID
			if err = p.saveKeeperLocalState(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "save_keeper_local_state").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save keeper local state")
				return
			}
		}
	}

	k, ok := cd.Keepers[p.keeperLocalState.UID]
	if !ok {
		p.baseLog().
			Info().
			Str(log.FieldKeeperUID, p.keeperLocalState.UID).
			Msg("this keeper is not listed in cluster data yet; waiting")
		return
	}

	db := cd.FindDB(k)
	if db == nil {
		p.baseLog().
			Info().
			Str(log.FieldKeeperUID, k.UID).
			Msg("no database is assigned to this keeper yet; stopping PostgreSQL if it is running")
		if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
		}
		return
	}

	if p.bootUUID != k.Status.BootUUID {
		p.baseLog().Info().
			Str("local_boot_uuid", p.bootUUID).
			Str("cluster_boot_uuid", k.Status.BootUUID).
			Msg("boot UID from local process differs from cluster data; stopping PostgreSQL until sentinel updates cluster state")
		if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
		}
		return
	}

	// Generate hba auth from clusterData
	pgManager.SetHba(p.generateHBA(cd, db, p.waitSyncStandbysSynced))

	p.baseLog().Debug().
		Str(log.FieldDBUID, db.UID).
		Int64("db_generation", db.Generation).
		Int64("db_status_generation", db.Status.CurrentGeneration).
		Str("db_role", string(db.Spec.Role)).
		Str("db_init_mode", string(db.Spec.InitMode)).
		Msg("reconciling assigned database: applying cluster specification to local PostgreSQL (state machine tick)")

	var pgParameters common.Parameters

	dbLocalState := p.dbLocalStateCopy()
	if dbLocalState.Initializing {
		// If we are here this means that the db initialization or
		// resync has failed so we have to clean up stale data
		p.baseLog().Error().Msg("db failed to initialize or resync")

		if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
			return
		}

		// Clean up cluster db datadir
		if err = pgManager.RemoveAll(); err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "remove_data_dir").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to remove the postgres data dir")
			return
		}
		// Reset current db local state since it's not valid anymore
		nextDBLocalState := &DBLocalState{
			UID:          "",
			Generation:   cluster.NoGeneration,
			Initializing: false,
		}
		if err = p.saveDBLocalState(nextDBLocalState); err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "save_db_local_state").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to save db local state")
			return
		}
	}

	if p.dbLocalState.UID != db.UID {
		var initialized bool
		initialized, err = pgManager.IsInitialized()
		if err != nil {
			reconcileErrorsTotal.WithLabelValues(reconcilePhase, "is_initialized").Inc()
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to detect if instance is initialized")
			return
		}
		p.baseLog().Info().
			Str("local_db_uid", p.dbLocalState.UID).
			Str(log.FieldDBUID, db.UID).
			Msg("local database UID does not match cluster assignment; will re-initialize or resync as required")

		pgManager.SetRecoveryOptions(nil)
		p.waitSyncStandbysSynced = false

		switch db.Spec.InitMode {
		case cluster.DBInitModeNew:
			p.baseLog().Info().Msg("initializing the database cluster")
			nextDBLocalState := &DBLocalState{
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(nextDBLocalState); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "save_db_local_state").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgManager postgres parameters
			pgManager.SetParameters(pgParameters)

			initConfig := &postgresql.InitConfig{}

			if db.Spec.NewConfig != nil {
				initConfig.Locale = db.Spec.NewConfig.Locale
				initConfig.Encoding = db.Spec.NewConfig.Encoding
				initConfig.DataChecksums = db.Spec.NewConfig.DataChecksums
			}

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgManager.RemoveAll(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "remove_data_dir").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to remove the postgres data dir")
				return
			}
			bootstrapStart := time.Now()
			if err = pgManager.Init(initConfig); err != nil {
				bootstrapDurationSeconds.WithLabelValues("new").Observe(time.Since(bootstrapStart).Seconds())
				bootstrapTotal.WithLabelValues("new", "error").Inc()
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "init_postgres").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to initialize postgres database cluster")
				return
			}
			bootstrapDurationSeconds.WithLabelValues("new").Observe(time.Since(bootstrapStart).Seconds())
			bootstrapTotal.WithLabelValues("new", "success").Inc()

			if err = pgManager.StartTmpMerged(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "start_tmp_merged").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}
			if err = pgManager.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "wait_ready").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
			if db.Spec.IncludeConfig {
				pgParameters, err = pgManager.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				nextDBLocalState.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(nextDBLocalState); err != nil {
					reconcileErrorsTotal.WithLabelValues(reconcilePhase, "save_db_local_state").Inc()
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}

			p.baseLog().
				Info().
				Msg("database files created; creating replication and application roles")
			if err = pgManager.SetupRoles(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "setup_roles").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to setup roles")
				return
			}

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
		case cluster.DBInitModePITR:
			p.baseLog().
				Info().
				Msg("starting point-in-time recovery / restore into new data directory")
			nextDBLocalState := &DBLocalState{
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(nextDBLocalState); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "save_db_local_state").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgManager postgres parameters
			pgManager.SetParameters(pgParameters)

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "stop_postgres").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgManager.RemoveAll(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "remove_data_dir").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to remove the postgres data dir")
				return
			}
			p.baseLog().
				Info().
				Msg("running archive restore command from cluster specification")
			bootstrapStart := time.Now()
			if err = pgManager.Restore(db.Spec.PITRConfig.DataRestoreCommand); err != nil {
				bootstrapDurationSeconds.WithLabelValues("pitr").Observe(time.Since(bootstrapStart).Seconds())
				bootstrapTotal.WithLabelValues("pitr", "error").Inc()
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "restore_data").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to restore postgres database cluster")
				return
			}
			bootstrapDurationSeconds.WithLabelValues("pitr").Observe(time.Since(bootstrapStart).Seconds())
			bootstrapTotal.WithLabelValues("pitr", "success").Inc()

			recoveryMode := postgresql.RecoveryModeRecovery
			var standbySettings *cluster.StandbySettings
			if db.Spec.FollowConfig != nil &&
				db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
				recoveryMode = postgresql.RecoveryModeStandby
				standbySettings = db.Spec.FollowConfig.StandbySettings
			}

			pgManager.SetRecoveryOptions(
				p.createRecoveryOptions(
					recoveryMode,
					standbySettings,
					db.Spec.PITRConfig.ArchiveRecoverySettings,
					db.Spec.PITRConfig.RecoveryTargetSettings,
				),
			)

			if err = pgManager.StartTmpMerged(); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "start_tmp_merged").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}

			if recoveryMode == postgresql.RecoveryModeRecovery {
				// wait for the db having replyed all the wals
				p.baseLog().
					Info().
					Str(log.FieldDBUID, db.UID).
					Msg("waiting for PostgreSQL to finish replaying WAL (PITR)")
				if err = pgManager.WaitRecoveryDone(cd.Cluster.DefSpec().SyncTimeout.Duration); err != nil {
					reconcileErrorsTotal.WithLabelValues(reconcilePhase, "wait_recovery_done").Inc()
					p.baseLog().
						Error().
						Err(err).
						Str(log.FieldDBUID, db.UID).
						Msg("point-in-time recovery did not finish within the configured timeout")
					return
				}
				p.baseLog().
					Info().
					Str(log.FieldDBUID, db.UID).
					Msg("point-in-time recovery replay completed")
			}
			if err = pgManager.WaitReady(cd.Cluster.DefSpec().SyncTimeout.Duration); err != nil {
				reconcileErrorsTotal.WithLabelValues(reconcilePhase, "wait_ready").Inc()
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}

			if db.Spec.IncludeConfig {
				pgParameters, err = pgManager.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				nextDBLocalState.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(nextDBLocalState); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}

		case cluster.DBInitModeResync:
			p.baseLog().Info().Msg("database resync requested")
			nextDBLocalState := &DBLocalState{
				// replace our current db uid with the required one.
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(nextDBLocalState); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgManager postgres parameters
			pgManager.SetParameters(pgParameters)

			var systemID string
			if !initialized {
				p.baseLog().Info().Msg("database cluster is not initialized")
			} else {
				systemID, err = pgManager.GetSystemdID()
				if err != nil {
					p.baseLog().Error().Err(err).Msg("error retrieving systemd ID")
					return
				}
			}

			followedUID := db.Spec.FollowConfig.DBUID
			followedDB, ok := cd.DBs[followedUID]
			if !ok {
				p.baseLog().
					Error().
					Str("followed_db", followedUID).
					Msg("followed database is missing from cluster data")
				return
			}

			masterDB, ok := cd.DBs[cd.Cluster.Status.Master]
			masterOlderWal := ""
			if ok {
				masterOlderWal = masterDB.Status.OlderWalFile
			}
			decision := evaluatePgrewindDecision(
				initialized,
				systemID,
				followedDB.Status.SystemID,
				ok,
				db.Status.XLogPos,
				masterOlderWal,
			)
			tryPgrewind := decision.try
			switch decision.reason {
			case pgrewindReasonNotInitialized:
				p.baseLog().Info().Msg("pg_rewind disabled because local database is not initialized")
			case pgrewindReasonSystemIDDiff:
				p.baseLog().Warn().Msg("pg_rewind disabled because local and followed system IDs differ")
			case pgrewindReasonNoMaster:
				p.baseLog().Warn().Msg("pg_rewind disabled because no master database is available")
			case pgrewindReasonWalCheckErr:
				p.baseLog().Warn().
					Err(decision.walCheckErr).
					Str("older_master_wal", masterOlderWal).
					Msg("cannot verify required WAL availability for pg_rewind path")
			case pgrewindReasonWalMissing:
				p.baseLog().Info().
					Str("required_wal", decision.requiredWal).
					Str("older_master_wal", decision.olderWal).
					Msg("pg_rewind disabled because required WAL is no longer available on master")
			}

			// pg_rewind can leave a node on a diverged branch in edge cases.
			// Verify branch alignment after rewind and force full resync when
			// divergence is still detected.

			// A rewinded standby needs WAL from the master starting from the
			// common ancestor. If those WAL files are unavailable or startup
			// still stalls, fall back to full resync with pg_basebackup.
			if err = p.resync(db, masterDB, followedDB, tryPgrewind); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to resync from followed instance")
				return
			}
			if err = pgManager.Start(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}

			if tryPgrewind {
				fullResync := false
				// If standby is not accepting connections after rewind, assume it is
				// blocked waiting for missing WAL and fallback to full basebackup resync.
				if err = pgManager.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("standby did not become ready after pg_rewind, forcing full resync")
					fullResync = true
				} else {
					// Check again if it was really synced
					var pgState *cluster.PostgresState
					pgState, err = p.GetPGState(pctx)
					if err != nil {
						p.baseLog().Error().Err(err).Msg("cannot get current pgstate")
						return
					}
					if p.isDifferentTimelineBranch(followedDB, pgState) {
						fullResync = true
					}
				}

				if fullResync {
					if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to stop pg instance")
						return
					}
					if err = p.resync(db, masterDB, followedDB, false); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to resync from followed instance")
						return
					}
				}
			}

		case cluster.DBInitModeExisting:
			nextDBLocalState := &DBLocalState{
				// replace our current db uid with the required one.
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: false,
			}
			if err = p.saveDBLocalState(nextDBLocalState); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgManager postgres parameters
			pgManager.SetParameters(pgParameters)

			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgManager.StartTmpMerged(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}
			if err = pgManager.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
			if db.Spec.IncludeConfig {
				pgParameters, err = pgManager.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				nextDBLocalState.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(nextDBLocalState); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}
			if err = p.stopPostgresIfStarted(pgManager, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
		case cluster.DBInitModeNone:
			p.baseLog().
				Error().
				Msg("local database state invariant broken: init mode is none")
			return
		default:
			p.baseLog().
				Error().
				Str("db_init_mode", string(db.Spec.InitMode)).
				Msg("unknown database init mode")
			return
		}
	}

	initialized, err := pgManager.IsInitialized()
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("failed to detect if instance is initialized")
		return
	}

	if initialized {
		var started bool
		started, err = pgManager.IsStarted()
		if err != nil {
			// log error getting instance state but go ahead.
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve instance status")
		}
		p.baseLog().
			Debug().
			Bool("initialized", true).
			Bool("pg_started", started).
			Msg("database instance status")
	} else {
		p.baseLog().
			Debug().
			Bool("initialized", false).
			Bool("pg_started", false).
			Msg("database instance status")
	}

	// create postgres parameters
	pgParameters = p.createPGParameters(db)
	// update pgManager postgres parameters
	pgManager.SetParameters(pgParameters)

	var localRole common.Role
	if !initialized {
		p.baseLog().Info().Msg("database cluster is not initialized")
		localRole = common.RoleUndefined
	} else {
		localRole, err = pgManager.GetRole()
		if err != nil {
			p.baseLog().Error().Err(err).Msg("error retrieving current pg role")
			return
		}
	}

	targetRole := db.Spec.Role
	p.baseLog().
		Debug().
		Str("target_role", string(targetRole)).
		Msg("applying target PostgreSQL role")

	// Set metrics to power alerts about mismatched roles
	setRole(localRoleGauge, &localRole)
	setRole(targetRoleGauge, &targetRole)

	switch targetRole {
	case common.RoleMaster:
		// We are the elected master
		p.baseLog().Info().Msg("applying requested master role")
		if localRole == common.RoleUndefined {
			p.baseLog().
				Error().
				Msg("master role requested but data directory is uninitialized")
			return
		}

		pgManager.SetRecoveryOptions(nil)

		started, err := pgManager.IsStarted()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve instance status")
			return
		}
		if !started {
			// if we have syncrepl enabled and the postgres instance is stopped, before opening connections to normal users wait for having the defined synchronousStandbys in sync state.
			if db.Spec.SynchronousReplication {
				p.waitSyncStandbysSynced = true
				p.baseLog().
					Info().
					Msg("restricting normal users in pg_hba until synchronous standbys catch up")
				pgManager.SetHba(p.generateHBA(cd, db, true))
			}

			if err = pgManager.Start(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start postgres")
				return
			}
			if err = pgManager.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
		}

		if localRole == common.RoleStandby {
			if err = p.runPrePromoteHook(db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Str(log.FieldDBUID, db.UID).
					Msg("pre-promote hook failed; refusing promote")
				return
			}
			p.baseLog().Info().Msg("promoting standby to master")
			if err = pgManager.Promote(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to promote instance")
				return
			}
		} else {
			p.baseLog().Info().Msg("PostgreSQL is already primary")
		}

		if err := p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("error updating replication slots")
			return
		}

	case common.RoleStandby:
		// We are a standby
		var standbySettings *cluster.StandbySettings
		switch db.Spec.FollowConfig.Type {
		case cluster.FollowTypeInternal:
			followedUID := db.Spec.FollowConfig.DBUID
			p.baseLog().
				Info().
				Str("followed_db", followedUID).
				Msg("applying requested standby role")
			followedDB, ok := cd.DBs[followedUID]
			if !ok {
				p.baseLog().
					Error().
					Str("followed_db", followedUID).
					Msg("followed database is missing from cluster data")
				return
			}
			replConnParams := p.getReplConnParams(db, followedDB)
			standbySettings = &cluster.StandbySettings{
				PrimaryConninfo: replConnParams.ConnString(),
				PrimarySlotName: common.HysteronName(db.UID),
			}
		case cluster.FollowTypeExternal:
			standbySettings = db.Spec.FollowConfig.StandbySettings
		default:
			p.baseLog().
				Error().
				Str("follow_type", string(db.Spec.FollowConfig.Type)).
				Msg("unknown follow type")
			return
		}
		switch localRole {
		case common.RoleMaster:
			p.baseLog().
				Error().
				Msg("refusing invalid transition from master to standby")
			return
		case common.RoleStandby:
			p.baseLog().Info().Msg("PostgreSQL is already standby")
			started, err := pgManager.IsStarted()
			if err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to retrieve instance status")
				return
			}
			if !started {
				pgManager.SetRecoveryOptions(
					p.createRecoveryOptions(
						postgresql.RecoveryModeStandby,
						standbySettings,
						nil,
						nil,
					),
				)
				if err = pgManager.Start(); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to start postgres")
					return
				}
			}

			// Update our primary_conninfo if replConnString changed
			switch db.Spec.FollowConfig.Type {
			case cluster.FollowTypeInternal:
				followedUID := db.Spec.FollowConfig.DBUID
				followedDB, ok := cd.DBs[followedUID]
				if !ok {
					p.baseLog().
						Error().
						Str("followed_db", followedUID).
						Msg("followed database is missing from cluster data")
					return
				}
				newReplConnParams := p.getReplConnParams(db, followedDB)
				p.baseLog().
					Debug().
					Fields(postgresql.LogSummaryConnParams(newReplConnParams)).
					Msg("standby replication connection parameters updated")

				standbySettings := &cluster.StandbySettings{
					PrimaryConninfo: newReplConnParams.ConnString(),
					PrimarySlotName: common.HysteronName(db.UID),
				}

				curRecoveryOptions := pgManager.CurRecoveryOptions()
				newRecoveryOptions := p.createRecoveryOptions(
					postgresql.RecoveryModeStandby,
					standbySettings,
					nil,
					nil,
				)

				// Update recovery conf if parameters has changed
				if !curRecoveryOptions.RecoveryParameters.Equals(
					newRecoveryOptions.RecoveryParameters,
				) {
					p.baseLog().Info().
						Interface("recovery_prev", postgresql.LogSummaryRecoveryParameters(curRecoveryOptions.RecoveryParameters)).
						Interface("recovery_new", postgresql.LogSummaryRecoveryParameters(newRecoveryOptions.RecoveryParameters)).
						Msg("recovery parameters changed; restarting PostgreSQL")
					pgManager.SetRecoveryOptions(newRecoveryOptions)
					p.runBeforeStopHook(db)

					if err = pgManager.Restart(true); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to restart postgres instance")
						return
					}
				}

				if err = p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("error updating replication slots")
				}

			case cluster.FollowTypeExternal:
				curRecoveryOptions := pgManager.CurRecoveryOptions()
				newRecoveryOptions := p.createRecoveryOptions(
					postgresql.RecoveryModeStandby,
					db.Spec.FollowConfig.StandbySettings,
					db.Spec.FollowConfig.ArchiveRecoverySettings,
					nil,
				)

				// Update recovery conf if parameters has changed
				if !curRecoveryOptions.RecoveryParameters.Equals(
					newRecoveryOptions.RecoveryParameters,
				) {
					p.baseLog().Info().
						Interface("recovery_prev", postgresql.LogSummaryRecoveryParameters(curRecoveryOptions.RecoveryParameters)).
						Interface("recovery_new", postgresql.LogSummaryRecoveryParameters(newRecoveryOptions.RecoveryParameters)).
						Msg("recovery parameters changed; restarting PostgreSQL")
					pgManager.SetRecoveryOptions(newRecoveryOptions)
					p.runBeforeStopHook(db)

					if err = pgManager.Restart(true); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to restart postgres instance")
						return
					}
				}

				if err = p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("error updating replication slots")
				}
			}

			p.ensureStandbyWALReplayRunning(pgManager, db.UID)

		case common.RoleUndefined:
			p.baseLog().Info().Msg("current database role is undefined")
			return
		}
	case common.RoleUndefined:
		p.baseLog().Info().Msg("target database role is undefined")
		return
	}

	// update pg parameters
	pgParameters = p.createPGParameters(db)

	// Log synchronous replication changes
	prevSyncStandbyNames := pgManager.CurParameters()["synchronous_standby_names"]
	syncStandbyNames := pgParameters["synchronous_standby_names"]
	if db.Spec.SynchronousReplication {
		if prevSyncStandbyNames != syncStandbyNames {
			p.baseLog().Info().
				Str("sync_standby_names_prev", prevSyncStandbyNames).
				Str("sync_standby_names_new", syncStandbyNames).
				Msg("synchronous standby names changed")
		}
	} else {
		if prevSyncStandbyNames != "" {
			p.baseLog().Info().
				Str("sync_standby_names_cleared", prevSyncStandbyNames).
				Msg("synchronous replication disabled, clearing synchronous standbys")
		}
	}

	needsReload := false

	if !pgParameters.Equals(pgManager.CurParameters()) {
		p.baseLog().Info().Msg("postgres parameters changed, reloading postgres instance")
		pgManager.SetParameters(pgParameters)
		needsReload = true
	} else {
		// for tests
		p.baseLog().Debug().Msg("postgres parameters not changed")
	}

	// Generate hba auth from clusterData

	// if we have syncrepl enabled and the postgres instance is stopped, before opening connections to normal users wait for having the defined synchronousStandbys in sync state.
	if db.Spec.SynchronousReplication && p.waitSyncStandbysSynced {
		inSyncStandbys, err := p.GetInSyncStandbys()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve current in sync standbys from instance")
			return
		}
		if !slicesutil.CompareStringSliceNoOrder(
			inSyncStandbys,
			db.Spec.SynchronousStandbys,
		) {
			p.baseLog().
				Info().
				Msg("waiting for synchronous standbys before allowing normal users")
		} else {
			p.waitSyncStandbysSynced = false
		}
	} else {
		p.waitSyncStandbysSynced = false
	}
	newHBA := p.generateHBA(cd, db, p.waitSyncStandbysSynced)
	if !reflect.DeepEqual(newHBA, pgManager.CurHba()) {
		p.baseLog().Info().Msg("pg_hba changed, reloading postgres instance")
		pgManager.SetHba(newHBA)
		needsReload = true
	} else {
		// for tests
		p.baseLog().Debug().Msg("pg_hba not changed")
	}

	if needsReload {
		needsReloadGauge.Set(1) // mark as reload needed
		if err := pgManager.Reload(); err != nil {
			p.baseLog().Error().Err(err).Msg("failed to reload postgres instance")
		} else {
			needsReloadGauge.Set(0) // successful reload implies no longer required
		}
	}

	{
		clusterSpec := cd.Cluster.DefSpec()
		automaticPgRestartEnabled := *clusterSpec.AutomaticPgRestart

		restartRequirement, err := pgManager.IsRestartRequiredDetailed()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to check if restart is required")
		}

		if restartRequirement != nil && restartRequirement.Required {
			needsRestartGauge.Set(1) // mark as restart needed
			pgPendingRestartGauge.Set(1)
			p.baseLog().
				Warn().
				Strs("pending_restart_params", restartRequirement.PendingParams).
				Msg("PostgreSQL reports pending restart parameters")
			if automaticPgRestartEnabled {
				p.baseLog().Info().Msg("automatic PostgreSQL restart scheduled")
				p.runBeforeStopHook(db)
				if err := pgManager.Restart(true); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to restart postgres instance")
				} else {
					needsRestartGauge.Set(0) // successful restart implies no longer required
					pgPendingRestartGauge.Set(0)
				}
			}
		} else {
			pgPendingRestartGauge.Set(0)
		}
	}

	// If we are here, then all went well and we can update the db generation and save it locally
	nextDBLocalState := p.dbLocalStateCopy()
	nextDBLocalState.Generation = db.Generation
	nextDBLocalState.Initializing = false
	if err := p.saveDBLocalState(nextDBLocalState); err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("failed to save db local state")
		return
	}

	// We want to set this only if no error has occurred. We should be able to identify
	// keeper issues by watching for this value becoming stale.
	lastSyncSuccessSeconds.SetToCurrentTime()
}
