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
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
)

// findInitialKeeper chooses one keeper as bootstrap owner for initial DB setup.
func (s *Sentinel) findInitialKeeper(cd *cluster.ClusterData) (*cluster.Keeper, error) {
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

// setDBSpecFromClusterSpec updates DB specs from current effective cluster spec.
func (s *Sentinel) setDBSpecFromClusterSpec(cd *cluster.ClusterData) {
	clusterSpec := cd.Cluster.DefSpec()
	for _, db := range cd.DBs {
		db.Spec.RequestTimeout = *clusterSpec.RequestTimeout
		db.Spec.MaxStandbys = *clusterSpec.MaxStandbys
		db.Spec.UsePgrewind = *clusterSpec.UsePgrewind
		db.Spec.CheckpointBeforePgrewind = clusterSpec.CheckpointBeforePgrewind != nil &&
			*clusterSpec.CheckpointBeforePgrewind
		if clusterSpec.ReplicationTLSMode != nil {
			db.Spec.ReplicationTLSMode = *clusterSpec.ReplicationTLSMode
		} else {
			db.Spec.ReplicationTLSMode = ""
		}
		db.Spec.PGParameters = clusterSpec.PGParameters
		db.Spec.PGHBA = clusterSpec.PGHBA
		db.Spec.BeforeStopCommand = clusterSpec.BeforeStopCommand
		db.Spec.PrePromoteCommand = clusterSpec.PrePromoteCommand
		db.Spec.EnableLogicalSlotFailover = clusterSpec.EnableLogicalSlotFailover

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
			db.Spec.IgnoreReplicationSlotMatchers = clusterSpec.IgnoreMasterReplicationSlotMatchers
			db.Spec.ManagedLogicalReplicationSlots = clusterSpec.ManagedLogicalReplicationSlots
			db.Spec.NoStream = false

		case dbTypeReplicaLine:
			db.Spec.AdditionalReplicationSlots = nil
			db.Spec.IgnoreReplicationSlots = nil
			db.Spec.IgnoreReplicationSlotMatchers = nil
			db.Spec.NoStream = clusterSpec.StandbyConfig != nil && clusterSpec.StandbyConfig.NoStream
			if clusterSpec.EnableLogicalSlotFailover {
				db.Spec.ManagedLogicalReplicationSlots = clusterSpec.ManagedLogicalReplicationSlots
			} else {
				db.Spec.ManagedLogicalReplicationSlots = nil
			}
			// Additional physical slot policy is primary-line only.
			// Logical slot failover behavior is staged behind
			// enableLogicalSlotFailover and currently standby readiness-only.
		}
	}
}

// freeKeepers returns healthy keepers that currently have no DB assignment.
func (s *Sentinel) freeKeepers(cd *cluster.ClusterData) []*cluster.Keeper {
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

// applyInitialDBConvergence transitions initialization state based on
// convergence result of the bootstrap DB.
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
