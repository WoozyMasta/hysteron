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
	"fmt"
	"slices"
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/id"
)

// GetInSyncStandbys returns Hysteron standby UIDs currently reported as synchronous.
func (p *PostgresKeeper) GetInSyncStandbys() ([]string, error) {
	inSyncStandbysFullName, err := p.pgm.GetSyncStandbys()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to retrieve current sync standbys status from instance: %v",
			err,
		)
	}

	inSyncStandbys := []string{}
	for _, standbyName := range inSyncStandbysFullName {
		if common.IsHysteronName(standbyName) {
			inSyncStandbys = append(
				inSyncStandbys,
				common.NameFromHysteronName(standbyName),
			)
		}
	}

	return inSyncStandbys, nil
}

// GetPGState returns the current PostgreSQL state observed by the keeper.
func (p *PostgresKeeper) GetPGState(
	_ context.Context,
) (*cluster.PostgresState, error) {
	p.getPGStateMutex.Lock()
	defer p.getPGStateMutex.Unlock()
	// Just get one pgstate at a time to avoid exausting available connections.
	pgState := &cluster.PostgresState{}

	dbLocalState := p.dbLocalStateCopy()
	pgState.UID = dbLocalState.UID
	pgState.Generation = dbLocalState.Generation

	pgState.ListenAddress = p.pgAdvertiseAddress
	pgState.Port = p.pgAdvertisePort

	initialized, err := p.pgm.IsInitialized()
	if err != nil {
		return pgState, err
	}
	if initialized {
		pgParameters, err := p.pgm.GetConfigFilePGParameters()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("cannot get configured pg parameters")
			return pgState, nil
		}
		filteredPGParameters := common.Parameters{}
		for k, v := range pgParameters {
			if !slices.Contains(managedPGParameters, k) {
				filteredPGParameters[k] = v
			}
		}
		pgNames := make([]string, 0, len(filteredPGParameters))
		for k := range filteredPGParameters {
			pgNames = append(pgNames, k)
		}
		sort.Strings(pgNames)
		p.baseLog().Debug().
			Int("total_parameter_count", len(pgParameters)).
			Int("user_parameter_count", len(filteredPGParameters)).
			Strs("user_parameter_names", pgNames).
			Msg("PostgreSQL parameters from instance config (names only)")
		pgState.PGParameters = filteredPGParameters

		inSyncStandbys, err := p.GetInSyncStandbys()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve current in sync standbys from instance")
			return pgState, nil
		}

		pgState.SynchronousStandbys = inSyncStandbys

		systemData, err := p.pgm.GetSystemData()
		if err != nil {
			p.baseLog().Error().Err(err).Msg("error getting pg state")
			return pgState, nil
		}
		pgState.SystemID = systemData.SystemID
		pgState.TimelineID = systemData.TimelineID
		pgState.XLogPos = systemData.XLogPos

		timelinesHistory, err := getTimeLinesHistory(
			pgState,
			p.pgm,
			maxPostgresTimelinesHistory,
			p.baseLog(),
		)
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("error getting timeline history")
			return pgState, nil
		}
		pgState.TimelinesHistory = timelinesHistory

		olderWalFile, err := p.pgm.OlderWalFile()
		if err != nil {
			p.baseLog().
				Warn().
				Err(err).
				Msg("error getting older wal file")
		} else {
			p.baseLog().Debug().Str("filename", olderWalFile).Msg("older wal file")
			pgState.OlderWalFile = olderWalFile
		}
		pgState.Healthy = true
		role, roleErr := p.pgm.GetRole()
		if roleErr != nil {
			p.baseLog().Debug().Err(roleErr).Msg("failed to get PostgreSQL role for logical slot state publish")
		} else if role == common.RoleMaster {
			logicalSlots, slotsErr := p.pgm.GetLogicalReplicationSlots()
			if slotsErr != nil {
				p.baseLog().Debug().Err(slotsErr).Msg("failed to inspect logical replication slots for state publish")
			} else {
				pgState.ManagedLogicalSlots = logicalSlotLSNMap(logicalSlots)
			}
		}
	}

	return pgState, nil
}

// updateKeeperInfo publishes the keeper info object with last known PG state.
func (p *PostgresKeeper) updateKeeperInfo() error {
	p.localStateMutex.Lock()
	keeperUID := p.keeperLocalState.UID
	clusterUID := p.keeperLocalState.ClusterUID
	p.localStateMutex.Unlock()

	if clusterUID == "" {
		return nil
	}

	major, minor, err := p.binaryVersion()
	if err != nil {
		// In case we fail to parse the binary version, keep reporting 0/0.
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
	}

	keeperInfo := &cluster.KeeperInfo{
		InfoUID:    id.UID(),
		UID:        keeperUID,
		ClusterUID: clusterUID,
		BootUUID:   p.bootUUID,
		PostgresBinaryVersion: cluster.PostgresBinaryVersion{
			Maj: major,
			Min: minor,
		},
		PostgresState: p.getLastPGState(),

		CanBeMaster:             p.canBeMaster,
		CanBeSynchronousReplica: p.canBeSynchronousReplica,
	}
	keeperInfo.Hostname, keeperInfo.NodeName = runtimecommon.ResolveHostNodeMetadata()
	if p.masterPriority != nil {
		keeperInfo.MasterPriority = *p.masterPriority
	}

	// TTL only garbage-collects stale info keys; it is not freshness signal.
	if err := p.e.SetKeeperInfo(context.TODO(), keeperUID, keeperInfo, p.sleepInterval); err != nil {
		return err
	}
	return nil
}

// updatePGState refreshes keeper-cached PostgreSQL state and derived metrics.
func (p *PostgresKeeper) updatePGState(ctx context.Context) {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	pgState, err := p.GetPGState(ctx)
	if err != nil {
		p.baseLog().Error().Err(err).Msg("failed to get pg state")
	}
	p.lastPGState = pgState
	p.updatePGStateMetrics(pgState)
}

// updatePGStateMetrics updates PG health/role/replication gauges from pgState.
func (p *PostgresKeeper) updatePGStateMetrics(pgState *cluster.PostgresState) {
	if pgState == nil {
		pgRunningGauge.Set(0)
		pgInRecoveryGauge.Set(0)
		pgTimelineGauge.Set(0)
		pgStreamingGauge.Set(0)
		pgWALReceiveLSNBytesGauge.Set(0)
		pgWALReplayLSNBytesGauge.Set(0)
		pgReplayLagSecondsGauge.Set(0)
		return
	}

	pgRunningGauge.Set(boolToFloat64(pgState.Healthy))
	pgTimelineGauge.Set(float64(pgState.TimelineID))

	major, minor, err := p.binaryVersion()
	if err == nil {
		pgServerVersionGauge.Set(float64(major*10000 + minor))
	}

	role, roleErr := p.pgm.GetRole()
	if roleErr != nil {
		pgInRecoveryGauge.Set(0)
		pgStreamingGauge.Set(0)
		pgWALReceiveLSNBytesGauge.Set(0)
		pgWALReplayLSNBytesGauge.Set(0)
		pgReplayLagSecondsGauge.Set(0)
		return
	}

	inRecovery := role == common.RoleStandby
	pgInRecoveryGauge.Set(boolToFloat64(inRecovery))

	// Streaming is meaningful for standbys only; infer from configured upstream.
	streaming := inRecovery && pgState.PGParameters["primary_conninfo"] != ""
	pgStreamingGauge.Set(boolToFloat64(streaming))
	if !inRecovery {
		pgWALReceiveLSNBytesGauge.Set(0)
		pgWALReplayLSNBytesGauge.Set(0)
		pgReplayLagSecondsGauge.Set(0)
		return
	}

	standbyStatus, err := p.pgm.GetStandbyStatus()
	if err != nil || standbyStatus == nil {
		pgWALReceiveLSNBytesGauge.Set(0)
		pgWALReplayLSNBytesGauge.Set(0)
		pgReplayLagSecondsGauge.Set(0)
		return
	}
	pgWALReceiveLSNBytesGauge.Set(float64(standbyStatus.ReceiveLSN))
	pgWALReplayLSNBytesGauge.Set(float64(standbyStatus.ReplayLSN))
	pgReplayLagSecondsGauge.Set(standbyStatus.ReplayLagSeconds)
}
