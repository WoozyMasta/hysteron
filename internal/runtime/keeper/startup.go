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
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// loadInitialClusterData gets one startup snapshot to seed runtime config.
func (p *PostgresKeeper) loadInitialClusterData() *cluster.ClusterData {
	cd, _, err := p.e.GetClusterData(context.TODO())
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("error retrieving cluster data")
		return nil
	}
	if cd != nil {
		if cd.FormatVersion != cluster.CurrentCDFormatVersion {
			p.baseLog().
				Error().
				Uint64("version", cd.FormatVersion).
				Msg("unsupported clusterdata format version")
		}
		p.applyRuntimeConfigFromClusterData(cd)
	}
	return cd
}

// logInitialClusterData logs startup cluster snapshot after initial fetch.
func (p *PostgresKeeper) logInitialClusterData(cd *cluster.ClusterData) {
	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data snapshot at keeper start")
}

// setupPostgresManager initializes local manager and validates server version.
func (p *PostgresKeeper) setupPostgresManager() error {
	pgManager := postgresql.NewManager(
		p.pgBinPath,
		p.dataDir,
		p.pgWALDir,
		p.getLocalConnParams(),
		p.getLocalReplConnParams(),
		p.pgSUAuthMethod,
		p.pgSUUsername,
		p.pgSUPassword,
		p.pgReplAuthMethod,
		p.pgReplUsername,
		p.pgReplPassword,
		p.requestTimeout,
	)
	p.pgm = pgManager
	p.pgBinaryVersion = pgManager.BinaryVersion
	return p.validatePostgresVersion()
}

// runStartLoops runs keeper periodic workers until context cancellation.
func (p *PostgresKeeper) runStartLoops(ctx context.Context) {
	endSMCh := make(chan struct{})
	endPgStateCheckerCh := make(chan struct{})
	endUpdateKeeperInfoCh := make(chan struct{})

	smTimerCh := time.NewTimer(0).C
	updatePGStateTimerCh := time.NewTimer(0).C
	updateKeeperInfoTimerCh := time.NewTimer(0).C
	for {
		select {
		case <-ctx.Done():
			p.baseLog().Debug().Msg("shutting down keeper")
			if err := p.pgm.StopIfStarted(true); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
			}
			p.end <- nil
			return

		case <-smTimerCh:
			go func() {
				p.postgresKeeperSM(ctx)
				endSMCh <- struct{}{}
			}()

		case <-endSMCh:
			smTimerCh = time.NewTimer(p.sleepInterval).C

		case <-updatePGStateTimerCh:
			// Update keeper info two times faster than the sleep interval.
			go func() {
				p.updatePGState(ctx)
				endPgStateCheckerCh <- struct{}{}
			}()

		case <-endPgStateCheckerCh:
			// Update keeper info two times faster than the sleep interval.
			updatePGStateTimerCh = time.NewTimer(p.sleepInterval / 2).C

		case <-updateKeeperInfoTimerCh:
			go func() {
				if err := p.updateKeeperInfo(); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to update keeper info")
				}
				endUpdateKeeperInfoCh <- struct{}{}
			}()

		case <-endUpdateKeeperInfoCh:
			updateKeeperInfoTimerCh = time.NewTimer(p.sleepInterval).C
		}
	}
}
