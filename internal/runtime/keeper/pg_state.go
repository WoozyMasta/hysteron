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
	"strconv"

	"github.com/rs/zerolog"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// getTimeLinesHistory loads timeline history from PostgreSQL and returns only
// the latest entries up to maxPostgresTimelinesHistory.
func getTimeLinesHistory(
	pgState *cluster.PostgresState,
	pgManager postgresql.PGManager,
	maxPostgresTimelinesHistory int,
	logger *zerolog.Logger,
) (cluster.PostgresTimelinesHistory, error) {
	timelinesHistory := cluster.PostgresTimelinesHistory{}
	// if timeline <= 1 then no timeline history file exists.
	if pgState.TimelineID > 1 {
		var timelineEntries []*postgresql.TimelineHistory
		timelineEntries, err := pgManager.GetTimelinesHistory(pgState.TimelineID)
		if err != nil {
			logger.Error().
				Err(err).
				Uint64("timeline_id", pgState.TimelineID).
				Msg("could not read timeline history from PostgreSQL")
			return timelinesHistory, err
		}
		if len(timelineEntries) > maxPostgresTimelinesHistory {
			timelineEntries = timelineEntries[len(timelineEntries)-maxPostgresTimelinesHistory:]
		}
		for _, entry := range timelineEntries {
			historyEntry := &cluster.PostgresTimelineHistory{
				TimelineID:  entry.TimelineID,
				SwitchPoint: entry.SwitchPoint,
				Reason:      entry.Reason,
			}
			timelinesHistory = append(timelinesHistory, historyEntry)
		}
	}

	return timelinesHistory, nil
}

// getLastPGState returns a deep-copied snapshot of the last published
// PostgreSQL state.
func (p *PostgresKeeper) getLastPGState() *cluster.PostgresState {
	p.pgStateMutex.Lock()
	pgState := p.lastPGState.DeepCopy()
	p.pgStateMutex.Unlock()
	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryPostgresState(pgState)).
		Msg("PostgreSQL state snapshot from last publish")
	return pgState
}

// currentPGParameterInt reads an integer PG parameter from last known state.
func (p *PostgresKeeper) currentPGParameterInt(name string) (int, bool) {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	if p.lastPGState == nil || p.lastPGState.PGParameters == nil {
		return 0, false
	}

	raw, ok := p.lastPGState.PGParameters[name]
	if !ok || raw == "" {
		return 0, false
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}

	return n, true
}
