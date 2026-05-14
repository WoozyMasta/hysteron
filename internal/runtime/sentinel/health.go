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
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
)

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

// SetKeeperError marks a keeper as having a recent error.
func (s *Sentinel) SetKeeperError(uid string) {
	if _, ok := s.keeperErrorTimers[uid]; !ok {
		s.keeperErrorTimers[uid] = time.Now()
	}
}

// CleanKeeperError clears the recent error marker for a keeper.
func (s *Sentinel) CleanKeeperError(uid string) {
	delete(s.keeperErrorTimers, uid)
}

// SetDBError marks a database as having a recent error.
func (s *Sentinel) SetDBError(uid string) {
	if _, ok := s.dbErrorTimers[uid]; !ok {
		s.dbErrorTimers[uid] = time.Now()
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

func (s *Sentinel) isKeeperHealthy(cd *cluster.ClusterData, keeper *cluster.Keeper) bool {
	t, ok := s.keeperErrorTimers[keeper.UID]
	if !ok {
		return true
	}

	if time.Since(t) > cd.Cluster.DefSpec().FailInterval.Duration {
		return false
	}

	return true
}

func (s *Sentinel) isDBHealthy(cd *cluster.ClusterData, db *cluster.DB) bool {
	t, ok := s.dbErrorTimers[db.UID]
	if !ok {
		return true
	}

	if time.Since(t) > cd.Cluster.DefSpec().FailInterval.Duration {
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
			Timer:      time.Now(),
		}

		d, ok := s.dbConvergenceInfos[db.UID]
		if !ok {
			s.dbConvergenceInfos[db.UID] = nd
		} else if d.Generation != db.Generation {
			s.dbConvergenceInfos[db.UID] = nd
		}
	}
}

func (s *Sentinel) dbConvergenceState(db *cluster.DB, timeout time.Duration) ConvergenceState {
	if db.Status.CurrentGeneration == db.Generation {
		return Converged
	}

	if timeout != 0 {
		d, ok := s.dbConvergenceInfos[db.UID]
		if !ok {
			d = &DBConvergenceInfo{
				Generation: db.Generation,
				Timer:      time.Now(),
			}
			s.dbConvergenceInfos[db.UID] = d
			s.log.Debug().
				Str(slog.FieldDBUID, db.UID).
				Msg("database convergence tracking initialized")
		}

		if time.Since(d.Timer) > timeout {
			return ConvergenceFailed
		}
	}

	return Converging
}
