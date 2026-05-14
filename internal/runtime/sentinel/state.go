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
	"reflect"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
)

// updateKeepersStatus merges observed keeper/db state into cluster data and
// returns updated keeper history snapshot for the current loop.
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
				} else if kih.Seen && time.Since(kih.Timer) > s.sleepInterval {
					// Remove since it wasn't updated
					delete(tmpKeepersInfo, ki.UID)
				}
			}

			if kih.KeeperInfo.InfoUID != ki.InfoUID {
				kihs[keeperUID] = &KeeperInfoHistory{
					KeeperInfo: ki,
					Seen:       true,
					Timer:      time.Now(),
				}
			}
		} else {
			kihs[keeperUID] = &KeeperInfoHistory{KeeperInfo: ki, Seen: true, Timer: time.Now()}
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
	forceFailedKeeperUIDs := make(map[string]struct{})
	for _, k := range cd.Keepers {
		healthy := s.isKeeperHealthy(cd, k)
		if k.Status.ForceFail {
			healthy = false
			forceFailedKeeperUIDs[k.UID] = struct{}{}
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
	s.forceFailedKeeperUIDs = forceFailedKeeperUIDs

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
			if s.dbIncreasingXLogPosObservedAt == nil {
				s.dbIncreasingXLogPosObservedAt = make(map[string]time.Time)
			}
			s.dbIncreasingXLogPosObservedAt[db.UID] = time.Now()
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
			db.Status.ManagedLogicalSlots = dbs.ManagedLogicalSlots

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

// activeProxiesInfos returns proxies considered active based on history and
// proxy timeout, while keeping unknown-yet proxies temporarily active.
func (s *Sentinel) activeProxiesInfos(proxiesInfo cluster.ProxiesInfo) (cluster.ProxiesInfo, error) {
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
				if time.Since(pih.Timer) > 2*pi.ProxyTimeout {
					delete(activeProxiesInfo, pi.UID)
				}
			} else {
				pihs[pi.UID] = &ProxyInfoHistory{ProxyInfo: pi, Timer: time.Now()}
			}
		} else {
			// add proxyInfo if not in the history
			pihs[pi.UID] = &ProxyInfoHistory{ProxyInfo: pi, Timer: time.Now()}
		}
	}

	s.proxyInfoHistories = pihs

	return activeProxiesInfo, nil
}

// updateChangeTimes recalculates entity change timestamps for updated cluster
// data before persisting it.
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
