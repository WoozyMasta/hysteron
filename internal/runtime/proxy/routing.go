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

package proxy

import (
	"net"
	"sort"
	"strings"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	slog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/utils/readonly"
)

// setWritableDestination applies writable backend destination and updates
// writable routing metrics/state transitions.
func (c *ClusterChecker) setWritableDestination(addr *net.TCPAddr) {
	if c.writable != nil {
		next := ""
		if addr != nil {
			next = addr.String()
		}
		if c.lastWritableDestination != next {
			backendSwitchesTotal.WithLabelValues(string(proxyModeWritable)).Inc()
			c.lastWritableDestination = next
		}
		if addr == nil {
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "enabled").Set(0)
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "disabled").Set(1)
		} else {
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "enabled").Set(1)
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "disabled").Set(0)
		}
		c.writable.setDestination(addr)
	}
}

// setReadOnlyDestinations applies read-only destination pool and updates
// read-only routing metrics/state transitions.
func (c *ClusterChecker) setReadOnlyDestinations(addrs []*net.TCPAddr) {
	if c.readOnly != nil {
		keys := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			if addr != nil {
				keys = append(keys, addr.String())
			}
		}
		sort.Strings(keys)
		next := strings.Join(keys, ",")
		if c.lastReadOnlyDestinations != next {
			backendSwitchesTotal.WithLabelValues(string(proxyModeReadOnly)).Inc()
			c.lastReadOnlyDestinations = next
		}
		readOnlyDestinationsGauge.Set(float64(len(keys)))
		if len(keys) == 0 {
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "enabled").Set(0)
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "disabled").Set(1)
		} else {
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "enabled").Set(1)
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "disabled").Set(0)
		}
		c.readOnly.setDestinations(addrs)
	}
}

// readOnlyDestinations builds the effective read-only destination pool for the
// current cluster state and read-only policy.
func (c *ClusterChecker) readOnlyDestinations(cd *cluster.ClusterData, primary *cluster.DB) []*net.TCPAddr {
	if c.readOnly == nil || primary == nil {
		return nil
	}

	syncStandbys, asyncStandbys := c.readOnlyStandbyCandidates(cd, primary)
	selected := readonly.SelectPriority(c.readOnlyOptions.ReplicaPriority, syncStandbys, asyncStandbys)
	if c.readOnlyOptions.IncludePrimary {
		if primaryDest, ok := readOnlyDestinationFromDB(primary, 0); ok {
			selected = append(selected, primaryDest)
			log.Debug().
				Str(slog.FieldDBUID, primary.UID).
				Msg("including primary in read-only proxy destination pool")
		}
	}
	if len(selected) == 0 && !c.readOnlyOptions.NoFallback {
		if primaryDest, ok := readOnlyDestinationFromDB(primary, 0); ok {
			log.Info().
				Str(slog.FieldDBUID, primary.UID).
				Uint64("max_lag", uint64(c.readOnlyOptions.MaxLag)).
				Msg("read-only proxy falling back to primary")
			readOnlyFallbacksTotal.Inc()
			selected = append(selected, primaryDest)
		}
	}

	addrs := make([]*net.TCPAddr, 0, len(selected))
	for _, dest := range selected {
		addrs = append(addrs, dest.addr)
	}
	return addrs
}

// readOnlyStandbyCandidates classifies eligible standby backends into
// synchronous and asynchronous candidate groups.
func (c *ClusterChecker) readOnlyStandbyCandidates(cd *cluster.ClusterData, primary *cluster.DB) ([]proxyDestination, []proxyDestination) {
	syncStandbySet := map[string]struct{}{}
	for _, dbUID := range primary.Status.SynchronousStandbys {
		syncStandbySet[dbUID] = struct{}{}
	}

	dbUIDs := make([]string, 0, len(cd.DBs))
	for dbUID := range cd.DBs {
		dbUIDs = append(dbUIDs, dbUID)
	}
	sort.Strings(dbUIDs)

	var syncStandbys []proxyDestination
	var asyncStandbys []proxyDestination
	for _, dbUID := range dbUIDs {
		db := cd.DBs[dbUID]
		if db == nil || db.UID == primary.UID || db.Spec == nil {
			continue
		}
		if db.Spec.Role != common.RoleStandby || !readonly.DBStatusEligible(db) {
			continue
		}

		lag := readonly.XLogLag(primary.Status.XLogPos, db.Status.XLogPos)
		if lag > uint64(c.readOnlyOptions.MaxLag) {
			continue
		}
		dest, ok := readOnlyDestinationFromDB(db, lag)
		if !ok {
			continue
		}
		if _, ok := syncStandbySet[db.UID]; ok {
			syncStandbys = append(syncStandbys, dest)
		} else {
			asyncStandbys = append(asyncStandbys, dest)
		}
	}
	return syncStandbys, asyncStandbys
}

// readOnlyDestinationFromDB resolves one DB status into a proxy destination.
func readOnlyDestinationFromDB(db *cluster.DB, lag uint64) (proxyDestination, bool) {
	addr, err := net.ResolveTCPAddr(
		"tcp",
		net.JoinHostPort(db.Status.ListenAddress, db.Status.Port),
	)
	if err != nil {
		log.Error().
			Err(err).
			Str(slog.FieldDBUID, db.UID).
			Msg("cannot resolve read-only db address")
		return proxyDestination{}, false
	}
	return proxyDestination{
		dbUID: db.UID,
		addr:  addr,
		lag:   lag,
	}, true
}
