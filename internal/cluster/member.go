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

package cluster

import (
	"maps"
	"slices"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
)

// KeepersInfo maps keeper UID to published keeper info.
type KeepersInfo map[string]*KeeperInfo

// DeepCopy returns an independent copy of keeper infos.
func (k KeepersInfo) DeepCopy() KeepersInfo {
	if k == nil {
		return nil
	}
	nk := make(KeepersInfo, len(k))
	for uid, info := range k {
		nk[uid] = info.DeepCopy()
	}
	return nk
}

// KeeperInfo is the state published by one keeper.
type KeeperInfo struct {
	// PostgresState is the currently observed PostgreSQL state.
	PostgresState *PostgresState `json:"postgresState,omitempty"`
	// CanBeMaster advertises whether this keeper can become master.
	CanBeMaster *bool `json:"canBeMaster,omitempty"`
	// CanBeSynchronousReplica advertises sync-standby eligibility.
	CanBeSynchronousReplica *bool `json:"canBeSynchronousReplica,omitempty"`
	// An unique id for this info, used to know when this the keeper info
	// has been updated
	InfoUID string `json:"infoUID,omitempty"`
	// UID is the keeper UID.
	UID string `json:"uid,omitempty"`
	// ClusterUID is the cluster UID this keeper belongs to.
	ClusterUID string `json:"clusterUID,omitempty"`
	// BootUUID identifies the current keeper process boot.
	BootUUID string `json:"bootUUID,omitempty"`
	// PostgresBinaryVersion is the PostgreSQL binary version detected by keeper.
	PostgresBinaryVersion PostgresBinaryVersion `json:"postgresBinaryVersion,omitzero"`
}

// DeepCopy returns an independent copy of keeper info.
func (k *KeeperInfo) DeepCopy() *KeeperInfo {
	if k == nil {
		return nil
	}
	nk := *k
	nk.PostgresState = k.PostgresState.DeepCopy()
	if k.CanBeMaster != nil {
		v := *k.CanBeMaster
		nk.CanBeMaster = &v
	}
	if k.CanBeSynchronousReplica != nil {
		v := *k.CanBeSynchronousReplica
		nk.CanBeSynchronousReplica = &v
	}
	return &nk
}

// PostgresTimelinesHistory is a list of PostgreSQL timeline history entries.
type PostgresTimelinesHistory []*PostgresTimelineHistory

// PostgresTimelineHistory is one PostgreSQL timeline history entry.
type PostgresTimelineHistory struct {
	// Reason is the timeline switch reason.
	Reason string `json:"reason,omitempty"`
	// TimelineID is the new timeline identifier.
	TimelineID uint64 `json:"timelineID,omitempty"`
	// SwitchPoint is the LSN where the switch happened.
	SwitchPoint uint64 `json:"switchPoint,omitempty"`
}

// GetTimelineHistory returns the entry for id, if present.
func (tlsh PostgresTimelinesHistory) GetTimelineHistory(id uint64) *PostgresTimelineHistory {
	for _, tlh := range tlsh {
		if tlh.TimelineID == id {
			return tlh
		}
	}
	return nil
}

// PostgresState is the PostgreSQL state observed by a keeper.
type PostgresState struct {
	// PGParameters are current PostgreSQL runtime parameters.
	PGParameters common.Parameters `json:"pgParameters,omitempty"`
	// ManagedLogicalSlots stores confirmed_flush_lsn values for managed logical
	// replication slots observed on this instance.
	ManagedLogicalSlots map[string]uint64 `json:"managedLogicalSlots,omitempty"`
	// UID is the DB UID.
	UID string `json:"uid,omitempty"`
	// ListenAddress is PostgreSQL listen address.
	ListenAddress string `json:"listenAddress,omitempty"`
	// Port is PostgreSQL listen port.
	Port string `json:"port,omitempty"`
	// SystemID is PostgreSQL system identifier.
	SystemID string `json:"systemID,omitempty"`
	// OlderWalFile is the oldest required WAL segment filename.
	OlderWalFile string `json:"olderWalFile,omitempty"`
	// TimelinesHistory is known timeline history entries.
	TimelinesHistory PostgresTimelinesHistory `json:"timelinesHistory,omitempty"`
	// SynchronousStandbys are standbys currently configured as synchronous.
	SynchronousStandbys []string `json:"synchronousStandbys"`
	// Generation is desired/assigned DB generation.
	Generation int64 `json:"generation,omitempty"`
	// TimelineID is current timeline identifier.
	TimelineID uint64 `json:"timelineID,omitempty"`
	// XLogPos is current WAL position.
	XLogPos uint64 `json:"xLogPos,omitempty"`
	// Healthy reports PostgreSQL health.
	Healthy bool `json:"healthy,omitempty"`
}

// DeepCopy returns an independent copy of PostgreSQL state.
func (p *PostgresState) DeepCopy() *PostgresState {
	if p == nil {
		return nil
	}
	np := *p
	np.PGParameters = maps.Clone(p.PGParameters)
	np.SynchronousStandbys = slices.Clone(p.SynchronousStandbys)
	np.ManagedLogicalSlots = maps.Clone(p.ManagedLogicalSlots)
	if p.TimelinesHistory != nil {
		np.TimelinesHistory = make(PostgresTimelinesHistory, len(p.TimelinesHistory))
		for i, h := range p.TimelinesHistory {
			if h == nil {
				continue
			}
			hCopy := *h
			np.TimelinesHistory[i] = &hCopy
		}
	}
	return &np
}

// SentinelsInfo is a sortable list of sentinel info records.
type SentinelsInfo []*SentinelInfo

func (s SentinelsInfo) Len() int           { return len(s) }
func (s SentinelsInfo) Less(i, j int) bool { return s[i].UID < s[j].UID }
func (s SentinelsInfo) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SentinelInfo is the state published by one sentinel.
type SentinelInfo struct {
	UID string
}

// ProxyInfo is the state published by one proxy.
type ProxyInfo struct {
	// An unique id for this info, used to know when the proxy info
	// has been updated
	InfoUID string `json:"infoUID,omitempty"`

	// UID is the proxy UID.
	UID string
	// Generation is the proxy generation.
	Generation int64

	// ProxyTimeout is the current proxyTimeout used by the proxy
	// at the time of publishing its state.
	// It's used by the sentinel to know for how much time the
	// proxy should be considered active.
	// ProxyTimeout is timeout used by sentinel to consider the proxy active.
	ProxyTimeout time.Duration
	// Listeners describes proxy listeners and their runtime state.
	Listeners []ProxyListenerInfo `json:"listeners,omitempty"`
}

// ProxyListenerInfo describes one proxy listener endpoint and mode.
type ProxyListenerInfo struct {
	Mode    string `json:"mode,omitempty"`
	Address string `json:"address,omitempty"`
	Port    string `json:"port,omitempty"`
	Active  bool   `json:"active,omitempty"`
}

// ProxiesInfo maps proxy UID to published proxy info.
type ProxiesInfo map[string]*ProxyInfo

// DeepCopy returns an independent copy of proxy infos.
func (p ProxiesInfo) DeepCopy() ProxiesInfo {
	if p == nil {
		return nil
	}
	np := make(ProxiesInfo, len(p))
	for uid, info := range p {
		if info == nil {
			np[uid] = nil
			continue
		}
		infoCopy := *info
		if info.Listeners != nil {
			infoCopy.Listeners = slices.Clone(info.Listeners)
		}
		np[uid] = &infoCopy
	}
	return np
}

// ToSlice returns proxy infos as a sortable slice.
func (p ProxiesInfo) ToSlice() ProxiesInfoSlice {
	pis := make(ProxiesInfoSlice, 0, len(p))
	for _, pi := range p {
		pis = append(pis, pi)
	}
	return pis
}

// ProxiesInfoSlice is a sortable list of proxy info records.
type ProxiesInfoSlice []*ProxyInfo

func (p ProxiesInfoSlice) Len() int           { return len(p) }
func (p ProxiesInfoSlice) Less(i, j int) bool { return p[i].UID < p[j].UID }
func (p ProxiesInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
