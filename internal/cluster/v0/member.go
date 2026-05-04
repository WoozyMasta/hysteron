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

package v0

import "github.com/sorintlab/stolon/internal/common"

// KeepersInfo maps keeper ID to legacy keeper info.
type KeepersInfo map[string]*KeeperInfo

// KeeperInfo is the legacy state published by one keeper.
type KeeperInfo struct {
	ID                 string
	ClusterViewVersion int
	ListenAddress      string
	Port               string
	PGListenAddress    string
	PGPort             string
}

// Copy returns an independent copy of keeper info.
func (k *KeeperInfo) Copy() *KeeperInfo {
	if k == nil {
		return nil
	}
	nk := *k
	return &nk
}

// PostgresTimelinesHistory is a list of legacy PostgreSQL timeline entries.
type PostgresTimelinesHistory []*PostgresTimelineHistory

// Copy returns an independent copy of timeline history.
func (tlsh PostgresTimelinesHistory) Copy() PostgresTimelinesHistory {
	if tlsh == nil {
		return nil
	}
	ntlsh := make(PostgresTimelinesHistory, len(tlsh))
	copy(ntlsh, tlsh)
	return ntlsh
}

// PostgresTimelineHistory is one legacy PostgreSQL timeline history entry.
type PostgresTimelineHistory struct {
	TimelineID  uint64
	SwitchPoint uint64
	Reason      string
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

// PostgresState is the legacy PostgreSQL state observed by a keeper.
type PostgresState struct {
	Initialized      bool
	Role             common.Role
	SystemID         string
	TimelineID       uint64
	XLogPos          uint64
	TimelinesHistory PostgresTimelinesHistory
}

// Copy returns an independent copy of PostgreSQL state.
func (p *PostgresState) Copy() *PostgresState {
	if p == nil {
		return nil
	}
	np := *p
	np.TimelinesHistory = p.TimelinesHistory.Copy()
	return &np
}

// KeepersDiscoveryInfo is a list of legacy keeper discovery records.
type KeepersDiscoveryInfo []*KeeperDiscoveryInfo

// KeeperDiscoveryInfo is one legacy keeper discovery record.
type KeeperDiscoveryInfo struct {
	ListenAddress string
	Port          string
}

// SentinelsInfo is a sortable list of legacy sentinel info records.
type SentinelsInfo []*SentinelInfo

func (s SentinelsInfo) Len() int           { return len(s) }
func (s SentinelsInfo) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s SentinelsInfo) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SentinelInfo is the legacy state published by one sentinel.
type SentinelInfo struct {
	ID            string
	ListenAddress string
	Port          string
}

// ProxiesInfo is a sortable list of legacy proxy info records.
type ProxiesInfo []*ProxyInfo

func (p ProxiesInfo) Len() int           { return len(p) }
func (p ProxiesInfo) Less(i, j int) bool { return p[i].ID < p[j].ID }
func (p ProxiesInfo) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// ProxyInfo is the legacy state published by one proxy.
type ProxyInfo struct {
	ID                 string
	ListenAddress      string
	Port               string
	ClusterViewVersion int
}
