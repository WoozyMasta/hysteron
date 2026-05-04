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
	"reflect"
	"time"

	"github.com/sorintlab/stolon/internal/common"

	"github.com/mitchellh/copystructure"
)

// KeepersInfo maps keeper UID to published keeper info.
type KeepersInfo map[string]*KeeperInfo

// DeepCopy returns an independent copy of keeper infos.
func (k KeepersInfo) DeepCopy() KeepersInfo {
	if k == nil {
		return nil
	}
	nk, err := copystructure.Copy(k)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(k, nk) {
		panic("not equal")
	}
	return nk.(KeepersInfo)
}

// KeeperInfo is the state published by one keeper.
type KeeperInfo struct {
	// An unique id for this info, used to know when this the keeper info
	// has been updated
	InfoUID string `json:"infoUID,omitempty"`

	UID        string `json:"uid,omitempty"`
	ClusterUID string `json:"clusterUID,omitempty"`
	BootUUID   string `json:"bootUUID,omitempty"`

	PostgresBinaryVersion PostgresBinaryVersion `json:"postgresBinaryVersion,omitzero"`

	PostgresState *PostgresState `json:"postgresState,omitempty"`

	CanBeMaster             *bool `json:"canBeMaster,omitempty"`
	CanBeSynchronousReplica *bool `json:"canBeSynchronousReplica,omitempty"`
}

// DeepCopy returns an independent copy of keeper info.
func (k *KeeperInfo) DeepCopy() *KeeperInfo {
	if k == nil {
		return nil
	}
	nk, err := copystructure.Copy(k)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(k, nk) {
		panic("not equal")
	}
	return nk.(*KeeperInfo)
}

// PostgresTimelinesHistory is a list of PostgreSQL timeline history entries.
type PostgresTimelinesHistory []*PostgresTimelineHistory

// PostgresTimelineHistory is one PostgreSQL timeline history entry.
type PostgresTimelineHistory struct {
	TimelineID  uint64 `json:"timelineID,omitempty"`
	SwitchPoint uint64 `json:"switchPoint,omitempty"`
	Reason      string `json:"reason,omitempty"`
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
	UID        string `json:"uid,omitempty"`
	Generation int64  `json:"generation,omitempty"`

	ListenAddress string `json:"listenAddress,omitempty"`
	Port          string `json:"port,omitempty"`

	Healthy bool `json:"healthy,omitempty"`

	SystemID         string                   `json:"systemID,omitempty"`
	TimelineID       uint64                   `json:"timelineID,omitempty"`
	XLogPos          uint64                   `json:"xLogPos,omitempty"`
	TimelinesHistory PostgresTimelinesHistory `json:"timelinesHistory,omitempty"`

	PGParameters        common.Parameters `json:"pgParameters,omitempty"`
	SynchronousStandbys []string          `json:"synchronousStandbys"`
	OlderWalFile        string            `json:"olderWalFile,omitempty"`
}

// DeepCopy returns an independent copy of PostgreSQL state.
func (p *PostgresState) DeepCopy() *PostgresState {
	if p == nil {
		return nil
	}
	np, err := copystructure.Copy(p)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(p, np) {
		panic("not equal")
	}
	return np.(*PostgresState)
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

	UID        string
	Generation int64

	// ProxyTimeout is the current proxyTimeout used by the proxy
	// at the time of publishing its state.
	// It's used by the sentinel to know for how much time the
	// proxy should be considered active.
	ProxyTimeout time.Duration
}

// ProxiesInfo maps proxy UID to published proxy info.
type ProxiesInfo map[string]*ProxyInfo

// DeepCopy returns an independent copy of proxy infos.
func (p ProxiesInfo) DeepCopy() ProxiesInfo {
	if p == nil {
		return nil
	}
	np, err := copystructure.Copy(p)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(p, np) {
		panic("not equal")
	}
	return np.(ProxiesInfo)
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
