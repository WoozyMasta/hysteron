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

// Package v0 contains legacy cluster-data v0 contracts.
package v0

import (
	"fmt"
	"reflect"
	"sort"
	"time"
)

const (
	// CurrentCDFormatVersion is the legacy cluster-data format version.
	CurrentCDFormatVersion uint64 = 0
)

// KeepersState maps keeper ID to legacy keeper state.
type KeepersState map[string]*KeeperState

// SortedKeys returns sorted keeper IDs.
func (kss KeepersState) SortedKeys() []string {
	keys := make([]string, 0, len(kss))
	for k := range kss {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Copy returns an independent copy of keeper states.
func (kss KeepersState) Copy() KeepersState {
	nkss := KeepersState{}
	for k, v := range kss {
		nkss[k] = v.Copy()
	}
	return nkss
}

// NewFromKeeperInfo adds keeper state from keeper info.
func (kss KeepersState) NewFromKeeperInfo(ki *KeeperInfo) error {
	id := ki.ID
	if _, ok := kss[id]; ok {
		return fmt.Errorf("keeperState with id %q already exists", id)
	}
	kss[id] = &KeeperState{
		ErrorStartTime:     time.Time{},
		ID:                 ki.ID,
		ClusterViewVersion: ki.ClusterViewVersion,
		ListenAddress:      ki.ListenAddress,
		Port:               ki.Port,
		PGListenAddress:    ki.PGListenAddress,
		PGPort:             ki.PGPort,
	}
	return nil
}

// KeeperState is the legacy keeper state stored in cluster data.
type KeeperState struct {
	// ErrorStartTime is first time the keeper entered error state.
	ErrorStartTime time.Time
	// PGState is observed PostgreSQL state for this keeper.
	PGState *PostgresState
	// ID is the keeper identifier.
	ID string
	// ListenAddress is keeper API/listener address.
	ListenAddress string
	// Port is keeper API/listener port.
	Port string
	// PGListenAddress is PostgreSQL listen address.
	PGListenAddress string
	// PGPort is PostgreSQL listen port.
	PGPort string
	// ClusterViewVersion is last cluster view version acknowledged by keeper.
	ClusterViewVersion int
	// Healthy reports keeper health status.
	Healthy bool
}

// Copy returns an independent copy of keeper state.
func (ks *KeeperState) Copy() *KeeperState {
	if ks == nil {
		return nil
	}
	nks := *ks
	return &nks
}

// ChangedFromKeeperInfo reports whether keeper info differs from state.
func (ks *KeeperState) ChangedFromKeeperInfo(ki *KeeperInfo) (bool, error) {
	if ks.ID != ki.ID {
		return false, fmt.Errorf("different IDs, keeperState.ID: %s != keeperInfo.ID: %s", ks.ID, ki.ID)
	}
	if ks.ClusterViewVersion != ki.ClusterViewVersion ||
		ks.ListenAddress != ki.ListenAddress ||
		ks.Port != ki.Port ||
		ks.PGListenAddress != ki.PGListenAddress ||
		ks.PGPort != ki.PGPort {
		return true, nil
	}
	return false, nil
}

// UpdateFromKeeperInfo updates keeper state from keeper info.
func (ks *KeeperState) UpdateFromKeeperInfo(ki *KeeperInfo) error {
	if ks.ID != ki.ID {
		return fmt.Errorf("different IDs, keeperState.ID: %s != keeperInfo.ID: %s", ks.ID, ki.ID)
	}
	ks.ClusterViewVersion = ki.ClusterViewVersion
	ks.ListenAddress = ki.ListenAddress
	ks.Port = ki.Port
	ks.PGListenAddress = ki.PGListenAddress
	ks.PGPort = ki.PGPort

	return nil
}

// SetError records the first keeper error time.
func (ks *KeeperState) SetError() {
	if ks.ErrorStartTime.IsZero() {
		ks.ErrorStartTime = time.Now()
	}
}

// CleanError clears the keeper error time.
func (ks *KeeperState) CleanError() {
	ks.ErrorStartTime = time.Time{}
}

// KeepersRole maps keeper ID to legacy keeper role.
type KeepersRole map[string]*KeeperRole

// NewKeepersRole creates an empty keepers role map.
func NewKeepersRole() KeepersRole {
	return make(KeepersRole)
}

// Copy returns an independent copy of keepers role.
func (ksr KeepersRole) Copy() KeepersRole {
	nksr := KeepersRole{}
	for k, v := range ksr {
		nksr[k] = v.Copy()
	}
	return nksr
}

// Add adds a keeper role.
func (ksr KeepersRole) Add(id string, follow string) error {
	if _, ok := ksr[id]; ok {
		return fmt.Errorf("keeperRole with id %q already exists", id)
	}
	ksr[id] = &KeeperRole{ID: id, Follow: follow}
	return nil
}

// KeeperRole stores a keeper role and follow target.
type KeeperRole struct {
	ID     string
	Follow string
}

// Copy returns an independent copy of keeper role.
func (kr *KeeperRole) Copy() *KeeperRole {
	if kr == nil {
		return nil
	}
	nkr := *kr
	return &nkr
}

// ProxyConf stores legacy proxy connection settings.
type ProxyConf struct {
	Host string
	Port string
}

// Copy returns an independent copy of proxy config.
func (pc *ProxyConf) Copy() *ProxyConf {
	if pc == nil {
		return nil
	}
	npc := *pc
	return &npc
}

// ClusterView stores the legacy computed cluster view.
type ClusterView struct {
	// ChangeTime is cluster view last change time.
	ChangeTime time.Time
	// KeepersRole maps keepers to their role/follow target.
	KeepersRole KeepersRole
	// ProxyConf is proxy destination configuration.
	ProxyConf *ProxyConf
	// Config is cluster configuration snapshot.
	Config *NilConfig
	// Master is current master keeper ID.
	Master string
	// Version is monotonically increasing cluster view version.
	Version int
}

// NewClusterView return an initialized clusterView with Version: 0, zero
// ChangeTime, no Master and empty KeepersRole.
func NewClusterView() *ClusterView {
	return &ClusterView{
		KeepersRole: NewKeepersRole(),
		Config:      &NilConfig{},
	}
}

// Equals checks if the clusterViews are the same. It ignores the ChangeTime.
func (cv *ClusterView) Equals(ncv *ClusterView) bool {
	if cv == nil {
		return ncv == nil
	}
	return cv.Version == ncv.Version &&
		cv.Master == ncv.Master &&
		reflect.DeepEqual(cv.KeepersRole, ncv.KeepersRole) &&
		reflect.DeepEqual(cv.ProxyConf, ncv.ProxyConf) &&
		reflect.DeepEqual(cv.Config, ncv.Config)
}

// Copy returns an independent copy of the cluster view.
func (cv *ClusterView) Copy() *ClusterView {
	if cv == nil {
		return nil
	}
	ncv := *cv
	ncv.KeepersRole = cv.KeepersRole.Copy()
	ncv.ProxyConf = cv.ProxyConf.Copy()
	ncv.Config = cv.Config.Copy()
	ncv.ChangeTime = cv.ChangeTime
	return &ncv
}

// GetFollowersIDs returns a sorted list of follower IDs.
func (cv *ClusterView) GetFollowersIDs(id string) []string {
	followersIDs := []string{}
	for keeperID, kr := range cv.KeepersRole {
		if kr.Follow == id {
			followersIDs = append(followersIDs, keeperID)
		}
	}
	sort.Strings(followersIDs)
	return followersIDs
}

// ClusterData contains keeper state and cluster view that must stay in sync.
type ClusterData struct {
	KeepersState KeepersState
	ClusterView  *ClusterView
	// ClusterData format version. Used to detect incompatible
	// version and do upgrade. Needs to be bumped when a non
	// backward compatible change is done to the other struct
	// members.
	FormatVersion uint64
}
