// Copyright 2016 Sorint.lab
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

package cluster

import "time"

// ClusterData document contracts and typed collections.

// Keepers maps keeper UID to keeper object.
type Keepers map[string]*Keeper

// DBs maps db UID to db object.
type DBs map[string]*DB

// ClusterData stores the complete cluster-data document.
//
// For simplicity all component changes are kept atomic through a unique key.
type ClusterData struct { //nolint:revive
	// ChangeTime is cluster-data last change time.
	ChangeTime time.Time `json:"changeTime"`
	// Cluster is cluster-wide desired and observed state.
	Cluster *Cluster `json:"cluster"`
	// Keepers maps keeper UID to keeper state.
	Keepers Keepers `json:"keepers"`
	// DBs maps DB UID to database state.
	DBs DBs `json:"dbs"`
	// Proxy is the proxy desired/observed state.
	Proxy *Proxy `json:"proxy"`
	// ClusterData format version. Used to detect incompatible
	// version and do upgrade. Needs to be bumped when a non
	// backward compatible change is done to the other struct
	// members.
	FormatVersion uint64 `json:"formatVersion"`
}

// NewClusterData creates an initial cluster-data document.
func NewClusterData(c *Cluster) *ClusterData {
	return &ClusterData{
		FormatVersion: CurrentCDFormatVersion,
		Cluster:       c,
		Keepers:       make(Keepers),
		DBs:           make(DBs),
		Proxy:         &Proxy{},
	}
}

// FindDB returns the db assigned to keeper, if any.
func (c *ClusterData) FindDB(keeper *Keeper) *DB {
	for _, db := range c.DBs {
		if db.Spec.KeeperUID == keeper.UID {
			return db
		}
	}
	return nil
}
