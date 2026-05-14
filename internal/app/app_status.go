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

package app

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/woozymasta/hysteron/internal/cluster"
)

const (
	proxyListenerModeWritable = "writable"
	proxyListenerModeReadOnly = "read-only"
)

// proxyStatusMode maps listener modes to a concise user-facing proxy mode.
func proxyStatusMode(listeners []cluster.ProxyListenerInfo) string {
	hasWritable := false
	hasReadOnly := false

	for _, listener := range listeners {
		if !listener.Active {
			continue
		}
		switch listener.Mode {
		case proxyListenerModeWritable:
			hasWritable = true
		case proxyListenerModeReadOnly:
			hasReadOnly = true
		}
	}

	switch {
	case hasWritable && hasReadOnly:
		return "write+read"
	case hasWritable:
		return "write"
	case hasReadOnly:
		return "read"
	default:
		return "-"
	}
}

// proxyStatusListeners renders listener addresses and readiness for status output.
func proxyStatusListeners(listeners []cluster.ProxyListenerInfo) string {
	if len(listeners) == 0 {
		return "-"
	}

	items := make([]string, 0, len(listeners))
	for _, listener := range listeners {
		if strings.TrimSpace(listener.Address) == "" || strings.TrimSpace(listener.Port) == "" {
			continue
		}
		mode := listener.Mode
		if mode == "" {
			mode = "unknown"
		}
		state := "down"
		if listener.Active {
			state = "up"
		}
		items = append(items, fmt.Sprintf("%s=%s:%s(%s)", mode, listener.Address, listener.Port, state))
	}
	if len(items) == 0 {
		return "-"
	}
	slices.Sort(items)
	return strings.Join(items, ", ")
}

// makeKeeperStatus builds keeper rows from current cluster data.
func makeKeeperStatus(cd *cluster.ClusterData) []KeeperStatus {
	if cd == nil || cd.Keepers == nil {
		return nil
	}

	keys := cd.Keepers.SortedKeys()
	status := make([]KeeperStatus, 0, len(keys))
	for _, keeperUID := range keys {
		keeper := cd.Keepers[keeperUID]
		if keeper == nil {
			continue
		}

		keeperStatus := KeeperStatus{
			UID:           keeperUID,
			Healthy:       keeper.Status.Healthy,
			DBUID:         "-",
			Role:          "-",
			PGVersion:     "-",
			ListenAddress: "(no db assigned)",
		}
		if keeper.Status.PostgresBinaryVersion.Maj > 0 {
			if keeper.Status.PostgresBinaryVersion.Min > 0 {
				keeperStatus.PGVersion = strconv.Itoa(keeper.Status.PostgresBinaryVersion.Maj) +
					"." + strconv.Itoa(keeper.Status.PostgresBinaryVersion.Min)
			} else {
				keeperStatus.PGVersion = strconv.Itoa(keeper.Status.PostgresBinaryVersion.Maj)
			}
		}
		if keeper.Status.CanBeMaster != nil {
			keeperStatus.CanBeMaster = *keeper.Status.CanBeMaster
		}
		if keeper.Status.CanBeSynchronousReplica != nil {
			keeperStatus.CanBeSyncReplica = *keeper.Status.CanBeSynchronousReplica
		}

		db := cd.FindDB(keeper)
		if db != nil {
			keeperStatus.DBUID = db.UID
			keeperStatus.Role = string(db.Spec.Role)
			keeperStatus.PgHealthy = db.Status.Healthy
			keeperStatus.PgWantedGeneration = db.Generation
			keeperStatus.PgCurrentGeneration = db.Status.CurrentGeneration

			keeperStatus.ListenAddress = "(unknown)"
			if db.Status.ListenAddress != "" {
				keeperStatus.ListenAddress = fmt.Sprintf("%s:%s", db.Status.ListenAddress, db.Status.Port)
			}
		}

		status = append(status, keeperStatus)
	}

	return status
}

// makeClusterStatus summarizes cluster-level state for status output.
func makeClusterStatus(cd *cluster.ClusterData) ClusterStatus {
	clusterStatus := ClusterStatus{}
	if cd == nil || cd.Cluster == nil || cd.DBs == nil {
		clusterStatus.Available = false
		return clusterStatus
	}

	clusterStatus.Available = true
	clusterStatus.Phase = string(cd.Cluster.Status.Phase)
	clusterStatus.Generation = cd.Cluster.Generation
	clusterStatus.FormatVersion = cd.FormatVersion
	clusterStatus.KeepersTotal = len(cd.Keepers)
	clusterStatus.DBsTotal = len(cd.DBs)

	for _, keeper := range cd.Keepers {
		if keeper != nil && keeper.Status.Healthy {
			clusterStatus.KeepersHealthy++
		}
	}
	for _, db := range cd.DBs {
		if db != nil && db.Status.Healthy {
			clusterStatus.DBsHealthy++
		}
	}

	masterDBUID := cd.Cluster.Status.Master
	if masterDBUID == "" {
		return clusterStatus
	}
	masterDB := cd.DBs[masterDBUID]
	if masterDB == nil {
		return clusterStatus
	}
	clusterStatus.MasterDBUID = masterDB.UID
	if keeper := cd.Keepers[masterDB.Spec.KeeperUID]; keeper != nil {
		clusterStatus.MasterKeeperUID = keeper.UID
	}
	return clusterStatus
}

// keeperTreeLines returns structured keeper tree nodes from master/follower topology.
func keeperTreeLines(masterDBUID string, cd *cluster.ClusterData) []KeeperTreeNode {
	if masterDBUID == "" || cd == nil || cd.DBs == nil {
		return nil
	}
	var nodes []KeeperTreeNode
	appendTreeNode(&nodes, masterDBUID, cd, 0)
	return nodes
}

// appendTreeNode appends one DB subtree rooted at dbUID to keeper tree nodes.
func appendTreeNode(nodes *[]KeeperTreeNode, dbUID string, cd *cluster.ClusterData, level int) {
	db := cd.DBs[dbUID]
	if db == nil {
		return
	}
	label := db.Spec.KeeperUID
	if cd.Cluster != nil && dbUID == cd.Cluster.Status.Master {
		label += " (master)"
	}
	*nodes = append(*nodes, KeeperTreeNode{Label: label, Level: level})

	for _, followerUID := range db.Spec.Followers {
		appendTreeNode(nodes, followerUID, cd, level+1)
	}
}
