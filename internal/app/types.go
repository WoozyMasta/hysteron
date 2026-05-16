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

import runtimex "github.com/woozymasta/hysteron/internal/runtime"

// RuntimeTarget identifies a runtime component and selected backend.
type RuntimeTarget = runtimex.Target

// Status is the `hysteron cluster status` output model.
type Status struct {
	Sentinels  []SentinelStatus `json:"sentinels"`
	Proxies    []ProxyStatus    `json:"proxies"`
	Keepers    []KeeperStatus   `json:"keepers"`
	KeeperTree []KeeperTreeNode `json:"-"` // KeeperTree stores hierarchical keeper nodes for text output rendering.
	Cluster    ClusterStatus    `json:"cluster"`
}

// KeeperTreeNode is one node in keeper hierarchy output.
type KeeperTreeNode struct {
	Label string // Label is the user-facing node text.
	Level int    // Level is zero-based nesting depth.
}

// SentinelStatus is the status output for one sentinel.
type SentinelStatus struct {
	UID      string `json:"uid"`
	Hostname string `json:"hostname,omitempty"`
	NodeName string `json:"node_name,omitempty"`
	Leader   bool   `json:"leader"`
}

// ProxyStatus is the status output for one proxy.
type ProxyStatus struct {
	UID        string `json:"uid"`
	Hostname   string `json:"hostname,omitempty"`
	NodeName   string `json:"node_name,omitempty"`
	Mode       string `json:"mode"`
	Listeners  string `json:"listeners"`
	Generation int64  `json:"generation"`
}

// KeeperStatus is the status output for one keeper.
type KeeperStatus struct {
	UID                 string `json:"uid"`
	ListenAddress       string `json:"listen_address"`
	DBUID               string `json:"db_uid"`
	Role                string `json:"role"`
	SyncRole            string `json:"sync_role"`
	PGVersion           string `json:"pg_version"`
	Hostname            string `json:"hostname,omitempty"`
	NodeName            string `json:"node_name,omitempty"`
	MasterPriority      int    `json:"master_priority"`
	PgWantedGeneration  int64  `json:"pg_wanted_generation"`
	PgCurrentGeneration int64  `json:"pg_current_generation"`
	Healthy             bool   `json:"healthy"`
	PgHealthy           bool   `json:"pg_healthy"`
	CanBeMaster         bool   `json:"can_be_master"`
	CanBeSyncReplica    bool   `json:"can_be_sync_replica"`
}

// ClusterStatus is the status output for the cluster summary.
type ClusterStatus struct {
	MasterKeeperUID string `json:"master_keeper_uid"`
	MasterDBUID     string `json:"master_db_uid"`
	Phase           string `json:"phase"`
	PauseReason     string `json:"pause_reason,omitempty"`
	PauseUntil      string `json:"pause_until,omitempty"`
	Generation      int64  `json:"generation"`
	FormatVersion   uint64 `json:"format_version"`
	KeepersTotal    int    `json:"keepers_total"`
	KeepersHealthy  int    `json:"keepers_healthy"`
	DBsTotal        int    `json:"dbs_total"`
	DBsHealthy      int    `json:"dbs_healthy"`
	ProxiesSeen     int    `json:"proxies_seen"`
	Paused          bool   `json:"paused"`
	Available       bool   `json:"available"`
}
