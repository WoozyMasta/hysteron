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

import (
	"sort"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
)

// Runtime entity contract types for keeper/db/proxy state.

// KeeperSpec is the desired keeper configuration.
type KeeperSpec struct{}

// KeeperStatus is the observed keeper status.
type KeeperStatus struct {
	// LastHealthyTime is last time the keeper was considered healthy.
	LastHealthyTime time.Time `json:"lastHealthyTime,omitzero"`
	// CanBeMaster advertises whether this keeper can become master.
	CanBeMaster *bool `json:"canBeMaster,omitempty"`
	// CanBeSynchronousReplica advertises sync-standby eligibility.
	CanBeSynchronousReplica *bool `json:"canBeSynchronousReplica,omitempty"`
	// BootUUID identifies current keeper process boot.
	BootUUID string `json:"bootUUID,omitempty"`
	// Hostname is local OS hostname reported by keeper process.
	Hostname string `json:"hostname,omitempty"`
	// NodeName is optional logical node label (for example kubernetes node name).
	NodeName string `json:"nodeName,omitempty"`
	// PostgresBinaryVersion is PostgreSQL binary version detected by keeper.
	PostgresBinaryVersion PostgresBinaryVersion `json:"postgresBinaryVersion,omitzero"`
	// MasterPriority is keeper priority used as failover tie-break when candidates are otherwise equal.
	MasterPriority int `json:"masterPriority,omitempty"`
	// Healthy reports keeper health.
	Healthy bool `json:"healthy,omitempty"`
	// ForceFail requests sentinel to consider this keeper failed.
	ForceFail bool `json:"forceFail,omitempty"`
}

// Keeper is a keeper object in cluster data.
type Keeper struct {
	ChangeTime time.Time   `json:"changeTime,omitzero"`
	Spec       *KeeperSpec `json:"spec,omitempty"`
	// Keeper ID
	UID        string       `json:"uid,omitempty"`
	Status     KeeperStatus `json:"status,omitzero"`
	Generation int64        `json:"generation,omitempty"`
}

// NewKeeperFromKeeperInfo creates a keeper object from keeper info.
func NewKeeperFromKeeperInfo(ki *KeeperInfo) *Keeper {
	return &Keeper{
		UID:        ki.UID,
		Generation: InitialGeneration,
		ChangeTime: time.Time{},
		Spec:       &KeeperSpec{},
		Status: KeeperStatus{
			Healthy:         true,
			LastHealthyTime: time.Now(),
			BootUUID:        ki.BootUUID,
			MasterPriority:  ki.MasterPriority,
			Hostname:        ki.Hostname,
			NodeName:        ki.NodeName,
		},
	}
}

// SortedKeys returns sorted keeper UIDs.
func (kss Keepers) SortedKeys() []string {
	keys := make([]string, 0, len(kss))
	for k := range kss {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// DBSpec is the desired database configuration.
type DBSpec struct {
	// Init configuration used when InitMode is "new"
	NewConfig *NewConfig `json:"newConfig,omitempty"`
	// Point in time recovery init configuration used when InitMode is "pitr"
	PITRConfig *PITRConfig `json:"pitrConfig,omitempty"`
	// Map of postgres parameters
	PGParameters PGParameters `json:"pgParameters,omitempty"`
	// FollowConfig when Role is "standby"
	FollowConfig *FollowConfig `json:"followConfig,omitempty"`
	// The KeeperUID this db is assigned to
	KeeperUID string `json:"keeperUID,omitempty"`
	// InitMode defines the db initialization mode. Current modes are: none, new
	InitMode DBInitMode `json:"initMode,omitempty"`
	// DB Role (master or standby)
	Role common.Role `json:"role,omitempty"`
	// BeforeStopCommand defines a best-effort command executed by keeper before
	// stopping PostgreSQL for this DB assignment.
	BeforeStopCommand string `json:"beforeStopCommand,omitempty"`
	// PrePromoteCommand defines a fencing command executed by keeper before
	// promoting this standby to primary.
	PrePromoteCommand string `json:"prePromoteCommand,omitempty"`
	// AdditionalReplicationSlots is a list of additional replication slots.
	// Replication slots not defined here will be dropped from the instance
	// (i.e. manually created replication slots will be removed).
	AdditionalReplicationSlots []string `json:"additionalReplicationSlots"`
	// IgnoreReplicationSlots defines replication slots that hysteron should not
	// create, alter, or drop on the instance.
	IgnoreReplicationSlots []string `json:"ignoreReplicationSlots"`
	// IgnoreReplicationSlotMatchers defines structured matcher rules for
	// replication slots that hysteron should not create, alter, or drop.
	IgnoreReplicationSlotMatchers []ReplicationSlotMatcher `json:"ignoreReplicationSlotMatchers,omitempty"`
	// ManagedLogicalReplicationSlots defines logical slots managed by hysteron
	// on this database instance.
	ManagedLogicalReplicationSlots []ManagedLogicalReplicationSlot `json:"managedLogicalReplicationSlots,omitempty"`
	// Additional pg_hba.conf entries
	// We don't set omitempty since we want to distinguish between null or empty slice
	PGHBA []string `json:"pgHBA"`
	// Followers DB UIDs
	Followers []string `json:"followers"`
	// SynchronousStandbys are the standbys to be configured as synchronous
	SynchronousStandbys []string `json:"synchronousStandbys"`
	// External SynchronousStandbys are external standbys names to be configured as synchronous
	ExternalSynchronousStandbys []string `json:"externalSynchronousStandbys"`
	// Time after which any request (keepers checks from sentinel etc...) will fail.
	RequestTimeout Duration `json:"requestTimeout,omitzero"`
	// See ClusterSpec MaxStandbys description
	MaxStandbys uint16 `json:"maxStandbys,omitempty"`
	// AdditionalWalSenders defines the number of additional wal_senders in
	// addition to the ones internally defined by hysteron
	AdditionalWalSenders uint16 `json:"additionalWalSenders"`
	// NoStream declares archive-recovery-only mode for this DB assignment.
	NoStream bool `json:"noStream,omitempty"`
	// EnableLogicalSlotFailover enables experimental logical slot failover
	// semantics for this database assignment.
	EnableLogicalSlotFailover bool `json:"enableLogicalSlotFailover,omitempty"`
	// Use Synchronous replication between master and its standbys
	SynchronousReplication bool `json:"synchronousReplication,omitempty"`
	// Whether to use pg_rewind
	UsePgrewind bool `json:"usePgrewind,omitempty"`
	// Whether to include previous postgresql.conf
	IncludeConfig bool `json:"includePreviousConfig,omitempty"`
}

// DBStatus is the observed database status.
type DBStatus struct {
	// PGParameters are PostgreSQL parameters currently reported by the instance.
	PGParameters PGParameters `json:"pgParameters,omitempty"`

	// OrphanMemberSlots stores first-observed timestamps for orphaned member
	// replication slots (`hysteron_<dbuid>`) tracked on the current master.
	OrphanMemberSlots map[string]time.Time `json:"orphanMemberSlots,omitempty"`
	// ManagedLogicalSlots stores confirmed_flush_lsn values for managed logical
	// replication slots observed on the current instance.
	ManagedLogicalSlots map[string]uint64 `json:"managedLogicalSlots,omitempty"`

	// ListenAddress is PostgreSQL listen address.
	ListenAddress string `json:"listenAddress,omitempty"`
	// Port is PostgreSQL listen port.
	Port string `json:"port,omitempty"`

	// SystemID is PostgreSQL system identifier.
	SystemID string `json:"systemdID,omitempty"`

	// OlderWalFile is the oldest required WAL segment filename.
	OlderWalFile string `json:"olderWalFile,omitempty"`
	// TimelinesHistory is timeline history known by the instance.
	TimelinesHistory PostgresTimelinesHistory `json:"timelinesHistory,omitempty"`

	// DBUIDs of the internal standbys currently reported as in sync by the instance
	CurSynchronousStandbys []string `json:"-"`

	// SynchronousStandbys stores DBUIDs of internal standbys that we know are
	// in sync.
	// They could be currently down but we know that they were reported as in
	// sync in the past and they are defined inside synchronous_standby_names
	// so the instance will wait for acknowledge from them.
	// External synchronous standbys are not currently reported in DBStatus.
	SynchronousStandbys []string `json:"synchronousStandbys"`

	// CurrentGeneration is DB generation currently reported by PostgreSQL.
	CurrentGeneration int64 `json:"currentGeneration,omitempty"`

	// TimelineID is current timeline identifier.
	TimelineID uint64 `json:"timelineID,omitempty"`
	// XLogPos is current WAL position.
	XLogPos uint64 `json:"xLogPos,omitempty"`
	// Healthy reports PostgreSQL health.
	Healthy bool `json:"healthy,omitempty"`
}

// DB is a database object in cluster data.
type DB struct {
	ChangeTime time.Time `json:"changeTime,omitzero"`
	Spec       *DBSpec   `json:"spec,omitempty"`
	UID        string    `json:"uid,omitempty"`
	Status     DBStatus  `json:"status,omitzero"`
	Generation int64     `json:"generation,omitempty"`
}

// ProxySpec is the desired proxy configuration.
type ProxySpec struct {
	// MasterDBUID is DB UID currently selected as writable destination.
	MasterDBUID string `json:"masterDbUid,omitempty"`
	// EnabledProxies limits proxy UIDs allowed to serve traffic.
	EnabledProxies []string `json:"enabledProxies,omitempty"`
}

// ProxyStatus is the observed proxy status.
type ProxyStatus struct {
}

// Proxy is a proxy object in cluster data.
type Proxy struct {
	// Status is current observed proxy status.
	Status ProxyStatus `json:"status,omitzero"`
	// ChangeTime is proxy object last change time.
	ChangeTime time.Time `json:"changeTime,omitzero"`

	// UID is proxy UID.
	UID string `json:"uid,omitempty"`

	// Spec is desired proxy configuration.
	Spec ProxySpec `json:"spec,omitzero"`

	// Generation is proxy object generation.
	Generation int64 `json:"generation,omitempty"`
}
