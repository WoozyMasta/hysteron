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

// Package cluster defines Hysteron cluster-data contracts and helpers.
package cluster

import (
	"time"
)

// ClusterSpec is the desired cluster configuration.
type ClusterSpec struct { //nolint:revive
	// Interval to wait before next check
	SleepInterval *Duration `json:"sleepInterval,omitempty"`
	// Time after which any request (keepers checks from sentinel etc...) will fail.
	RequestTimeout *Duration `json:"requestTimeout,omitempty"`
	// Interval to wait for a db to be converged to the required state when
	// no long operation are expected.
	ConvergenceTimeout *Duration `json:"convergenceTimeout,omitempty"`
	// Interval to wait for a db to be initialized (doing a initdb)
	InitTimeout *Duration `json:"initTimeout,omitempty"`
	// Interval to wait for a db to be synced with a master
	SyncTimeout *Duration `json:"syncTimeout,omitempty"`
	// Interval to wait for a db to boot and become ready
	DBWaitReadyTimeout *Duration `json:"dbWaitReadyTimeout,omitempty"`
	// Interval after the first fail to declare a keeper or a db as not healthy.
	FailInterval *Duration `json:"failInterval,omitempty"`
	// EnableFailsafeMode allows a gated failsafe behavior during temporary DCS
	// outages. Disabled by default.
	EnableFailsafeMode *bool `json:"enableFailsafeMode,omitempty"`
	// FailsafeProbeInterval defines how often failsafe peer probes are executed
	// while DCS is degraded.
	FailsafeProbeInterval *Duration `json:"failsafeProbeInterval,omitempty"`
	// FailsafeProbeTimeout defines the timeout for one failsafe peer probe.
	FailsafeProbeTimeout *Duration `json:"failsafeProbeTimeout,omitempty"`
	// FailsafeMaxMissingPeers defines how many peer probes may be missing while
	// keeping failsafe active.
	FailsafeMaxMissingPeers *uint16 `json:"failsafeMaxMissingPeers,omitempty"`
	// FailsafeTTL defines the maximum time window for failsafe mode while DCS is
	// unavailable.
	FailsafeTTL *Duration `json:"failsafeTTL,omitempty"`
	// Interval after which a dead keeper will be removed from the cluster data
	DeadKeeperRemovalInterval *Duration `json:"deadKeeperRemovalInterval,omitempty"`
	// Interval to wait before next proxy check
	ProxyCheckInterval *Duration `json:"proxyCheckInterval,omitempty"`
	// Interval where the proxy must successfully complete a check
	ProxyTimeout *Duration `json:"proxyTimeout,omitempty"`
	// Max number of standbys. This needs to be greater enough to cover both
	// standby managed by hysteron and additional standbys configured by the
	// user. Its value affect different postgres parameters like
	// max_replication_slots and max_wal_senders. Setting this to a number
	// lower than the sum of hysteron managed standbys and user managed
	// standbys will have unpredicatable effects due to problems creating
	// replication slots or replication problems due to exhausted wal
	// senders.
	MaxStandbys *uint16 `json:"maxStandbys,omitempty"`
	// Max number of standbys for every sender. A sender can be a master or
	// another standby (if/when implementing cascading replication).
	MaxStandbysPerSender *uint16 `json:"maxStandbysPerSender,omitempty"`
	// Max lag in bytes that an asynchronous standy can have to be elected in
	// place of a failed master
	MaxStandbyLag *uint32 `json:"maxStandbyLag,omitempty"`
	// Use Synchronous replication between master and its standbys
	SynchronousReplication *bool `json:"synchronousReplication,omitempty"`
	// MinSynchronousStandbys is the mininum number if synchronous standbys
	// to be configured when SynchronousReplication is true
	MinSynchronousStandbys *uint16 `json:"minSynchronousStandbys,omitempty"`
	// MaxSynchronousStandbys is the maximum number if synchronous standbys
	// to be configured when SynchronousReplication is true
	MaxSynchronousStandbys *uint16 `json:"maxSynchronousStandbys,omitempty"`
	// AdditionalWalSenders defines the number of additional wal_senders in
	// addition to the ones internally defined by hysteron
	AdditionalWalSenders *uint16 `json:"additionalWalSenders"`
	// Whether to use pg_rewind
	UsePgrewind *bool `json:"usePgrewind,omitempty"`
	// InitMode defines the cluster initialization mode. Current modes are: new, existing, pitr
	InitMode *ClusterInitMode `json:"initMode,omitempty"`
	// Whether to merge pgParameters of the initialized db cluster, useful
	// the retain initdb generated parameters when InitMode is new, retain
	// current parameters when initMode is existing or pitr.
	MergePgParameters *bool `json:"mergePgParameters,omitempty"`
	// Role defines the cluster operating role (master or standby of an external database)
	Role *ClusterRole `json:"role,omitempty"`
	// Init configuration used when InitMode is "new"
	NewConfig *NewConfig `json:"newConfig,omitempty"`
	// Point in time recovery init configuration used when InitMode is "pitr"
	PITRConfig *PITRConfig `json:"pitrConfig,omitempty"`
	// Existing init configuration used when InitMode is "existing"
	ExistingConfig *ExistingConfig `json:"existingConfig,omitempty"`
	// Standby config when role is standby
	StandbyConfig *StandbyConfig `json:"standbyConfig,omitempty"`
	// Define the mode of the default hba rules needed for replication by standby keepers (the su and repl auth methods will be the one provided in the keeper command line options)
	// Values can be "all" or "strict", "all" allow access from all ips, "strict" restrict master access to standby servers ips.
	// Default is "all"
	DefaultSUReplAccessMode *SUReplAccessMode `json:"defaultSUReplAccessMode,omitempty"`
	// Map of postgres parameters
	PGParameters PGParameters `json:"pgParameters,omitempty"`
	// Enable automatic pg restart when pg parameters that requires restart changes
	AutomaticPgRestart *bool `json:"automaticPgRestart"`
	// MemberReplicationSlotTTL defines how long orphaned member replication slots
	// may remain before cleanup is considered. Zero or nil disables TTL-based
	// cleanup.
	MemberReplicationSlotTTL *Duration `json:"memberReplicationSlotTTL,omitempty"`
	// BeforeStopCommand defines a best-effort command executed by keeper before
	// stopping PostgreSQL. Command failures are logged and do not block stop.
	BeforeStopCommand string `json:"beforeStopCommand,omitempty"`
	// PrePromoteCommand defines a fencing command executed by keeper before
	// promoting standby to primary. Command failures block promotion.
	PrePromoteCommand string `json:"prePromoteCommand,omitempty"`
	// AdditionalMasterReplicationSlots defines additional replication slots to
	// be created on the master postgres instance. Replication slots not defined
	// here will be dropped from the master instance (i.e. manually created
	// replication slots will be removed).
	AdditionalMasterReplicationSlots []string `json:"additionalMasterReplicationSlots"`
	// IgnoreMasterReplicationSlots defines replication slots that hysteron
	// should not create, alter, or drop on the current master instance.
	IgnoreMasterReplicationSlots []string `json:"ignoreMasterReplicationSlots"`
	// IgnoreMasterReplicationSlotMatchers defines structured matcher rules for
	// replication slots that hysteron should ignore on the current master.
	IgnoreMasterReplicationSlotMatchers []ReplicationSlotMatcher `json:"ignoreMasterReplicationSlotMatchers,omitempty"`
	// ManagedLogicalReplicationSlots defines desired logical slots managed by
	// hysteron on the current primary instance.
	ManagedLogicalReplicationSlots []ManagedLogicalReplicationSlot `json:"managedLogicalReplicationSlots,omitempty"`
	// Additional pg_hba.conf entries
	// we don't set omitempty since we want to distinguish between null or empty slice
	PGHBA []string `json:"pgHBA"`
	// EnableLogicalSlotFailover enables experimental logical slot failover
	// semantics. Disabled by default and currently reserved for controlled
	// rollouts.
	EnableLogicalSlotFailover bool `json:"enableLogicalSlotFailover,omitempty"`
}

// ClusterStatus is the observed cluster status.
type ClusterStatus struct { //nolint:revive
	// PauseUntil is optional pause expiry time in UTC.
	PauseUntil *time.Time `json:"pauseUntil,omitempty"`
	// Phase is current cluster lifecycle phase.
	Phase ClusterPhase `json:"phase,omitempty"`
	// Master DB UID
	Master string `json:"master,omitempty"`
	// PauseReason is optional operator-provided pause rationale.
	PauseReason       string `json:"pauseReason,omitempty"`
	CurrentGeneration int64  `json:"currentGeneration,omitempty"`
	// Paused reports whether mutating management operations are blocked.
	Paused bool `json:"paused,omitempty"`
}

// Cluster is the top-level cluster object in cluster data.
type Cluster struct {
	ChangeTime time.Time     `json:"changeTime,omitzero"`
	Spec       *ClusterSpec  `json:"spec,omitempty"`
	UID        string        `json:"uid,omitempty"`
	Status     ClusterStatus `json:"status,omitzero"`
	Generation int64         `json:"generation,omitempty"`
}
