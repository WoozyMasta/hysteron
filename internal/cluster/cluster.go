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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/copystructure"
	"github.com/woozymasta/hysteron/internal/common"
	util "github.com/woozymasta/hysteron/internal/postgresql"
)

// Uint16P returns a pointer to u.
func Uint16P(u uint16) *uint16 {
	return new(u)
}

// Uint32P returns a pointer to u.
func Uint32P(u uint32) *uint32 {
	return new(u)
}

// BoolP returns a pointer to b.
func BoolP(b bool) *bool {
	return new(b)
}

const (
	// CurrentCDFormatVersion is the supported cluster-data format version.
	CurrentCDFormatVersion uint64 = 1
)

const (
	// DefaultStoreTimeout is the default timeout for store requests.
	DefaultStoreTimeout = 5 * time.Second

	// DefaultDBNotIncreasingXLogPosTimes is the default tolerated stalled WAL checks.
	DefaultDBNotIncreasingXLogPosTimes = 10

	// DefaultSleepInterval is the default interval between cluster checks.
	DefaultSleepInterval = 5 * time.Second
	// DefaultRequestTimeout is the default timeout for component requests.
	DefaultRequestTimeout = 10 * time.Second
	// DefaultConvergenceTimeout is the default database convergence timeout.
	DefaultConvergenceTimeout = 30 * time.Second
	// DefaultInitTimeout is the default database initialization timeout.
	DefaultInitTimeout = 5 * time.Minute
	// DefaultSyncTimeout is the default database sync timeout.
	DefaultSyncTimeout = 0
	// DefaultDBWaitReadyTimeout is the default timeout for database readiness.
	DefaultDBWaitReadyTimeout = 60 * time.Second
	// DefaultFailInterval is the default interval before marking a component unhealthy.
	DefaultFailInterval = 30 * time.Second
	// DefaultDeadKeeperRemovalInterval is the default interval before removing dead keepers.
	DefaultDeadKeeperRemovalInterval = 48 * time.Hour
	// DefaultProxyCheckInterval is the default interval between proxy checks.
	DefaultProxyCheckInterval = 5 * time.Second
	// DefaultProxyTimeout is the default proxy check timeout.
	DefaultProxyTimeout = 15 * time.Second
	// DefaultMaxStandbys is the default maximum number of standbys.
	DefaultMaxStandbys uint16 = 20
	// DefaultMaxStandbysPerSender is the default maximum number of standbys per sender.
	DefaultMaxStandbysPerSender uint16 = 3
	// DefaultMaxStandbyLag is the default maximum lag for failover candidates.
	DefaultMaxStandbyLag = 1024 * 1204
	// DefaultSynchronousReplication is the default synchronous replication setting.
	DefaultSynchronousReplication = false
	// DefaultMinSynchronousStandbys is the default minimum synchronous standby count.
	DefaultMinSynchronousStandbys uint16 = 1
	// DefaultMaxSynchronousStandbys is the default maximum synchronous standby count.
	DefaultMaxSynchronousStandbys uint16 = 1
	// DefaultAdditionalWalSenders is the default additional wal_senders count.
	DefaultAdditionalWalSenders = 5
	// DefaultUsePgrewind is the default pg_rewind setting.
	DefaultUsePgrewind = false
	// DefaultMergePGParameter is the default pg parameter merge setting.
	DefaultMergePGParameter = true
	// DefaultRole is the default cluster role.
	DefaultRole ClusterRole = ClusterRoleMaster
	// DefaultSUReplAccess is the default superuser replication access mode.
	DefaultSUReplAccess SUReplAccessMode = SUReplAccessAll
	// DefaultAutomaticPgRestart is the default automatic PostgreSQL restart setting.
	DefaultAutomaticPgRestart = false
	// DefaultEnableFailsafeMode controls whether failsafe mode is enabled.
	DefaultEnableFailsafeMode = false
	// DefaultFailsafeProbeInterval is the default interval between failsafe probes.
	DefaultFailsafeProbeInterval = 2 * time.Second
	// DefaultFailsafeProbeTimeout is the default timeout of one failsafe probe.
	DefaultFailsafeProbeTimeout = 1 * time.Second
	// DefaultFailsafeMaxMissingPeers is the default allowed missing peer probes.
	DefaultFailsafeMaxMissingPeers uint16 = 0
	// DefaultFailsafeTTL is the default maximum time in failsafe mode without DCS.
	DefaultFailsafeTTL = 15 * time.Second
)

const (
	// NoGeneration is the zero generation marker.
	NoGeneration int64 = 0
	// InitialGeneration is the first generation assigned to new objects.
	InitialGeneration int64 = 1
)

// PGParameters maps PostgreSQL parameter names to values.
type PGParameters map[string]string

// FollowType identifies how a standby follows its source.
type FollowType string

const (
	// FollowTypeInternal follows a db managed by a keeper in our cluster.
	FollowTypeInternal FollowType = "internal"
	// FollowTypeExternal follows an external db.
	FollowTypeExternal FollowType = "external"
)

// FollowConfig configures the source followed by a standby db.
type FollowConfig struct {
	// Standby settings when Type is "external"
	StandbySettings *StandbySettings `json:"standbySettings,omitempty"`
	// ArchiveRecoverySettings defines restore behavior when following external source.
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
	// Type selects whether source is internal or external.
	Type FollowType `json:"type,omitempty"`
	// Keeper ID to follow when Type is "internal"
	DBUID string `json:"dbuid,omitempty"`
}

// PostgresBinaryVersion contains the PostgreSQL major and minor version.
type PostgresBinaryVersion struct {
	// Maj is PostgreSQL major version.
	Maj int
	// Min is PostgreSQL minor version.
	Min int
}

// ClusterPhase identifies the current cluster lifecycle phase.
type ClusterPhase string //nolint:revive

const (
	// ClusterPhaseInitializing means the cluster is being initialized.
	ClusterPhaseInitializing ClusterPhase = "initializing"
	// ClusterPhaseNormal means the cluster is operating normally.
	ClusterPhaseNormal ClusterPhase = "normal"
)

// ClusterRole identifies whether the cluster is primary or standby.
type ClusterRole string //nolint:revive

const (
	// ClusterRoleMaster is a primary cluster role.
	ClusterRoleMaster ClusterRole = "master"
	// ClusterRoleStandby is a standby cluster role.
	ClusterRoleStandby ClusterRole = "standby"
)

// ClusterInitMode identifies how a cluster is initialized.
type ClusterInitMode string //nolint:revive

const (
	// ClusterInitModeNew initializes a cluster from a fresh database cluster.
	ClusterInitModeNew ClusterInitMode = "new"
	// ClusterInitModePITR initializes a cluster through point-in-time recovery.
	ClusterInitModePITR ClusterInitMode = "pitr"
	// ClusterInitModeExisting initializes from an existing populated db cluster.
	ClusterInitModeExisting ClusterInitMode = "existing"
)

// ClusterInitModeP returns a pointer to s.
func ClusterInitModeP(s ClusterInitMode) *ClusterInitMode { //nolint:revive
	return new(s)
}

// ClusterRoleP returns a pointer to s.
func ClusterRoleP(s ClusterRole) *ClusterRole { //nolint:revive
	return new(s)
}

// DBInitMode identifies how a database is initialized.
type DBInitMode string

const (
	// DBInitModeNone means no database initialization is requested.
	DBInitModeNone DBInitMode = "none"
	// DBInitModeExisting uses existing db cluster data.
	DBInitModeExisting DBInitMode = "existing"
	// DBInitModeNew initializes a db from a fresh database cluster.
	DBInitModeNew DBInitMode = "new"
	// DBInitModePITR initializes a db through point-in-time recovery.
	DBInitModePITR DBInitMode = "pitr"
	// DBInitModeResync initializes a db by resyncing from a target db cluster.
	DBInitModeResync DBInitMode = "resync"
)

// NewConfig configures fresh database initialization.
type NewConfig struct {
	// Locale is initdb locale.
	Locale string `json:"locale,omitempty"`
	// Encoding is initdb encoding.
	Encoding string `json:"encoding,omitempty"`
	// DataChecksums enables initdb data checksums.
	DataChecksums bool `json:"dataChecksums,omitempty"`
}

// PITRConfig configures point-in-time recovery initialization.
type PITRConfig struct {
	// ArchiveRecoverySettings configures archive-based recovery.
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
	// RecoveryTargetSettings configures stop target during recovery.
	RecoveryTargetSettings *RecoveryTargetSettings `json:"recoveryTargetSettings,omitempty"`
	// DataRestoreCommand defines the command to execute for restoring the db
	// cluster data). %d is replaced with the full path to the db cluster
	// datadir. Use %% to embed an actual % character.
	DataRestoreCommand string `json:"dataRestoreCommand,omitempty"`
}

// ExistingConfig configures initialization from an existing keeper.
type ExistingConfig struct {
	// KeeperUID identifies keeper holding existing initialized data.
	KeeperUID string `json:"keeperUID,omitempty"`
}

// StandbyConfig configures the cluster when its role is standby.
type StandbyConfig struct {
	// StandbySettings defines primary connection settings.
	StandbySettings *StandbySettings `json:"standbySettings,omitempty"`
	// ArchiveRecoverySettings defines restore behavior for standby role.
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
	// NoStream declares archive-recovery-only mode (no streaming). When true,
	// standby logical-slot synchronization/advance paths are disabled.
	NoStream bool `json:"noStream,omitempty"`
}

// ArchiveRecoverySettings defines archive recovery settings.
type ArchiveRecoverySettings struct {
	// value for restore_command
	RestoreCommand string `json:"restoreCommand,omitempty"`
}

// RecoveryTargetSettings defines recovery target settings.
type RecoveryTargetSettings struct {
	// RecoveryTarget is generic recovery target selector.
	RecoveryTarget string `json:"recoveryTarget,omitempty"`
	// RecoveryTargetLsn is target LSN.
	RecoveryTargetLsn string `json:"recoveryTargetLsn,omitempty"`
	// RecoveryTargetName is target restore point name.
	RecoveryTargetName string `json:"recoveryTargetName,omitempty"`
	// RecoveryTargetTime is target timestamp.
	RecoveryTargetTime string `json:"recoveryTargetTime,omitempty"`
	// RecoveryTargetXid is target transaction ID.
	RecoveryTargetXid string `json:"recoveryTargetXid,omitempty"`
	// RecoveryTargetTimeline is target timeline selector.
	RecoveryTargetTimeline string `json:"recoveryTargetTimeline,omitempty"`
}

// StandbySettings defines standby settings.
type StandbySettings struct {
	// PrimaryConninfo is primary connection string.
	PrimaryConninfo string `json:"primaryConninfo,omitempty"`
	// PrimarySlotName is replication slot name on upstream.
	PrimarySlotName string `json:"primarySlotName,omitempty"`
	// RecoveryMinApplyDelay delays replay for standby.
	RecoveryMinApplyDelay string `json:"recoveryMinApplyDelay,omitempty"`
}

// ManagedLogicalReplicationSlot defines one managed logical slot desired in
// cluster spec.
type ManagedLogicalReplicationSlot struct {
	// Name is the logical replication slot name.
	Name string `json:"name,omitempty"`
	// Database is the database where the logical slot is created.
	Database string `json:"database,omitempty"`
	// Plugin is the logical decoding output plugin.
	Plugin string `json:"plugin,omitempty"`
}

// ReplicationSlotType identifies replication slot type for ignore matchers.
type ReplicationSlotType string

const (
	// ReplicationSlotTypePhysical matches physical replication slots.
	ReplicationSlotTypePhysical ReplicationSlotType = "physical"
	// ReplicationSlotTypeLogical matches logical replication slots.
	ReplicationSlotTypeLogical ReplicationSlotType = "logical"
)

// ReplicationSlotMatcher defines subset matching for slot ignore policies.
type ReplicationSlotMatcher struct {
	// Name is an optional slot name selector.
	Name string `json:"name,omitempty"`
	// Type optionally constrains slot type (`physical` or `logical`).
	Type ReplicationSlotType `json:"type,omitempty"`
	// Database optionally constrains logical slot database.
	Database string `json:"database,omitempty"`
	// Plugin optionally constrains logical slot plugin.
	Plugin string `json:"plugin,omitempty"`
}

// SUReplAccessMode identifies default superuser replication access scope.
type SUReplAccessMode string

const (
	// SUReplAccessAll allows access from every host.
	SUReplAccessAll SUReplAccessMode = "all"
	// SUReplAccessStrict allows access from standby server IPs only.
	SUReplAccessStrict SUReplAccessMode = "strict"
)

// SUReplAccessModeP returns a pointer to s.
func SUReplAccessModeP(s SUReplAccessMode) *SUReplAccessMode {
	return new(s)
}

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
	// Phase is current cluster lifecycle phase.
	Phase ClusterPhase `json:"phase,omitempty"`
	// Master DB UID
	Master            string `json:"master,omitempty"`
	CurrentGeneration int64  `json:"currentGeneration,omitempty"`
}

// Cluster is the top-level cluster object in cluster data.
type Cluster struct {
	ChangeTime time.Time     `json:"changeTime,omitzero"`
	Spec       *ClusterSpec  `json:"spec,omitempty"`
	UID        string        `json:"uid,omitempty"`
	Status     ClusterStatus `json:"status,omitzero"`
	Generation int64         `json:"generation,omitempty"`
}

// DeepCopy returns an independent copy of the cluster.
func (c *Cluster) DeepCopy() *Cluster {
	nc, err := copystructure.Copy(c)
	common.MustNot(err, "cluster deep copy")
	return nc.(*Cluster)
}

// DeepCopy returns an independent copy of the cluster spec.
func (c *ClusterSpec) DeepCopy() *ClusterSpec {
	nc, err := copystructure.Copy(c)
	common.MustNot(err, "cluster spec deep copy")
	return nc.(*ClusterSpec)
}

// DefSpec returns a new ClusterSpec with unspecified values populated with
// their defaults
func (c *Cluster) DefSpec() *ClusterSpec {
	return c.Spec.WithDefaults()
}

// WithDefaults returns a new ClusterSpec with unspecified values populated with
// their defaults
func (c *ClusterSpec) WithDefaults() *ClusterSpec {
	// Take a copy of the input ClusterSpec since we don't want to change the original
	s := c.DeepCopy()
	if s.SleepInterval == nil {
		s.SleepInterval = &Duration{Duration: DefaultSleepInterval}
	}
	if s.RequestTimeout == nil {
		s.RequestTimeout = &Duration{Duration: DefaultRequestTimeout}
	}
	if s.ConvergenceTimeout == nil {
		s.ConvergenceTimeout = &Duration{Duration: DefaultConvergenceTimeout}
	}
	if s.InitTimeout == nil {
		s.InitTimeout = &Duration{Duration: DefaultInitTimeout}
	}
	if s.SyncTimeout == nil {
		s.SyncTimeout = &Duration{Duration: DefaultSyncTimeout}
	}
	if s.DBWaitReadyTimeout == nil {
		s.DBWaitReadyTimeout = &Duration{Duration: DefaultDBWaitReadyTimeout}
	}
	if s.FailInterval == nil {
		s.FailInterval = &Duration{Duration: DefaultFailInterval}
	}
	if s.EnableFailsafeMode == nil {
		s.EnableFailsafeMode = BoolP(DefaultEnableFailsafeMode)
	}
	if s.FailsafeProbeInterval == nil {
		s.FailsafeProbeInterval = &Duration{Duration: DefaultFailsafeProbeInterval}
	}
	if s.FailsafeProbeTimeout == nil {
		s.FailsafeProbeTimeout = &Duration{Duration: DefaultFailsafeProbeTimeout}
	}
	if s.FailsafeMaxMissingPeers == nil {
		s.FailsafeMaxMissingPeers = Uint16P(DefaultFailsafeMaxMissingPeers)
	}
	if s.FailsafeTTL == nil {
		s.FailsafeTTL = &Duration{Duration: DefaultFailsafeTTL}
	}
	if s.DeadKeeperRemovalInterval == nil {
		s.DeadKeeperRemovalInterval = &Duration{Duration: DefaultDeadKeeperRemovalInterval}
	}
	if s.ProxyCheckInterval == nil {
		s.ProxyCheckInterval = &Duration{Duration: DefaultProxyCheckInterval}
	}
	if s.ProxyTimeout == nil {
		s.ProxyTimeout = &Duration{Duration: DefaultProxyTimeout}
	}
	if s.MaxStandbys == nil {
		s.MaxStandbys = new(DefaultMaxStandbys)
	}
	if s.MaxStandbysPerSender == nil {
		s.MaxStandbysPerSender = new(DefaultMaxStandbysPerSender)
	}
	if s.MaxStandbyLag == nil {
		s.MaxStandbyLag = Uint32P(DefaultMaxStandbyLag)
	}
	if s.SynchronousReplication == nil {
		s.SynchronousReplication = new(DefaultSynchronousReplication)
	}
	if s.UsePgrewind == nil {
		s.UsePgrewind = new(DefaultUsePgrewind)
	}
	if s.MinSynchronousStandbys == nil {
		s.MinSynchronousStandbys = new(DefaultMinSynchronousStandbys)
	}
	if s.MaxSynchronousStandbys == nil {
		s.MaxSynchronousStandbys = new(DefaultMaxSynchronousStandbys)
	}
	if s.AdditionalWalSenders == nil {
		s.AdditionalWalSenders = Uint16P(DefaultAdditionalWalSenders)
	}
	if s.MergePgParameters == nil {
		s.MergePgParameters = new(DefaultMergePGParameter)
	}
	if s.DefaultSUReplAccessMode == nil {
		v := DefaultSUReplAccess
		s.DefaultSUReplAccessMode = &v
	}
	if s.Role == nil {
		v := DefaultRole
		s.Role = &v
	}
	if s.AutomaticPgRestart == nil {
		s.AutomaticPgRestart = new(DefaultAutomaticPgRestart)
	}
	return s
}

// Validate validates a cluster spec.
func (c *ClusterSpec) Validate() error {
	s := c.WithDefaults()
	if s.SleepInterval.Duration < 0 {
		return errors.New("sleepInterval must be positive")
	}
	if s.RequestTimeout.Duration < 0 {
		return errors.New("requestTimeout must be positive")
	}
	if s.ConvergenceTimeout.Duration < 0 {
		return errors.New("convergenceTimeout must be positive")
	}
	if s.InitTimeout.Duration < 0 {
		return errors.New("initTimeout must be positive")
	}
	if s.SyncTimeout.Duration < 0 {
		return errors.New("syncTimeout must be positive")
	}
	if s.DBWaitReadyTimeout.Duration < 0 {
		return errors.New("dbWaitReadyTimeout must be positive")
	}
	if s.FailInterval.Duration < 0 {
		return errors.New("failInterval must be positive")
	}
	if s.FailsafeProbeInterval.Duration < 0 {
		return errors.New("failsafeProbeInterval must be positive")
	}
	if s.FailsafeProbeTimeout.Duration < 0 {
		return errors.New("failsafeProbeTimeout must be positive")
	}
	if s.FailsafeTTL.Duration < 0 {
		return errors.New("failsafeTTL must be positive")
	}
	if s.FailsafeProbeTimeout.Duration > s.FailsafeProbeInterval.Duration {
		return errors.New("failsafeProbeTimeout should be less than or equal to failsafeProbeInterval")
	}
	if s.FailsafeTTL.Duration < s.FailsafeProbeInterval.Duration {
		return errors.New("failsafeTTL should be greater than or equal to failsafeProbeInterval")
	}
	if s.DeadKeeperRemovalInterval.Duration < 0 {
		return errors.New("deadKeeperRemovalInterval must be positive")
	}
	if s.ProxyCheckInterval.Duration < 0 {
		return errors.New("proxyCheckInterval must be positive")
	}
	if s.ProxyTimeout.Duration < 0 {
		return errors.New("proxyTimeout must be positive")
	}
	if s.ProxyCheckInterval.Duration >= s.ProxyTimeout.Duration {
		return errors.New("proxyCheckInterval should be less than proxyTimeout")
	}
	if err := validateHATiming(
		s.SleepInterval.Duration,
		s.RequestTimeout.Duration,
		s.FailInterval.Duration,
	); err != nil {
		return err
	}
	if *s.MaxStandbys < 1 {
		return errors.New("maxStandbys must be at least 1")
	}
	if *s.MaxStandbysPerSender < 1 {
		return errors.New("maxStandbysPerSender must be at least 1")
	}
	if *s.MaxSynchronousStandbys < 1 {
		return errors.New("maxSynchronousStandbys must be at least 1")
	}
	if *s.MaxSynchronousStandbys < *s.MinSynchronousStandbys {
		return errors.New("maxSynchronousStandbys must be greater or equal to minSynchronousStandbys")
	}
	if s.InitMode == nil {
		return errors.New("initMode undefined")
	}
	for _, replicationSlot := range s.AdditionalMasterReplicationSlots {
		if err := validateReplicationSlot(replicationSlot); err != nil {
			return err
		}
	}
	for _, replicationSlot := range s.IgnoreMasterReplicationSlots {
		if err := validateReplicationSlotName(replicationSlot); err != nil {
			return err
		}
	}
	for _, matcher := range s.IgnoreMasterReplicationSlotMatchers {
		if err := validateReplicationSlotMatcher(matcher); err != nil {
			return err
		}
	}
	if s.MemberReplicationSlotTTL != nil && s.MemberReplicationSlotTTL.Duration < 0 {
		return errors.New("memberReplicationSlotTTL must be positive")
	}
	logicalSlotsSeen := map[string]struct{}{}
	for _, slot := range s.ManagedLogicalReplicationSlots {
		if err := validateReplicationSlotName(slot.Name); err != nil {
			return err
		}
		if _, ok := logicalSlotsSeen[slot.Name]; ok {
			return fmt.Errorf("duplicated managedLogicalReplicationSlots name: %q", slot.Name)
		}
		logicalSlotsSeen[slot.Name] = struct{}{}
		if strings.TrimSpace(slot.Database) == "" {
			return fmt.Errorf("managedLogicalReplicationSlots database undefined for slot %q", slot.Name)
		}
		if strings.TrimSpace(slot.Plugin) == "" {
			return fmt.Errorf("managedLogicalReplicationSlots plugin undefined for slot %q", slot.Name)
		}
	}
	if len(s.ManagedLogicalReplicationSlots) > 0 {
		walLevel := strings.ToLower(strings.TrimSpace(s.PGParameters["wal_level"]))
		if walLevel != "logical" {
			return errors.New(
				`managedLogicalReplicationSlots requires pgParameters.wal_level to be set to "logical"`,
			)
		}
	}
	if s.EnableLogicalSlotFailover && len(s.ManagedLogicalReplicationSlots) == 0 {
		return errors.New(
			`enableLogicalSlotFailover requires managedLogicalReplicationSlots to be configured`,
		)
	}
	if s.EnableLogicalSlotFailover {
		if raw, ok := s.PGParameters["hot_standby_feedback"]; ok {
			normalized := strings.ToLower(strings.TrimSpace(raw))
			if normalized != "on" && normalized != "true" && normalized != "1" {
				return errors.New(
					`enableLogicalSlotFailover requires pgParameters.hot_standby_feedback to be enabled (on/true/1)`,
				)
			}
		}
	}

	// The unique validation we're doing on pgHBA entries is that they don't contain a newline character
	for _, e := range s.PGHBA {
		if strings.Contains(e, "\n") {
			return errors.New("pgHBA entries cannot contain newline characters")
		}
	}

	switch *s.InitMode {
	case ClusterInitModeNew:
		if *s.Role == ClusterRoleStandby {
			return errors.New("invalid cluster role standby when initMode is \"new\"")
		}

	case ClusterInitModeExisting:
		if s.ExistingConfig == nil {
			return errors.New("existingConfig undefined. Required when initMode is \"existing\"")
		}
		if s.ExistingConfig.KeeperUID == "" {
			return errors.New("existingConfig.keeperUID undefined")
		}

	case ClusterInitModePITR:
		if s.PITRConfig == nil {
			return errors.New("pitrConfig undefined. Required when initMode is \"pitr\"")
		}
		if s.PITRConfig.DataRestoreCommand == "" {
			return errors.New("pitrConfig.DataRestoreCommand undefined")
		}
		if s.PITRConfig.RecoveryTargetSettings != nil && *s.Role == ClusterRoleStandby {
			return errors.New("cannot define pitrConfig.RecoveryTargetSettings when required cluster role is standby")
		}
		if err := validateRecoveryTargetSettings(s.PITRConfig.RecoveryTargetSettings); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown initMode: %q", *s.InitMode)
	}

	switch *s.DefaultSUReplAccessMode {
	case SUReplAccessAll:
	case SUReplAccessStrict:
	default:
		return fmt.Errorf("unknown defaultSUReplAccessMode: %q", *s.DefaultSUReplAccessMode)
	}

	switch *s.Role {
	case ClusterRoleMaster:
	case ClusterRoleStandby:
		if s.StandbyConfig == nil {
			return errors.New("standbyConfig undefined. Required when cluster role is \"standby\"")
		}
	default:
		return fmt.Errorf("unknown role: %q", *s.InitMode)
	}
	return nil
}

func validateHATiming(
	sleepInterval time.Duration,
	requestTimeout time.Duration,
	failInterval time.Duration,
) error {
	// Keep sentinel loop and request retries bounded by fail interval to reduce
	// self-inflicted false unhealthy/failover conditions.
	if sleepInterval+2*requestTimeout > failInterval {
		return errors.New(
			"invalid HA timing: sleepInterval + 2*requestTimeout must be less than or equal to failInterval",
		)
	}
	return nil
}

func validateReplicationSlot(replicationSlot string) error {
	if err := validateReplicationSlotName(replicationSlot); err != nil {
		return err
	}
	if common.IsHysteronName(replicationSlot) {
		return fmt.Errorf("replication slot name is reserved: %q", replicationSlot)
	}
	return nil
}

func validateReplicationSlotName(replicationSlot string) error {
	if !util.IsValidReplSlotName(replicationSlot) {
		return fmt.Errorf("wrong replication slot name: %q", replicationSlot)
	}
	return nil
}

func validateReplicationSlotMatcher(matcher ReplicationSlotMatcher) error {
	if matcher.Name != "" {
		if err := validateReplicationSlotName(matcher.Name); err != nil {
			return err
		}
	}
	switch matcher.Type {
	case "":
	case ReplicationSlotTypePhysical, ReplicationSlotTypeLogical:
	default:
		return fmt.Errorf("wrong replication slot matcher type: %q", matcher.Type)
	}
	if matcher.Type == ReplicationSlotTypePhysical {
		if strings.TrimSpace(matcher.Database) != "" || strings.TrimSpace(matcher.Plugin) != "" {
			return errors.New("physical replication slot matcher cannot define database or plugin")
		}
	}
	if strings.TrimSpace(matcher.Database) != "" || strings.TrimSpace(matcher.Plugin) != "" {
		if matcher.Type != "" && matcher.Type != ReplicationSlotTypeLogical {
			return errors.New("replication slot matcher with database or plugin must have logical type")
		}
	}
	if matcher.Name == "" && matcher.Type == "" &&
		strings.TrimSpace(matcher.Database) == "" &&
		strings.TrimSpace(matcher.Plugin) == "" {
		return errors.New("empty replication slot matcher is not allowed")
	}
	return nil
}

func validateRecoveryTargetSettings(settings *RecoveryTargetSettings) error {
	if settings == nil {
		return nil
	}

	recoveryTarget := strings.TrimSpace(settings.RecoveryTarget)
	if recoveryTarget != "" && recoveryTarget != "immediate" {
		return fmt.Errorf("recoveryTarget must be \"immediate\" when defined, got %q", settings.RecoveryTarget)
	}

	targets := 0
	for _, value := range []string{
		recoveryTarget,
		strings.TrimSpace(settings.RecoveryTargetLsn),
		strings.TrimSpace(settings.RecoveryTargetName),
		strings.TrimSpace(settings.RecoveryTargetTime),
		strings.TrimSpace(settings.RecoveryTargetXid),
	} {
		if value != "" {
			targets++
		}
	}
	if targets > 1 {
		return errors.New(
			"only one recovery target selector can be set among recoveryTarget, recoveryTargetLsn, recoveryTargetName, recoveryTargetTime, recoveryTargetXid",
		)
	}

	return nil
}

// UpdateSpec validates and replaces the cluster spec.
func (c *Cluster) UpdateSpec(ns *ClusterSpec) error {
	s := c.Spec
	if err := ns.Validate(); err != nil {
		return fmt.Errorf("invalid cluster spec: %v", err)
	}
	ds := s.WithDefaults()
	dns := ns.WithDefaults()
	if *ds.InitMode != *dns.InitMode {
		return errors.New("cannot change cluster init mode")
	}
	if *ds.Role == ClusterRoleMaster && *dns.Role == ClusterRoleStandby {
		return errors.New("cannot update a cluster from master role to standby role")
	}
	c.Spec = ns
	return nil
}

// NewCluster creates a new cluster with the initial generation.
func NewCluster(uid string, cs *ClusterSpec) *Cluster {
	c := &Cluster{
		UID:        uid,
		Generation: InitialGeneration,
		ChangeTime: time.Now(),
		Spec:       cs,
		Status: ClusterStatus{
			Phase: ClusterPhaseInitializing,
		},
	}
	return c
}

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
	// PostgresBinaryVersion is PostgreSQL binary version detected by keeper.
	PostgresBinaryVersion PostgresBinaryVersion `json:"postgresBinaryVersion,omitzero"`
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
	// NoStream declares archive-recovery-only mode for this DB assignment.
	NoStream bool `json:"noStream,omitempty"`
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

// Duration is needed to be able to marshal/unmarshal json strings with time
// unit (eg. 3s, 100ms) instead of ugly times in nanoseconds.
type Duration struct {
	time.Duration
}

// MarshalJSON encodes Duration as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// MarshalText encodes Duration as a Go duration string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalJSON decodes Duration from a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	return d.UnmarshalText([]byte(strings.Trim(string(b), `"`)))
}

// UnmarshalText decodes Duration from a Go duration string.
func (d *Duration) UnmarshalText(text []byte) error {
	du, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	d.Duration = du
	return nil
}

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

// DeepCopy returns an independent copy of cluster data.
func (c *ClusterData) DeepCopy() *ClusterData {
	nc, err := copystructure.Copy(c)
	common.MustNot(err, "cluster data deep copy")
	return nc.(*ClusterData)
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
