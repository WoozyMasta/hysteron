// Copyright 2016 Sorint.lab
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

// Package cluster defines Stolon cluster-data contracts and helpers.
package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/copystructure"
	"github.com/sorintlab/stolon/internal/common"
	util "github.com/sorintlab/stolon/internal/postgresql"
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
	DefaultFailInterval = 20 * time.Second
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
	Type FollowType `json:"type,omitempty"`
	// Keeper ID to follow when Type is "internal"
	DBUID string `json:"dbuid,omitempty"`
	// Standby settings when Type is "external"
	StandbySettings         *StandbySettings         `json:"standbySettings,omitempty"`
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
}

// PostgresBinaryVersion contains the PostgreSQL major and minor version.
type PostgresBinaryVersion struct {
	Maj int
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
	Locale        string `json:"locale,omitempty"`
	Encoding      string `json:"encoding,omitempty"`
	DataChecksums bool   `json:"dataChecksums,omitempty"`
}

// PITRConfig configures point-in-time recovery initialization.
type PITRConfig struct {
	// DataRestoreCommand defines the command to execute for restoring the db
	// cluster data). %d is replaced with the full path to the db cluster
	// datadir. Use %% to embed an actual % character.
	DataRestoreCommand      string                   `json:"dataRestoreCommand,omitempty"`
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
	RecoveryTargetSettings  *RecoveryTargetSettings  `json:"recoveryTargetSettings,omitempty"`
}

// ExistingConfig configures initialization from an existing keeper.
type ExistingConfig struct {
	KeeperUID string `json:"keeperUID,omitempty"`
}

// StandbyConfig configures the cluster when its role is standby.
type StandbyConfig struct {
	StandbySettings         *StandbySettings         `json:"standbySettings,omitempty"`
	ArchiveRecoverySettings *ArchiveRecoverySettings `json:"archiveRecoverySettings,omitempty"`
}

// ArchiveRecoverySettings defines the archive recovery settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/archive-recovery-settings.html )
type ArchiveRecoverySettings struct {
	// value for restore_command
	RestoreCommand string `json:"restoreCommand,omitempty"`
}

// RecoveryTargetSettings defines the recovery target settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/recovery-target-settings.html )
type RecoveryTargetSettings struct {
	RecoveryTarget         string `json:"recoveryTarget,omitempty"`
	RecoveryTargetLsn      string `json:"recoveryTargetLsn,omitempty"`
	RecoveryTargetName     string `json:"recoveryTargetName,omitempty"`
	RecoveryTargetTime     string `json:"recoveryTargetTime,omitempty"`
	RecoveryTargetXid      string `json:"recoveryTargetXid,omitempty"`
	RecoveryTargetTimeline string `json:"recoveryTargetTimeline,omitempty"`
}

// StandbySettings defines the standby settings in the recovery.conf file (https://www.postgresql.org/docs/9.6/static/standby-settings.html )
type StandbySettings struct {
	PrimaryConninfo       string `json:"primaryConninfo,omitempty"`
	PrimarySlotName       string `json:"primarySlotName,omitempty"`
	RecoveryMinApplyDelay string `json:"recoveryMinApplyDelay,omitempty"`
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
	// Interval after which a dead keeper will be removed from the cluster data
	DeadKeeperRemovalInterval *Duration `json:"deadKeeperRemovalInterval,omitempty"`
	// Interval to wait before next proxy check
	ProxyCheckInterval *Duration `json:"proxyCheckInterval,omitempty"`
	// Interval where the proxy must successfully complete a check
	ProxyTimeout *Duration `json:"proxyTimeout,omitempty"`
	// Max number of standbys. This needs to be greater enough to cover both
	// standby managed by stolon and additional standbys configured by the
	// user. Its value affect different postgres parameters like
	// max_replication_slots and max_wal_senders. Setting this to a number
	// lower than the sum of stolon managed standbys and user managed
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
	// addition to the ones internally defined by stolon
	AdditionalWalSenders *uint16 `json:"additionalWalSenders"`
	// AdditionalMasterReplicationSlots defines additional replication slots to
	// be created on the master postgres instance. Replication slots not defined
	// here will be dropped from the master instance (i.e. manually created
	// replication slots will be removed).
	AdditionalMasterReplicationSlots []string `json:"additionalMasterReplicationSlots"`
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
	// Additional pg_hba.conf entries
	// we don't set omitempty since we want to distinguish between null or empty slice
	PGHBA []string `json:"pgHBA"`
	// Enable automatic pg restart when pg parameters that requires restart changes
	AutomaticPgRestart *bool `json:"automaticPgRestart"`
}

// ClusterStatus is the observed cluster status.
type ClusterStatus struct { //nolint:revive
	CurrentGeneration int64        `json:"currentGeneration,omitempty"`
	Phase             ClusterPhase `json:"phase,omitempty"`
	// Master DB UID
	Master string `json:"master,omitempty"`
}

// Cluster is the top-level cluster object in cluster data.
type Cluster struct {
	UID        string    `json:"uid,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	ChangeTime time.Time `json:"changeTime,omitzero"`

	Spec *ClusterSpec `json:"spec,omitempty"`

	Status ClusterStatus `json:"status,omitzero"`
}

// DeepCopy returns an independent copy of the cluster.
func (c *Cluster) DeepCopy() *Cluster {
	nc, err := copystructure.Copy(c)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(c, nc) {
		panic("not equal")
	}
	return nc.(*Cluster)
}

// DeepCopy returns an independent copy of the cluster spec.
func (c *ClusterSpec) DeepCopy() *ClusterSpec {
	nc, err := copystructure.Copy(c)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(c, nc) {
		panic("not equal")
	}
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

func validateReplicationSlot(replicationSlot string) error {
	if !util.IsValidReplSlotName(replicationSlot) {
		return fmt.Errorf("wrong replication slot name: %q", replicationSlot)
	}
	if common.IsStolonName(replicationSlot) {
		return fmt.Errorf("replication slot name is reserved: %q", replicationSlot)
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
	Healthy         bool      `json:"healthy,omitempty"`
	LastHealthyTime time.Time `json:"lastHealthyTime,omitzero"`

	BootUUID string `json:"bootUUID,omitempty"`

	PostgresBinaryVersion PostgresBinaryVersion `json:"postgresBinaryVersion,omitzero"`

	ForceFail bool `json:"forceFail,omitempty"`

	CanBeMaster             *bool `json:"canBeMaster,omitempty"`
	CanBeSynchronousReplica *bool `json:"canBeSynchronousReplica,omitempty"`
}

// Keeper is a keeper object in cluster data.
type Keeper struct {
	// Keeper ID
	UID        string    `json:"uid,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	ChangeTime time.Time `json:"changeTime,omitzero"`

	Spec *KeeperSpec `json:"spec,omitempty"`

	Status KeeperStatus `json:"status,omitzero"`
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
	// The KeeperUID this db is assigned to
	KeeperUID string `json:"keeperUID,omitempty"`
	// Time after which any request (keepers checks from sentinel etc...) will fail.
	RequestTimeout Duration `json:"requestTimeout,omitzero"`
	// See ClusterSpec MaxStandbys description
	MaxStandbys uint16 `json:"maxStandbys,omitempty"`
	// Use Synchronous replication between master and its standbys
	SynchronousReplication bool `json:"synchronousReplication,omitempty"`
	// Whether to use pg_rewind
	UsePgrewind bool `json:"usePgrewind,omitempty"`
	// AdditionalWalSenders defines the number of additional wal_senders in
	// addition to the ones internally defined by stolon
	AdditionalWalSenders uint16 `json:"additionalWalSenders"`
	// AdditionalReplicationSlots is a list of additional replication slots.
	// Replication slots not defined here will be dropped from the instance
	// (i.e. manually created replication slots will be removed).
	AdditionalReplicationSlots []string `json:"additionalReplicationSlots"`
	// InitMode defines the db initialization mode. Current modes are: none, new
	InitMode DBInitMode `json:"initMode,omitempty"`
	// Init configuration used when InitMode is "new"
	NewConfig *NewConfig `json:"newConfig,omitempty"`
	// Point in time recovery init configuration used when InitMode is "pitr"
	PITRConfig *PITRConfig `json:"pitrConfig,omitempty"`
	// Map of postgres parameters
	PGParameters PGParameters `json:"pgParameters,omitempty"`
	// Additional pg_hba.conf entries
	// We don't set omitempty since we want to distinguish between null or empty slice
	PGHBA []string `json:"pgHBA"`
	// DB Role (master or standby)
	Role common.Role `json:"role,omitempty"`
	// FollowConfig when Role is "standby"
	FollowConfig *FollowConfig `json:"followConfig,omitempty"`
	// Followers DB UIDs
	Followers []string `json:"followers"`
	// Whether to include previous postgresql.conf
	IncludeConfig bool `json:"includePreviousConfig,omitempty"`
	// SynchronousStandbys are the standbys to be configured as synchronous
	SynchronousStandbys []string `json:"synchronousStandbys"`
	// External SynchronousStandbys are external standbys names to be configured as synchronous
	ExternalSynchronousStandbys []string `json:"externalSynchronousStandbys"`
}

// DBStatus is the observed database status.
type DBStatus struct {
	Healthy bool `json:"healthy,omitempty"`

	CurrentGeneration int64 `json:"currentGeneration,omitempty"`

	ListenAddress string `json:"listenAddress,omitempty"`
	Port          string `json:"port,omitempty"`

	SystemID         string                   `json:"systemdID,omitempty"`
	TimelineID       uint64                   `json:"timelineID,omitempty"`
	XLogPos          uint64                   `json:"xLogPos,omitempty"`
	TimelinesHistory PostgresTimelinesHistory `json:"timelinesHistory,omitempty"`

	PGParameters PGParameters `json:"pgParameters,omitempty"`

	// DBUIDs of the internal standbys currently reported as in sync by the instance
	CurSynchronousStandbys []string `json:"-"`

	// DBUIDs of the internal standbys that we know are in sync.
	// They could be currently down but we know that they were reported as in
	// sync in the past and they are defined inside synchronous_standby_names
	// so the instance will wait for acknowledge from them.
	SynchronousStandbys []string `json:"synchronousStandbys"`

	// NOTE(sgotti) we currently don't report the external synchronous standbys.
	// If/when needed lets add a new ExternalSynchronousStandbys field

	OlderWalFile string `json:"olderWalFile,omitempty"`
}

// DB is a database object in cluster data.
type DB struct {
	UID        string    `json:"uid,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	ChangeTime time.Time `json:"changeTime,omitzero"`

	Spec *DBSpec `json:"spec,omitempty"`

	Status DBStatus `json:"status,omitzero"`
}

// ProxySpec is the desired proxy configuration.
type ProxySpec struct {
	MasterDBUID    string   `json:"masterDbUid,omitempty"`
	EnabledProxies []string `json:"enabledProxies,omitempty"`
}

// ProxyStatus is the observed proxy status.
type ProxyStatus struct {
}

// Proxy is a proxy object in cluster data.
type Proxy struct {
	UID        string    `json:"uid,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	ChangeTime time.Time `json:"changeTime,omitzero"`

	Spec ProxySpec `json:"spec,omitzero"`

	Status ProxyStatus `json:"status,omitzero"`
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

// UnmarshalJSON decodes Duration from a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	du, err := time.ParseDuration(s)
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
	// ClusterData format version. Used to detect incompatible
	// version and do upgrade. Needs to be bumped when a non
	// backward compatible change is done to the other struct
	// members.
	FormatVersion uint64    `json:"formatVersion"`
	ChangeTime    time.Time `json:"changeTime"`
	Cluster       *Cluster  `json:"cluster"`
	Keepers       Keepers   `json:"keepers"`
	DBs           DBs       `json:"dbs"`
	Proxy         *Proxy    `json:"proxy"`
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
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(c, nc) {
		panic("not equal")
	}
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
