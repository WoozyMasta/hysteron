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

// Bootstrap/recovery contract types for cluster and database initialization.

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
