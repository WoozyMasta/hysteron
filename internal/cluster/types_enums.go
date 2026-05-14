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

// Enumerated cluster contract types and pointer helpers.

// FollowType identifies how a standby follows its upstream source.
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
