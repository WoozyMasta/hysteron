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

import "errors"

var (
	// ErrNoClusterData reports missing cluster data in the configured store.
	ErrNoClusterData = errors.New("no cluster data available")
	// ErrNoClusterSpec reports missing cluster spec in current cluster data.
	ErrNoClusterSpec = errors.New("no cluster spec available")
	// ErrConfirmationRequired reports a mutating action that requires explicit
	// confirmation.
	ErrConfirmationRequired = errors.New("confirmation required: pass --yes")
	// ErrKeeperUIDRequired reports a missing keeper UID parameter.
	ErrKeeperUIDRequired = errors.New("keeper uid required")
	// ErrKeeperNotFound reports unknown keeper UID in cluster data.
	ErrKeeperNotFound = errors.New("keeper doesn't exist")
	// ErrKeeperAssignedMaster reports keeper assigned to current master DB.
	ErrKeeperAssignedMaster = errors.New("keeper assigned db is the current cluster master db")
	// ErrClusterDataOverwriteRequiresYes reports overwrite attempt without
	// explicit confirmation.
	ErrClusterDataOverwriteRequiresYes = errors.New("cluster data already available: pass --yes to overwrite")
	// ErrPatchDataRequired reports missing patch payload.
	ErrPatchDataRequired = errors.New("patch data required")
	// ErrClusterSpecDataRequired reports missing cluster specification payload.
	ErrClusterSpecDataRequired = errors.New("cluster specification data required")
	// ErrNoMasterDatabase reports missing current master DB in cluster status.
	ErrNoMasterDatabase = errors.New("no master database available")
	// ErrNoMasterKeeper reports unresolved keeper for the current master DB.
	ErrNoMasterKeeper = errors.New("no master keeper available")
	// ErrClusterPaused reports that cluster mutations are blocked by pause mode.
	ErrClusterPaused = errors.New("cluster is paused; resume first")
	// ErrManualSwitchTargetKeeperRequired reports missing target keeper uid.
	ErrManualSwitchTargetKeeperRequired = errors.New("target keeper uid required")
	// ErrManualSwitchTargetKeeperNotFound reports unknown target keeper.
	ErrManualSwitchTargetKeeperNotFound = errors.New("target keeper not found")
)
