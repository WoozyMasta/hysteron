// Copyright 2015 Sorint.lab
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

package keeper

import (
	"sync"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/postgresql"
	"github.com/woozymasta/hysteron/internal/store"
)

// PostgresKeeper reconciles local PostgreSQL state with cluster data.
type PostgresKeeper struct {
	// First-observed timestamp of current DCS degraded window.
	dcsDegradedSince time.Time
	// External cluster store client.
	e store.Store
	// Parsed keeper command configuration.
	cfg *runConfig
	// PostgreSQL process manager.
	pgm *postgresql.Manager
	// Fatal background errors channel.
	end chan error
	// Injectable PostgreSQL binary version reader (tests may override).
	pgBinaryVersion func() (int, int, error)
	// Persisted keeper identity/cluster binding state.
	keeperLocalState *LocalState
	// Persisted local database assignment state.
	dbLocalState *DBLocalState
	// Last PostgreSQL state published to cluster data.
	lastPGState *cluster.PostgresState
	// Advertised capability: eligible for master role.
	canBeMaster *bool
	// Advertised capability: eligible for synchronous replica role.
	canBeSynchronousReplica *bool
	// Per-slot retry-after map for standby logical-slot advance failures.
	logicalSlotStandbyAdvanceRetryAfter map[string]time.Time
	// Pending async standby logical-slot advance operations keyed by slot/database.
	logicalSlotAdvancePending map[string]queuedLogicalSlotAdvanceOperation
	// Wake-up channel for async standby logical-slot advance worker.
	logicalSlotAdvanceNotify chan struct{}
	// Keeper process boot identifier.
	bootUUID string
	// Absolute keeper data directory path.
	dataDir string
	// PostgreSQL listen address.
	pgListenAddress string
	// Address advertised to other components.
	pgAdvertiseAddress string
	// PostgreSQL listen port.
	pgPort string
	// Port advertised to other components.
	pgAdvertisePort string
	// PostgreSQL binaries directory path.
	pgBinPath string
	// PostgreSQL WAL directory path (optional external path).
	pgWALDir string
	// Replication user auth method.
	pgReplAuthMethod string
	// Replication user name.
	pgReplUsername string
	// Replication user password.
	pgReplPassword string
	// Superuser auth method.
	pgSUAuthMethod string
	// Superuser name.
	pgSUUsername string
	// Superuser password.
	pgSUPassword string
	// Last emitted standby logical-slot readiness signature for warning dedup.
	logicalSlotReadinessLast string
	// Current keeper-local failsafe state (scaffold only, no behavior change).
	failsafeState failsafeState
	// Managed tablespace directory roots (cleanup is limited to these paths).
	pgTablespaceDirs []string
	// Main reconciliation loop sleep interval.
	sleepInterval time.Duration
	// Timeout for store and PostgreSQL requests.
	requestTimeout time.Duration
	// Delay before retrying failed standby logical-slot advance operations.
	logicalSlotStandbyAdvanceRetryDelay time.Duration
	// Runtime-configured probe interval for failsafe mode.
	failsafeProbeInterval time.Duration
	// Runtime-configured probe timeout for failsafe mode.
	failsafeProbeTimeout time.Duration
	// Runtime-configured maximum failsafe active window.
	failsafeTTL time.Duration
	// Guards keeperLocalState/dbLocalState access.
	localStateMutex sync.Mutex
	// Guards lastPGState and pgManager state transitions.
	pgStateMutex sync.Mutex
	// Serializes expensive PG state collection.
	getPGStateMutex sync.Mutex
	// Serialized state for async standby logical-slot advance queue and retries.
	logicalSlotAdvanceMutex sync.Mutex
	// Runtime-configured allowed missing peer probes in failsafe mode.
	failsafeMaxMissingPeers uint16
	// Enables waiting for synchronous standbys before promotion flow completion.
	waitSyncStandbysSynced bool
	// Emits one-time warning when experimental logical slot failover gate is enabled.
	logicalSlotGateNoticeEmitted bool
	// Emits one-time warning when gate is enabled on PG versions without native logical failover slots.
	logicalSlotLegacyModeNoticeEmitted bool
	// Emits one-time info when native PG17+ logical failover slot mode is active.
	logicalSlotNativeModeNoticeEmitted bool
	// Emits one-time warning when standby logical-slot advance path is unavailable.
	logicalSlotStandbyAdvanceUnavailableNoticeEmitted bool
	// Emits one-time warning when noStream disables standby logical-slot sync path.
	logicalSlotNoStreamNoticeEmitted bool
	// Runtime-configured failsafe mode flag from cluster spec.
	failsafeEnabled bool
	// Tracks whether DCS errors are currently observed.
	dcsDegraded bool
}
