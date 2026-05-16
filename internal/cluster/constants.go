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

import "time"

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
	// DefaultUnsafeAutoFailback controls unsafe automatic switchback behavior.
	DefaultUnsafeAutoFailback = false
	// DefaultAutoFailbackMinUptime is the default minimum healthy time before
	// unsafe auto-failback may switch.
	DefaultAutoFailbackMinUptime = 60 * time.Second
	// DefaultAutoFailbackCooldown is the default minimum interval between unsafe
	// auto-failback switches.
	DefaultAutoFailbackCooldown = 5 * time.Minute
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
