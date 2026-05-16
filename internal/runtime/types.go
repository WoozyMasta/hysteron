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

package runtime

import stconfig "github.com/woozymasta/hysteron/internal/config"

// Target identifies a runtime component and selected backend.
type Target struct {
	CommonConfig *stconfig.CommonConfig
	Sentinel     *SentinelOptions
	Proxy        *ProxyOptions
	Keeper       *KeeperOptions
	Backend      string
	Component    string
}

// SentinelOptions configures sentinel runtime options from unified CLI.
type SentinelOptions struct {
	InitialClusterSpecFile string

	WebListenAddress string
	WebBasePath      string
	WebAuthUsername  string
	WebAuthPassword  string
	WebReadTimeout   string
	WebWriteTimeout  string
	ClusterSpecFiles []string

	WebUnsafeNoAuth bool
}

// ProxyOptions configures proxy runtime options from unified CLI.
type ProxyOptions struct {
	ListenAddress string
	Port          string

	ReadOnlyListenAddress   string
	ReadOnlyPort            string
	ReadOnlyReplicaPriority string
	ReadOnlyMaxLagBytes     uint64
	ReadOnlyNoFallback      bool
	ReadOnlyIncludePrimary  bool

	DisableWritableListener bool
}

// KeeperOptions configures keeper runtime options from unified CLI.
type KeeperOptions struct {
	UID     string
	DataDir string

	PG KeeperPostgresOptions

	MasterPriority int

	CanBeMaster             bool
	CanBeSynchronousReplica bool
	DisableDataDirLocking   bool
	AllowNewerPG            bool
}

// KeeperPostgresOptions contains keeper-managed PostgreSQL settings.
type KeeperPostgresOptions struct {
	Repl             KeeperPostgresReplOptions
	SU               KeeperPostgresSUOptions
	ListenAddress    string
	AdvertiseAddress string
	Port             string
	AdvertisePort    string
	BinPath          string
	WALDir           string
	TablespaceDirs   []string
}

// KeeperPostgresReplOptions configures replication user settings.
type KeeperPostgresReplOptions struct {
	AuthMethod   string
	Username     string
	Password     string
	PasswordFile string
}

// KeeperPostgresSUOptions configures superuser settings.
type KeeperPostgresSUOptions struct {
	AuthMethod   string
	Username     string
	Password     string
	PasswordFile string
}
