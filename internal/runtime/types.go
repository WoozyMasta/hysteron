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
	Backend      string
	Component    string
	Sentinel     *SentinelOptions
	Proxy        *ProxyOptions
	Keeper       *KeeperOptions
}

// SentinelOptions configures sentinel runtime options from unified CLI.
type SentinelOptions struct {
	InitialClusterSpecFile string
	ClusterSpecFiles       []string

	WebListenAddress               string
	WebBasePath                    string
	WebAuthUsername                string
	WebAuthPassword                string
	WebReadTimeout                 string
	WebWriteTimeout                string
	WebAllowUnsafeAdminWithoutAuth bool
}

// ProxyOptions configures proxy runtime options from unified CLI.
type ProxyOptions struct {
	ListenAddress string
	Port          string

	DisableWritableListener bool

	ReadOnlyListenAddress string
	ReadOnlyPort          string
}

// KeeperOptions configures keeper runtime options from unified CLI.
type KeeperOptions struct {
	UID     string
	DataDir string

	CanBeMaster             bool
	CanBeSynchronousReplica bool
	DisableDataDirLocking   bool
	AllowNewerPG            bool

	PG KeeperPostgresOptions
}

// KeeperPostgresOptions contains keeper-managed PostgreSQL settings.
type KeeperPostgresOptions struct {
	ListenAddress    string
	AdvertiseAddress string
	Port             string
	AdvertisePort    string
	BinPath          string
	Repl             KeeperPostgresReplOptions
	SU               KeeperPostgresSUOptions
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
