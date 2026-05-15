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

import runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"

type runConfig struct {
	PG postgresOptions `group:"PostgreSQL" namespace:"pg" env-namespace:"PG"`

	UID     string `short:"i" long:"uid" env:"UID" long-alias:"id" description:"keeper uid (must be unique in the cluster and can contain only lower-case letters, numbers and the underscore character). If not provided a random uid will be generated."`
	DataDir string `short:"d" long:"data-dir" env:"DATA_DIR" description:"data directory"`
	runtimecommon.CommonConfig

	CanBeMaster             bool `long:"can-be-master" env:"CAN_BE_MASTER" description:"allow keeper to be elected as master (default true)"`
	CanBeSynchronousReplica bool `long:"can-be-synchronous-replica" env:"CAN_BE_SYNCHRONOUS_REPLICA" description:"allow keeper to be chosen as synchronous replica (default true)"`
	DisableDataDirLocking   bool `long:"disable-data-dir-locking" env:"DISABLE_DATA_DIR_LOCKING" description:"disable locking on data dir. Warning! It'll cause data corruptions if two keepers are concurrently running with the same data dir."`
	AllowNewerPG            bool `long:"allow-newer-postgres-version" env:"ALLOW_NEWER_POSTGRES_VERSION" description:"allow running with PostgreSQL major versions newer than the highest default-supported major. Older-than-supported versions are always rejected."`
}

// postgresOptions groups PostgreSQL connection settings managed by the keeper.
// The group namespaces produce flags like `--pg-listen-address`
// and env vars like `PG_LISTEN_ADDRESS`.
type postgresOptions struct {
	Repl             postgresReplOptions `group:"PostgreSQL Replication User" namespace:"repl" env-namespace:"REPL"`
	SU               postgresSUOptions   `group:"PostgreSQL Superuser" namespace:"su" env-namespace:"SU"`
	ListenAddress    string              `long:"listen-address" env:"LISTEN_ADDRESS" description:"postgresql instance listening address, local address used for the postgres instance. For all network interface, you can set the value to '*'."`
	AdvertiseAddress string              `long:"advertise-address" env:"ADVERTISE_ADDRESS" description:"postgresql instance address from outside. Use it to expose ip different than local ip with a NAT networking config"`
	Port             string              `long:"port" env:"PORT" description:"postgresql instance listening port" short:"p" default:"5432"`
	AdvertisePort    string              `long:"advertise-port" env:"ADVERTISE_PORT" description:"postgresql instance port from outside. Use it to expose port different than local port with a PAT networking config"`
	BinPath          string              `long:"bin-path" env:"BIN_PATH" description:"absolute path to postgresql binaries. If empty they will be searched in the current PATH"`
	WALDir           string              `long:"wal-dir" env:"WAL_DIR" description:"postgresql WAL directory (optional, useful when WAL is on a separate disk)"`
	TablespaceDirs   []string            `long:"tablespace-dir" env:"TABLESPACE_DIR" description:"managed PostgreSQL tablespace directory root; only directories under these roots can be cleaned during reinit/resync"`
}

// postgresReplOptions configures the postgres replication user.
type postgresReplOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;scram-sha-256;trust" default:"md5" description:"postgres replication user auth method"`
	Username     string `long:"username"     env:"USERNAME"     description:"postgres replication user name. Required. It'll be created on db initialization. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"     description:"postgres replication user password. Mutually exclusive with --pg-repl-passwordfile. Must be the same for all keepers."  xor:"pg-repl-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE" description:"postgres replication user password file. Mutually exclusive with --pg-repl-password. Must be the same for all keepers." xor:"pg-repl-secret"`
}

// postgresSUOptions configures the postgres superuser.
type postgresSUOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;scram-sha-256;trust" default:"md5" description:"postgres superuser auth method"`
	Username     string `long:"username"     env:"USERNAME"     description:"postgres superuser user name. Defaults to the effective user running keeper. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"     description:"postgres superuser password. Mutually exclusive with --pg-su-passwordfile. Must be the same for all keepers."   xor:"pg-su-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE" description:"postgres superuser password file. Mutually exclusive with --pg-su-password. Must be the same for all keepers."  xor:"pg-su-secret"`
}

// Defaults that cannot be expressed as struct tags (booleans default to `true` for our use case).
var cfg = runConfig{
	CanBeMaster:             true,
	CanBeSynchronousReplica: true,
}
