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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/mitchellh/copystructure"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	slog "github.com/woozymasta/hysteron/internal/log"
	pg "github.com/woozymasta/hysteron/internal/postgresql"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/fs"
	"github.com/woozymasta/hysteron/internal/utils/id"
	"github.com/woozymasta/hysteron/internal/utils/osuser"
	slicesutil "github.com/woozymasta/hysteron/internal/utils/slices"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/woozymasta/flags"
)

// keeperRootLog is for helpers before a PostgresKeeper exists.
// Call only after runtimecommon.InitLogging so the root output/level are configured.
func keeperRootLog(cfg *config) *zerolog.Logger {
	var l zerolog.Logger
	if cfg == nil {
		l = slog.WithComponent("keeper")
	} else {
		l = slog.WithComponent("keeper").With().
			Str(slog.FieldClusterName, cfg.ClusterName()).
			Str(slog.FieldKeeperUID, cfg.UID).
			Logger()
	}
	return &l
}

const (
	maxPostgresTimelinesHistory = 2
	minWalKeepSegments          = 8
)

// LocalState is the keeper state persisted on local disk.
type LocalState struct {
	// UID is persistent keeper UID.
	UID string
	// ClusterUID is current cluster binding for this keeper.
	ClusterUID string
}

// DBLocalState is the local database state persisted by the keeper.
type DBLocalState struct {
	// InitPGParameters contains the postgres parameter after the
	// initialization
	InitPGParameters common.Parameters
	// UID is persistent DB UID assigned to this keeper.
	UID string
	// Generation is desired DB generation persisted locally.
	Generation int64
	// Initializing registers when the db is initializing. Needed to detect
	// when the initialization has failed.
	Initializing bool
}

// DeepCopy returns an independent copy of the local database state.
func (s *DBLocalState) DeepCopy() *DBLocalState {
	if s == nil {
		return nil
	}
	ns, err := copystructure.Copy(s)
	common.MustNot(err, "keeper DBLocalState deep copy failed")
	return ns.(*DBLocalState)
}

type config struct {
	PG postgresOptions `group:"PostgreSQL" namespace:"pg" env-namespace:"PG"`

	UID     string `short:"i" long:"uid"      env:"UID"      long-alias:"id" description:"keeper uid (must be unique in the cluster and can contain only lower-case letters, numbers and the underscore character). If not provided a random uid will be generated."`
	DataDir string `short:"d" long:"data-dir" env:"DATA_DIR"                 description:"data directory"`
	runtimecommon.CommonConfig

	CanBeMaster             bool `long:"can-be-master"                      env:"CAN_BE_MASTER"                      description:"allow keeper to be elected as master (default true)"`
	CanBeSynchronousReplica bool `long:"can-be-synchronous-replica"         env:"CAN_BE_SYNCHRONOUS_REPLICA"         description:"allow keeper to be chosen as synchronous replica (default true)"`
	DisableDataDirLocking   bool `long:"disable-data-dir-locking"           env:"DISABLE_DATA_DIR_LOCKING"           description:"disable locking on data dir. Warning! It'll cause data corruptions if two keepers are concurrently running with the same data dir."`
	AllowNewerPG            bool `long:"allow-newer-postgres-version"       env:"ALLOW_NEWER_POSTGRES_VERSION"       description:"allow running with PostgreSQL major versions newer than the highest default-supported major. Older-than-supported versions are always rejected."`
}

// postgresOptions groups PostgreSQL connection settings managed by the
// keeper. The group namespaces produce flags like `--pg-listen-address`
// and env vars like `PG_LISTEN_ADDRESS`.
type postgresOptions struct {
	ListenAddress    string `long:"listen-address"    env:"LISTEN_ADDRESS"    description:"postgresql instance listening address, local address used for the postgres instance. For all network interface, you can set the value to '*'."`
	AdvertiseAddress string `long:"advertise-address" env:"ADVERTISE_ADDRESS" description:"postgresql instance address from outside. Use it to expose ip different than local ip with a NAT networking config"`
	Port             string `long:"port"              env:"PORT"              description:"postgresql instance listening port"                                                                                                            short:"p" default:"5432"`
	AdvertisePort    string `long:"advertise-port"    env:"ADVERTISE_PORT"    description:"postgresql instance port from outside. Use it to expose port different than local port with a PAT networking config"`
	BinPath          string `long:"bin-path"          env:"BIN_PATH"          description:"absolute path to postgresql binaries. If empty they will be searched in the current PATH"`

	Repl postgresReplOptions `group:"PostgreSQL Replication User" namespace:"repl" env-namespace:"REPL"`
	SU   postgresSUOptions   `group:"PostgreSQL Superuser"        namespace:"su"   env-namespace:"SU"`
}

// postgresReplOptions configures the postgres replication user.
type postgresReplOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;trust" default:"md5" description:"postgres replication user auth method"`
	Username     string `long:"username"     env:"USERNAME"                                       description:"postgres replication user name. Required. It'll be created on db initialization. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"                                       description:"postgres replication user password. Mutually exclusive with --pg-repl-passwordfile. Must be the same for all keepers."  xor:"pg-repl-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE"                                   description:"postgres replication user password file. Mutually exclusive with --pg-repl-password. Must be the same for all keepers." xor:"pg-repl-secret"`
}

// postgresSUOptions configures the postgres superuser.
type postgresSUOptions struct {
	AuthMethod   string `long:"auth-method"  env:"AUTH_METHOD"  choices:"md5;trust" default:"md5" description:"postgres superuser auth method"`
	Username     string `long:"username"     env:"USERNAME"                                       description:"postgres superuser user name. Defaults to the effective user running keeper. Must be the same for all keepers."`
	Password     string `long:"password"     env:"PASSWORD"                                       description:"postgres superuser password. Mutually exclusive with --pg-su-passwordfile. Must be the same for all keepers."          xor:"pg-su-secret"`
	PasswordFile string `long:"passwordfile" env:"PASSWORDFILE"                                   description:"postgres superuser password file. Mutually exclusive with --pg-su-password. Must be the same for all keepers."         xor:"pg-su-secret"`
}

// Defaults that cannot be expressed as struct tags (booleans default to
// `true` for our use case).
var cfg = config{
	CanBeMaster:             true,
	CanBeSynchronousReplica: true,
}

var managedPGParameters = []string{
	"unix_socket_directories",
	"wal_keep_segments",
	"wal_keep_size",
	"hot_standby",
	"listen_addresses",
	"port",
	"max_replication_slots",
	"max_wal_senders",
	"wal_log_hints",
	"synchronous_standby_names",

	// parameters moved from recovery.conf to postgresql.conf in PostgresSQL 12
	"primary_conninfo",
	"primary_slot_name",
	"recovery_min_apply_delay",
	"restore_command",
	"recovery_target_timeline",
	"recovery_target",
	"recovery_target_lsn",
	"recovery_target_name",
	"recovery_target_time",
	"recovery_target_xid",
	"recovery_target_timeline",
	"recovery_target_action",
}

func readPasswordFromFile(filepath string) (string, error) {
	fi, err := os.Lstat(filepath)
	if err != nil {
		return "", fmt.Errorf(
			"unable to read password from file %s: %v",
			filepath,
			err,
		)
	}

	if fi.Mode() > 0600 {
		// Keep warning-only behavior for now: some Kubernetes volume projections
		// use file modes more permissive than 0600.
		keeperRootLog(nil).Warn().
			Str("file", filepath).
			Str("mode", fmt.Sprintf("%#o", fi.Mode())).
			Msg("password file mode is more permissive than 0600; tighten permissions in production")
	}

	pwBytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf(
			"unable to read password from file %s: %v",
			filepath,
			err,
		)
	}
	return string(pwBytes), nil
}

// walLevel returns wal_level value to use.
// If user provided wal_level is "logical" return it, otherwise use "replica".
func (p *PostgresKeeper) walLevel(db *cluster.DB) string {
	var additionalValidWalLevels = []string{
		"logical",
	}
	walLevel := "replica"

	if db.Spec.PGParameters != nil {
		if l, ok := db.Spec.PGParameters["wal_level"]; ok {
			if slices.Contains(additionalValidWalLevels, l) {
				walLevel = l
			}
		}
	}

	return walLevel
}

func (p *PostgresKeeper) walKeepSegments(db *cluster.DB) int {
	walKeepSegments := minWalKeepSegments
	if db.Spec.PGParameters != nil {
		if v, ok := db.Spec.PGParameters["wal_keep_segments"]; ok {
			// ignore wrong wal_keep_segments values
			if configuredWalKeepSegments, err := strconv.Atoi(v); err == nil {
				if configuredWalKeepSegments > walKeepSegments {
					walKeepSegments = configuredWalKeepSegments
				}
			}
		}
	}

	return walKeepSegments
}

func (p *PostgresKeeper) walKeepSize(db *cluster.DB) string {
	// Assume default PostgreSQL segment size unless explicit segment-size support
	// is introduced in cluster contracts.
	minMiB := uint64(minWalKeepSegments) * uint64(pg.WalSegSize/(1024*1024))
	defaultWalKeepSize := strconv.FormatUint(minMiB, 10) + "MB"
	if db.Spec.PGParameters != nil {
		if v, ok := db.Spec.PGParameters["wal_keep_size"]; ok {
			sizeBytes, err := parseWalKeepSizeBytes(v)
			if err != nil {
				p.baseLog().Warn().
					Str("wal_keep_size", v).
					Err(err).
					Msg("invalid wal_keep_size, using minimum safe value")
				return defaultWalKeepSize
			}
			minBytes := minMiB * 1024 * 1024
			if sizeBytes < minBytes {
				p.baseLog().Warn().
					Str("wal_keep_size", v).
					Uint64("minimum_bytes", minBytes).
					Msg("wal_keep_size below minimum safe value, clamping")
				return defaultWalKeepSize
			}
			return strings.TrimSpace(v)
		}
	}

	return defaultWalKeepSize
}

func parseWalKeepSizeBytes(value string) (uint64, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return 0, errors.New("empty wal_keep_size")
	}

	i := 0
	for i < len(v) && unicode.IsDigit(rune(v[i])) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid wal_keep_size %q", value)
	}

	n, err := strconv.ParseUint(v[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse wal_keep_size value: %w", err)
	}

	unit := strings.TrimSpace(v[i:])
	if unit == "" {
		// PostgreSQL wal_keep_size defaults to MB when no unit is specified.
		return n * 1024 * 1024, nil
	}

	switch strings.ToUpper(unit) {
	case "B":
		return n, nil
	case "KB":
		return n * 1024, nil
	case "MB":
		return n * 1024 * 1024, nil
	case "GB":
		return n * 1024 * 1024 * 1024, nil
	case "TB":
		return n * 1024 * 1024 * 1024 * 1024, nil
	}

	return 0, fmt.Errorf("unsupported wal_keep_size unit %q", unit)
}

func (p *PostgresKeeper) binaryVersion() (int, int, error) {
	if p.pgBinaryVersion != nil {
		return p.pgBinaryVersion()
	}
	return p.pgm.BinaryVersion()
}

func (p *PostgresKeeper) mandatoryPGParameters(
	db *cluster.DB,
) common.Parameters {
	params := common.Parameters{
		"unix_socket_directories": common.PgUnixSocketDirectories,
		"wal_level":               p.walLevel(db),
		"hot_standby":             "on",
	}

	maj, _, err := p.binaryVersion()
	if err != nil {
		// in case we fail to parse the binary version don't return any wal_keep_segments or wal_keep_size
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
		return params
	}

	if maj >= 13 {
		params["wal_keep_size"] = p.walKeepSize(db)
	} else {
		params["wal_keep_segments"] = strconv.Itoa(p.walKeepSegments(db))
	}

	return params
}

func (p *PostgresKeeper) getSUConnParams(
	db, followedDB *cluster.DB,
) pg.ConnParams {
	cp := pg.ConnParams{
		"user":             p.pgSUUsername,
		"host":             followedDB.Status.ListenAddress,
		"port":             followedDB.Status.Port,
		"application_name": common.HysteronName(db.UID),
		"dbname":           "postgres",
		// prefer ssl if available (already the default for postgres libpq but not for golang lib pq)
		"sslmode": "prefer",
	}
	if p.pgSUAuthMethod != "trust" {
		cp.Set("password", p.pgSUPassword)
	}
	return cp
}

func (p *PostgresKeeper) getReplConnParams(
	db, followedDB *cluster.DB,
) pg.ConnParams {
	cp := pg.ConnParams{
		"user":             p.pgReplUsername,
		"host":             followedDB.Status.ListenAddress,
		"port":             followedDB.Status.Port,
		"application_name": common.HysteronName(db.UID),
		// prefer ssl if available (already the default for postgres libpq but not for golang lib pq)
		"sslmode": "prefer",
	}
	if p.pgReplAuthMethod != "trust" {
		cp.Set("password", p.pgReplPassword)
	}
	return cp
}

func (p *PostgresKeeper) getLocalConnParams() pg.ConnParams {
	cp := pg.ConnParams{
		"user":   p.pgSUUsername,
		"host":   common.PgUnixSocketDirectories,
		"port":   p.pgPort,
		"dbname": "postgres",
		// no sslmode defined since it's not needed and supported over unix sockets
	}
	if p.pgSUAuthMethod != "trust" {
		cp.Set("password", p.pgSUPassword)
	}
	return cp
}

func (p *PostgresKeeper) getLocalReplConnParams() pg.ConnParams {
	cp := pg.ConnParams{
		"user":     p.pgReplUsername,
		"password": p.pgReplPassword,
		"host":     common.PgUnixSocketDirectories,
		"port":     p.pgPort,
		// no sslmode defined since it's not needed and supported over unix sockets
	}
	if p.pgReplAuthMethod != "trust" {
		cp.Set("password", p.pgReplPassword)
	}
	return cp
}

func (p *PostgresKeeper) createPGParameters(
	db *cluster.DB,
) common.Parameters {
	parameters := common.Parameters{}

	// Include init parameters if include config is required
	dbls := p.dbLocalStateCopy()
	if db.Spec.IncludeConfig {
		maps.Copy(parameters, dbls.InitPGParameters)
	}

	// Copy user defined pg parameters
	maps.Copy(parameters, db.Spec.PGParameters)

	// Add/Replace mandatory PGParameters
	maps.Copy(parameters, p.mandatoryPGParameters(db))
	enforceHotStandbyFeedbackForLogicalSlotFailover(
		parameters,
		db.Spec.EnableLogicalSlotFailover,
	)

	parameters["listen_addresses"] = p.pgListenAddress

	parameters["port"] = p.pgPort
	desiredMaxReplSlots := int(db.Spec.MaxStandbys)
	if current, ok := p.currentPGParameterInt("max_replication_slots"); ok && current > desiredMaxReplSlots {
		desiredMaxReplSlots = current
	}
	parameters["max_replication_slots"] = strconv.Itoa(desiredMaxReplSlots)

	// Add some more wal senders, since also the keeper will use them
	desiredMaxWalSenders := int((db.Spec.MaxStandbys * 2) + 2 + db.Spec.AdditionalWalSenders)
	if current, ok := p.currentPGParameterInt("max_wal_senders"); ok && current > desiredMaxWalSenders {
		desiredMaxWalSenders = current
	}
	parameters["max_wal_senders"] = strconv.Itoa(desiredMaxWalSenders)

	// required by pg_rewind (if data checksum is enabled it's ignored)
	if db.Spec.UsePgrewind {
		parameters["wal_log_hints"] = "on"
	}

	// Setup synchronous replication
	if db.Spec.SynchronousReplication &&
		(len(db.Spec.SynchronousStandbys) > 0 || len(db.Spec.ExternalSynchronousStandbys) > 0) {
		synchronousStandbys := make(
			[]string,
			0,
			len(
				db.Spec.SynchronousStandbys,
			)+len(
				db.Spec.ExternalSynchronousStandbys,
			),
		)
		for _, synchronousStandby := range db.Spec.SynchronousStandbys {
			synchronousStandbys = append(
				synchronousStandbys,
				common.HysteronName(synchronousStandby),
			)
		}
		synchronousStandbys = append(
			synchronousStandbys,
			db.Spec.ExternalSynchronousStandbys...)

		// We deliberately don't use postgres FIRST or ANY methods with N
		// different than len(synchronousStandbys) because we need that all the
		// defined standbys are synchronous (so just only one failed standby
		// will block the primary).
		// This is needed for consistency. If we have 3 standbys and we use
		// FIRST 2 (a, b, c), the sentinel, when the master fails, won't be able to know
		// which of the 3 standbys is really synchronous and in sync with the
		// master. And choosing the non synchronous one will cause the loss of
		// the transactions contained in the wal records not transmitted.
		if len(synchronousStandbys) > 1 {
			parameters["synchronous_standby_names"] = fmt.Sprintf(
				"%d (%s)",
				len(synchronousStandbys),
				strings.Join(synchronousStandbys, ","),
			)
		} else {
			parameters["synchronous_standby_names"] = strings.Join(synchronousStandbys, ",")
		}
	} else {
		parameters["synchronous_standby_names"] = ""
	}

	return parameters
}

func (p *PostgresKeeper) createRecoveryOptions(
	recoveryMode pg.RecoveryMode,
	standbySettings *cluster.StandbySettings,
	archiveRecoverySettings *cluster.ArchiveRecoverySettings,
	recoveryTargetSettings *cluster.RecoveryTargetSettings,
) *pg.RecoveryOptions {
	parameters := common.Parameters{}

	if standbySettings != nil {
		if standbySettings.PrimaryConninfo != "" {
			parameters["primary_conninfo"] = standbySettings.PrimaryConninfo
		}
		if standbySettings.PrimarySlotName != "" {
			parameters["primary_slot_name"] = standbySettings.PrimarySlotName
		}
		if standbySettings.RecoveryMinApplyDelay != "" {
			parameters["recovery_min_apply_delay"] = standbySettings.RecoveryMinApplyDelay
		}
	}

	if archiveRecoverySettings != nil {
		parameters["restore_command"] = archiveRecoverySettings.RestoreCommand
	}

	parameters["recovery_target_timeline"] = "latest"
	if recoveryTargetSettings == nil {
		return &pg.RecoveryOptions{
			RecoveryMode:       recoveryMode,
			RecoveryParameters: parameters,
		}
	}

	if recoveryTargetSettings.RecoveryTargetTimeline != "" {
		parameters["recovery_target_timeline"] = recoveryTargetSettings.RecoveryTargetTimeline
	}
	if recoveryTargetSettings.RecoveryTarget != "" {
		parameters["recovery_target"] = recoveryTargetSettings.RecoveryTarget
	}
	if recoveryTargetSettings.RecoveryTargetLsn != "" {
		parameters["recovery_target_lsn"] = recoveryTargetSettings.RecoveryTargetLsn
	}
	if recoveryTargetSettings.RecoveryTargetName != "" {
		parameters["recovery_target_name"] = recoveryTargetSettings.RecoveryTargetName
	}
	if recoveryTargetSettings.RecoveryTargetTime != "" {
		parameters["recovery_target_time"] = recoveryTargetSettings.RecoveryTargetTime
	}
	if recoveryTargetSettings.RecoveryTargetXid != "" {
		parameters["recovery_target_xid"] = recoveryTargetSettings.RecoveryTargetXid
	}
	if hasRecoveryTargetSelector(recoveryTargetSettings) {
		parameters["recovery_target_action"] = "promote"
	}

	return &pg.RecoveryOptions{
		RecoveryMode:       recoveryMode,
		RecoveryParameters: parameters,
	}
}

func hasRecoveryTargetSelector(recoveryTargetSettings *cluster.RecoveryTargetSettings) bool {
	if recoveryTargetSettings == nil {
		return false
	}
	for _, value := range []string{
		recoveryTargetSettings.RecoveryTarget,
		recoveryTargetSettings.RecoveryTargetLsn,
		recoveryTargetSettings.RecoveryTargetName,
		recoveryTargetSettings.RecoveryTargetTime,
		recoveryTargetSettings.RecoveryTargetXid,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

// PostgresKeeper reconciles local PostgreSQL state with cluster data.
type PostgresKeeper struct {
	// External cluster store client.
	e store.Store
	// Parsed keeper command configuration.
	cfg *config
	// PostgreSQL process manager.
	pgm *pg.Manager
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

	// Main reconciliation loop sleep interval.
	sleepInterval time.Duration
	// Timeout for store and PostgreSQL requests.
	requestTimeout time.Duration

	// Guards keeperLocalState/dbLocalState access.
	localStateMutex sync.Mutex
	// Guards lastPGState and pgm state transitions.
	pgStateMutex sync.Mutex
	// Serializes expensive PG state collection.
	getPGStateMutex sync.Mutex

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
	// Per-slot retry-after map for standby logical-slot advance failures.
	logicalSlotStandbyAdvanceRetryAfter map[string]time.Time
	// Delay before retrying failed standby logical-slot advance operations.
	logicalSlotStandbyAdvanceRetryDelay time.Duration
}

type standbyReplayController interface {
	IsWALReplayPaused() (bool, error)
	ResumeWALReplay() error
}

// baseLog is the structured logger with stable keeper identity (use for all
// PostgresKeeper events).
func (p *PostgresKeeper) baseLog() *zerolog.Logger {
	uid := ""
	if p.keeperLocalState != nil {
		uid = p.keeperLocalState.UID
	}
	l := slog.L().With().
		Str(slog.FieldComponent, "keeper").
		Str(slog.FieldClusterName, p.cfg.ClusterName()).
		Str(slog.FieldKeeperUID, uid).
		Str("boot_uuid", p.bootUUID).
		Logger()
	return &l
}

// NewPostgresKeeper creates a PostgreSQL keeper from command configuration.
func NewPostgresKeeper(
	cfg *config,
	end chan error,
) (*PostgresKeeper, error) {
	e, err := runtimecommon.NewStore(&cfg.CommonConfig, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	// Clean and get absolute datadir path
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot get absolute datadir path for %q: %v",
			cfg.DataDir,
			err,
		)
	}

	p := &PostgresKeeper{
		cfg: cfg,

		bootUUID: id.UUID(),

		dataDir: dataDir,

		pgListenAddress:    cfg.PG.ListenAddress,
		pgAdvertiseAddress: cfg.PG.AdvertiseAddress,
		pgPort:             cfg.PG.Port,
		pgAdvertisePort:    cfg.PG.AdvertisePort,
		pgBinPath:          cfg.PG.BinPath,
		pgReplAuthMethod:   cfg.PG.Repl.AuthMethod,
		pgReplUsername:     cfg.PG.Repl.Username,
		pgReplPassword:     cfg.PG.Repl.Password,
		pgSUAuthMethod:     cfg.PG.SU.AuthMethod,
		pgSUUsername:       cfg.PG.SU.Username,
		pgSUPassword:       cfg.PG.SU.Password,

		sleepInterval:  cluster.DefaultSleepInterval,
		requestTimeout: cluster.DefaultRequestTimeout,

		keeperLocalState: &LocalState{},
		dbLocalState:     &DBLocalState{},

		canBeMaster:             &cfg.CanBeMaster,
		canBeSynchronousReplica: &cfg.CanBeSynchronousReplica,

		e:   e,
		end: end,

		logicalSlotStandbyAdvanceRetryAfter: make(map[string]time.Time),
		logicalSlotStandbyAdvanceRetryDelay: 10 * time.Second,
	}

	err = p.loadKeeperLocalState()
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"failed to load keeper local state file: %v",
			err,
		)
	}
	if p.keeperLocalState.UID != "" && p.cfg.UID != "" &&
		p.keeperLocalState.UID != p.cfg.UID {
		return nil, fmt.Errorf(
			"refusing to start: uid in local state file %q does not match --uid %q",
			p.keeperLocalState.UID,
			cfg.UID,
		)
	}
	if p.keeperLocalState.UID == "" {
		p.keeperLocalState.UID = cfg.UID
		if cfg.UID == "" {
			p.keeperLocalState.UID = id.UID()
			p.baseLog().
				Info().
				Str(slog.FieldKeeperUID, p.keeperLocalState.UID).
				Msg("generated a new keeper UID (none was configured on the command line)")
		}
		if err = p.saveKeeperLocalState(); err != nil {
			return nil, fmt.Errorf("could not write keeper local state file: %w", err)
		}
	}

	p.baseLog().
		Info().
		Str(slog.FieldKeeperUID, p.keeperLocalState.UID).
		Msg("keeper identity loaded; continuing startup")

	err = p.loadDBLocalState()
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"failed to load db local state file: %v",
			err,
		)
	}
	return p, nil
}

func (p *PostgresKeeper) dbLocalStateCopy() *DBLocalState {
	p.localStateMutex.Lock()
	defer p.localStateMutex.Unlock()
	return p.dbLocalState.DeepCopy()
}

func (p *PostgresKeeper) usePgrewind(db *cluster.DB) bool {
	return p.pgSUUsername != "" && p.pgSUPassword != "" &&
		db.Spec.UsePgrewind
}

type pgrewindDecision struct {
	walCheckErr error
	reason      string
	requiredWal string
	olderWal    string
	try         bool
}

const (
	pgrewindReasonNotInitialized = "not_initialized"
	pgrewindReasonSystemIDDiff   = "system_id_mismatch"
	pgrewindReasonNoMaster       = "no_master"
	pgrewindReasonWalCheckErr    = "wal_check_error"
	pgrewindReasonWalMissing     = "required_wal_missing"
	pgrewindReasonAllowed        = "allowed"
)

func evaluatePgrewindDecision(
	initialized bool,
	localSystemID string,
	followedSystemID string,
	hasMaster bool,
	dbXLogPos uint64,
	masterOlderWal string,
) pgrewindDecision {
	if !initialized {
		return pgrewindDecision{reason: pgrewindReasonNotInitialized}
	}
	if localSystemID != followedSystemID {
		return pgrewindDecision{reason: pgrewindReasonSystemIDDiff}
	}
	if !hasMaster {
		return pgrewindDecision{reason: pgrewindReasonNoMaster}
	}

	walAvailable, walErr := pg.IsRequiredWalAvailable(
		dbXLogPos,
		masterOlderWal,
		pg.WalSegSize,
	)
	if walErr != nil {
		// Keep warning-only behavior: inability to verify WAL availability
		// should not disable pg_rewind path.
		return pgrewindDecision{
			try:         true,
			reason:      pgrewindReasonWalCheckErr,
			walCheckErr: walErr,
		}
	}
	if !walAvailable {
		requiredWal := pg.XlogPosToWalFileNameNoTimeline(dbXLogPos, pg.WalSegSize)
		olderWal, _ := pg.WalFileNameNoTimeLine(masterOlderWal)
		return pgrewindDecision{
			try:         false,
			reason:      pgrewindReasonWalMissing,
			requiredWal: requiredWal,
			olderWal:    olderWal,
		}
	}

	return pgrewindDecision{try: true, reason: pgrewindReasonAllowed}
}

func (p *PostgresKeeper) updateKeeperInfo() error {
	p.localStateMutex.Lock()
	keeperUID := p.keeperLocalState.UID
	clusterUID := p.keeperLocalState.ClusterUID
	p.localStateMutex.Unlock()

	if clusterUID == "" {
		return nil
	}

	major, minor, err := p.binaryVersion()
	if err != nil {
		// in case we fail to parse the binary version then log it and just report maj and min as 0
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
	}

	keeperInfo := &cluster.KeeperInfo{
		InfoUID:    id.UID(),
		UID:        keeperUID,
		ClusterUID: clusterUID,
		BootUUID:   p.bootUUID,
		PostgresBinaryVersion: cluster.PostgresBinaryVersion{
			Maj: major,
			Min: minor,
		},
		PostgresState: p.getLastPGState(),

		CanBeMaster:             p.canBeMaster,
		CanBeSynchronousReplica: p.canBeSynchronousReplica,
	}

	// The time to live is just to automatically remove old entries, it's
	// not used to determine if the keeper info has been updated.
	if err := p.e.SetKeeperInfo(context.TODO(), keeperUID, keeperInfo, p.sleepInterval); err != nil {
		return err
	}
	return nil
}

func (p *PostgresKeeper) updatePGState(pctx context.Context) {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	pgState, err := p.GetPGState(pctx)
	if err != nil {
		p.baseLog().Error().Err(err).Msg("failed to get pg state")
	}
	p.lastPGState = pgState
}

func (p *PostgresKeeper) validatePostgresVersion() error {
	major, minor, err := p.binaryVersion()
	if err != nil {
		return fmt.Errorf(
			"failed to get postgres binary version: %w",
			err,
		)
	}
	if err := pg.ValidateKnownMajorVersion(major); err != nil {
		if p.cfg.AllowNewerPG && major > pg.MaxKnownMajorVersion() {
			p.baseLog().Warn().
				Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
				Str("supported_major_versions", pg.SupportedMajorVersionsString()).
				Str("legacy_major_versions", pg.SupportedLegacyMajorVersionsString()).
				Msg("newer unsupported PostgreSQL version allowed by configuration")
			return nil
		}
		return fmt.Errorf(
			"%w; use --allow-newer-postgres-version only for newer majors than %d",
			err,
			pg.MaxKnownMajorVersion(),
		)
	}
	if pg.IsLegacySupportedMajorVersion(major) {
		p.baseLog().Warn().
			Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
			Str("supported_major_versions", pg.SupportedMajorVersionsString()).
			Str("legacy_major_versions", pg.SupportedLegacyMajorVersionsString()).
			Msg("PostgreSQL major version is legacy best-effort; behavior is not guaranteed")
		return nil
	}
	p.baseLog().
		Info().
		Str("pg_version", fmt.Sprintf("%d.%d", major, minor)).
		Msg("PostgreSQL version is supported")
	return nil
}

// GetInSyncStandbys returns Hysteron standby UIDs currently reported as synchronous.
func (p *PostgresKeeper) GetInSyncStandbys() ([]string, error) {
	inSyncStandbysFullName, err := p.pgm.GetSyncStandbys()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to retrieve current sync standbys status from instance: %v",
			err,
		)
	}

	inSyncStandbys := []string{}
	for _, s := range inSyncStandbysFullName {
		if common.IsHysteronName(s) {
			inSyncStandbys = append(
				inSyncStandbys,
				common.NameFromHysteronName(s),
			)
		}
	}

	return inSyncStandbys, nil
}

// GetPGState returns the current PostgreSQL state observed by the keeper.
func (p *PostgresKeeper) GetPGState(
	_ context.Context,
) (*cluster.PostgresState, error) {
	p.getPGStateMutex.Lock()
	defer p.getPGStateMutex.Unlock()
	// Just get one pgstate at a time to avoid exausting available connections
	pgState := &cluster.PostgresState{}

	dbls := p.dbLocalStateCopy()
	pgState.UID = dbls.UID
	pgState.Generation = dbls.Generation

	pgState.ListenAddress = p.pgAdvertiseAddress
	pgState.Port = p.pgAdvertisePort

	initialized, err := p.pgm.IsInitialized()
	if err != nil {
		return pgState, err
	}
	if initialized {
		pgParameters, err := p.pgm.GetConfigFilePGParameters()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("cannot get configured pg parameters")
			return pgState, nil
		}
		filteredPGParameters := common.Parameters{}
		for k, v := range pgParameters {
			if !slices.Contains(managedPGParameters, k) {
				filteredPGParameters[k] = v
			}
		}
		pgnames := make([]string, 0, len(filteredPGParameters))
		for k := range filteredPGParameters {
			pgnames = append(pgnames, k)
		}
		sort.Strings(pgnames)
		p.baseLog().Debug().
			Int("total_parameter_count", len(pgParameters)).
			Int("user_parameter_count", len(filteredPGParameters)).
			Strs("user_parameter_names", pgnames).
			Msg("PostgreSQL parameters from instance config (names only)")
		pgState.PGParameters = filteredPGParameters

		inSyncStandbys, err := p.GetInSyncStandbys()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve current in sync standbys from instance")
			return pgState, nil
		}

		pgState.SynchronousStandbys = inSyncStandbys

		sd, err := p.pgm.GetSystemData()
		if err != nil {
			p.baseLog().Error().Err(err).Msg("error getting pg state")
			return pgState, nil
		}
		pgState.SystemID = sd.SystemID
		pgState.TimelineID = sd.TimelineID
		pgState.XLogPos = sd.XLogPos

		ctlsh, err := getTimeLinesHistory(
			pgState,
			p.pgm,
			maxPostgresTimelinesHistory,
			p.baseLog(),
		)
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("error getting timeline history")
			return pgState, nil
		}
		pgState.TimelinesHistory = ctlsh

		ow, err := p.pgm.OlderWalFile()
		if err != nil {
			p.baseLog().
				Warn().
				Err(err).
				Msg("error getting older wal file")
		} else {
			p.baseLog().Debug().Str("filename", ow).Msg("older wal file")
			pgState.OlderWalFile = ow
		}
		pgState.Healthy = true
		role, roleErr := p.pgm.GetRole()
		if roleErr != nil {
			p.baseLog().Debug().Err(roleErr).Msg("failed to get PostgreSQL role for logical slot state publish")
		} else if role == common.RoleMaster {
			logicalSlots, slotsErr := p.pgm.GetLogicalReplicationSlots()
			if slotsErr != nil {
				p.baseLog().Debug().Err(slotsErr).Msg("failed to inspect logical replication slots for state publish")
			} else {
				pgState.ManagedLogicalSlots = logicalSlotLSNMap(logicalSlots)
			}
		}
	}

	return pgState, nil
}

func getTimeLinesHistory(
	pgState *cluster.PostgresState,
	pgm pg.PGManager,
	maxPostgresTimelinesHistory int,
	lg *zerolog.Logger,
) (cluster.PostgresTimelinesHistory, error) {
	ctlsh := cluster.PostgresTimelinesHistory{}
	// if timeline <= 1 then no timeline history file exists.
	if pgState.TimelineID > 1 {
		var tlsh []*pg.TimelineHistory
		tlsh, err := pgm.GetTimelinesHistory(pgState.TimelineID)
		if err != nil {
			lg.Error().
				Err(err).
				Uint64("timeline_id", pgState.TimelineID).
				Msg("could not read timeline history from PostgreSQL")
			return ctlsh, err
		}
		if len(tlsh) > maxPostgresTimelinesHistory {
			tlsh = tlsh[len(tlsh)-maxPostgresTimelinesHistory:]
		}
		for _, tlh := range tlsh {
			ctlh := &cluster.PostgresTimelineHistory{
				TimelineID:  tlh.TimelineID,
				SwitchPoint: tlh.SwitchPoint,
				Reason:      tlh.Reason,
			}
			ctlsh = append(ctlsh, ctlh)
		}
	}
	return ctlsh, nil
}

func (p *PostgresKeeper) getLastPGState() *cluster.PostgresState {
	p.pgStateMutex.Lock()
	pgState := p.lastPGState.DeepCopy()
	p.pgStateMutex.Unlock()
	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryPostgresState(pgState)).
		Msg("PostgreSQL state snapshot from last publish")
	return pgState
}

func (p *PostgresKeeper) currentPGParameterInt(name string) (int, bool) {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	if p.lastPGState == nil || p.lastPGState.PGParameters == nil {
		return 0, false
	}
	raw, ok := p.lastPGState.PGParameters[name]
	if !ok || raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (p *PostgresKeeper) applyRuntimeConfigFromClusterData(cd *cluster.ClusterData) {
	if cd == nil || cd.Cluster == nil {
		return
	}
	spec := cd.Cluster.DefSpec()
	newSleepInterval := spec.SleepInterval.Duration
	newRequestTimeout := spec.RequestTimeout.Duration
	if p.sleepInterval != newSleepInterval {
		p.baseLog().Info().
			Dur("sleep_interval_old", p.sleepInterval).
			Dur("sleep_interval_new", newSleepInterval).
			Msg("updating keeper sleep interval from cluster spec")
		p.sleepInterval = newSleepInterval
	}
	if p.requestTimeout != newRequestTimeout {
		p.baseLog().Info().
			Dur("request_timeout_old", p.requestTimeout).
			Dur("request_timeout_new", newRequestTimeout).
			Msg("updating keeper request timeout from cluster spec")
		p.requestTimeout = newRequestTimeout
		if p.pgm != nil {
			p.pgm.SetRequestTimeout(newRequestTimeout)
		}
	}
}

// Start runs keeper reconciliation loops until the context is canceled.
func (p *PostgresKeeper) Start(ctx context.Context) {
	endSMCh := make(chan struct{})
	endPgStatecheckerCh := make(chan struct{})
	endUpdateKeeperInfo := make(chan struct{})

	var err error
	var cd *cluster.ClusterData
	cd, _, err = p.e.GetClusterData(context.TODO())
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("error retrieving cluster data")
	} else if cd != nil {
		if cd.FormatVersion != cluster.CurrentCDFormatVersion {
			p.baseLog().
				Error().
				Uint64("version", cd.FormatVersion).
				Msg("unsupported clusterdata format version")
		}
		p.applyRuntimeConfigFromClusterData(cd)
	}

	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data snapshot at keeper start")

	pgm := pg.NewManager(
		p.pgBinPath,
		p.dataDir,
		p.getLocalConnParams(),
		p.getLocalReplConnParams(),
		p.pgSUAuthMethod,
		p.pgSUUsername,
		p.pgSUPassword,
		p.pgReplAuthMethod,
		p.pgReplUsername,
		p.pgReplPassword,
		p.requestTimeout,
	)
	p.pgm = pgm
	p.pgBinaryVersion = pgm.BinaryVersion

	if err = p.validatePostgresVersion(); err != nil {
		p.end <- err
		return
	}

	_ = p.pgm.StopIfStarted(true)

	smTimerCh := time.NewTimer(0).C
	updatePGStateTimerCh := time.NewTimer(0).C
	updateKeeperInfoTimerCh := time.NewTimer(0).C
	for {
		// The sleepInterval can be updated during normal execution. Ensure we regularly
		// refresh the metric to account for those changes.
		sleepInterval.Set(float64(p.sleepInterval / time.Second))

		select {
		case <-ctx.Done():
			p.baseLog().Debug().Msg("shutting down keeper")
			if err = p.pgm.StopIfStarted(true); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
			}
			p.end <- nil
			return

		case <-smTimerCh:
			go func() {
				p.postgresKeeperSM(ctx)
				endSMCh <- struct{}{}
			}()

		case <-endSMCh:
			smTimerCh = time.NewTimer(p.sleepInterval).C

		case <-updatePGStateTimerCh:
			// updateKeeperInfo two times faster than the sleep interval
			go func() {
				p.updatePGState(ctx)
				endPgStatecheckerCh <- struct{}{}
			}()

		case <-endPgStatecheckerCh:
			// updateKeeperInfo two times faster than the sleep interval
			updatePGStateTimerCh = time.NewTimer(p.sleepInterval / 2).C

		case <-updateKeeperInfoTimerCh:
			go func() {
				if err := p.updateKeeperInfo(); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to update keeper info")
				}
				endUpdateKeeperInfo <- struct{}{}
			}()

		case <-endUpdateKeeperInfo:
			updateKeeperInfoTimerCh = time.NewTimer(p.sleepInterval).C
		}
	}
}

func (p *PostgresKeeper) resync(
	db, masterDB, followedDB *cluster.DB,
	tryPgrewind bool,
) error {
	pgm := p.pgm
	replConnParams := p.getReplConnParams(db, followedDB)
	standbySettings := &cluster.StandbySettings{
		PrimaryConninfo: replConnParams.ConnString(),
		PrimarySlotName: common.HysteronName(db.UID),
	}

	// We intentionally do not hard-fail on pg_rewind capability checks here.
	// If pg_rewind is missing or unusable, SyncFromFollowedPGRewind returns an
	// error and we fall back to pg_basebackup.
	if tryPgrewind && p.usePgrewind(db) {
		// pg_rewind doesn't support running against a database that is in recovery, as it
		// builds temporary tables and this is not supported on a hot-standby. Hysteron doesn't
		// currently support cascading replication, but we should be clear when issuing a
		// rewind that it targets the current primary, rather than whatever database we
		// follow.
		connParams := p.getSUConnParams(db, masterDB)
		p.baseLog().Info().
			Str(slog.FieldDBUID, masterDB.UID).
			Str(slog.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Msg("attempting pg_rewind against current primary to sync data directory")
		if err := pgm.SyncFromFollowedPGRewind(connParams, p.pgSUPassword); err != nil {
			// log pg_rewind error and fallback to pg_basebackup
			p.baseLog().
				Error().
				Err(err).
				Msg("error syncing with pg_rewind")
		} else {
			pgm.SetRecoveryOptions(p.createRecoveryOptions(pg.RecoveryModeStandby, standbySettings, nil, nil))
			return nil
		}
	}

	_, _, err := p.binaryVersion()
	if err != nil {
		// in case we fail to parse the binary version then log it and just don't use replSlot
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
	}
	replSlot := common.HysteronName(db.UID)

	if err := pgm.RemoveAll(); err != nil {
		return fmt.Errorf(
			"failed to remove the postgres data dir: %v",
			err,
		)
	}
	if slog.IsDebug() {
		p.baseLog().Debug().
			Str(slog.FieldDBUID, followedDB.UID).
			Str(slog.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Str("repl_conn_params", fmt.Sprintf("%v", replConnParams)).
			Msg("starting base backup / clone from followed PostgreSQL instance")
	} else {
		p.baseLog().Info().
			Str(slog.FieldDBUID, followedDB.UID).
			Str(slog.FieldKeeperUID, followedDB.Spec.KeeperUID).
			Msg("starting base backup / clone from followed PostgreSQL instance")
	}

	if err := pgm.SyncFromFollowed(replConnParams, replSlot); err != nil {
		return fmt.Errorf("sync error: %v", err)
	}
	p.baseLog().
		Info().
		Str(slog.FieldDBUID, followedDB.UID).
		Msg("successfully cloned data directory from followed instance")

	return nil
}

func (p *PostgresKeeper) isDifferentTimelineBranch(
	followedDB *cluster.DB,
	pgState *cluster.PostgresState,
) bool {
	res := cluster.DetectTimelineBranchDivergence(
		followedDB.Status.TimelineID,
		followedDB.Status.TimelinesHistory,
		followedDB.Status.XLogPos,
		pgState.TimelineID,
		pgState.TimelinesHistory,
		pgState.XLogPos,
	)
	if !res.Different {
		return false
	}

	switch res.Reason {
	case cluster.TimelineDivergenceFollowedTimelineOlder:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("timeline", pgState.TimelineID).
			Msg("followed instance timeline < than our timeline")
	case cluster.TimelineDivergenceSameTimelineDifferentSwitchPoint:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("followedXlogpos", res.FollowedSwitchPoint).
			Interface("timeline", pgState.TimelineID).
			Interface("xlogpos", res.CurrentSwitchPoint).
			Msg("followed instance timeline forked at a different xlog pos than our timeline")
	case cluster.TimelineDivergenceFollowedForkedBeforeCurrentPosition:
		p.baseLog().Info().
			Interface("followedTimeline", followedDB.Status.TimelineID).
			Interface("followedXlogpos", res.FollowedSwitchPoint).
			Interface("timeline", pgState.TimelineID).
			Interface("xlogpos", res.CurrentSwitchPoint).
			Msg("followed instance timeline forked before our current state")
	}
	return true
}

func (p *PostgresKeeper) updateReplSlots(
	curReplSlots []string,
	uid string,
	followersUIDs, additionalReplSlots, ignoredReplSlots []string,
	memberSlotTTL time.Duration,
	orphanMemberSlots map[string]time.Time,
	physicalSlotState map[string]pg.PhysicalReplicationSlot,
	knownDBUIDs map[string]struct{},
) error {
	internalReplSlots, ignoredSlots := managedReplicationSlots(
		uid,
		followersUIDs,
		additionalReplSlots,
		ignoredReplSlots,
	)

	// Drop internal replication slots
	for _, slot := range curReplSlots {
		if !common.IsHysteronName(slot) {
			continue
		}
		if _, ignored := ignoredSlots[slot]; ignored {
			continue
		}
		if _, ok := internalReplSlots[slot]; !ok {
			shouldDrop, reason := shouldDropUnmanagedHysteronSlot(
				slot,
				memberSlotTTL,
				orphanMemberSlots,
				physicalSlotState,
				knownDBUIDs,
				time.Now(),
			)
			if !shouldDrop {
				p.baseLog().
					Debug().
					Str("slot", slot).
					Str("reason", reason).
					Msg("skipping replication slot drop")
				continue
			}
			p.baseLog().
				Info().
				Str("slot", slot).
				Msg("dropping replication slot")
			if err := p.pgm.DropReplicationSlot(slot); err != nil {
				p.baseLog().
					Error().
					Str("slot", slot).
					Err(err).
					Msg("failed to drop replication slot")

				// don't return the error but continue also if drop failed (standby still connected)
			}
		}
	}

	// Create internal replication slots
	for slot := range internalReplSlots {
		if !slices.Contains(curReplSlots, slot) {
			p.baseLog().
				Info().
				Str("slot", slot).
				Msg("creating replication slot")
			if err := p.pgm.CreateReplicationSlot(slot); err != nil {
				p.baseLog().
					Error().
					Str("slot", slot).
					Err(err).
					Msg("failed to create replication slot")
				return err
			}
		}
	}
	return nil
}

func managedReplicationSlots(
	uid string,
	followersUIDs, additionalReplSlots, ignoredReplSlots []string,
) (map[string]struct{}, map[string]struct{}) {
	internalReplSlots := map[string]struct{}{}
	ignoredSlots := map[string]struct{}{}

	for _, slot := range ignoredReplSlots {
		ignoredSlots[slot] = struct{}{}
	}

	// Create a list of the wanted internal replication slots.
	for _, followerUID := range followersUIDs {
		if followerUID == uid {
			continue
		}
		slot := common.HysteronName(followerUID)
		if _, ignored := ignoredSlots[slot]; ignored {
			continue
		}
		internalReplSlots[slot] = struct{}{}
	}

	// Add AdditionalReplicationSlots.
	for _, slot := range additionalReplSlots {
		hysteronSlot := common.HysteronName(slot)
		if _, ignored := ignoredSlots[hysteronSlot]; ignored {
			continue
		}
		internalReplSlots[hysteronSlot] = struct{}{}
	}

	return internalReplSlots, ignoredSlots
}

func shouldDropUnmanagedHysteronSlot(
	slot string,
	memberSlotTTL time.Duration,
	orphanMemberSlots map[string]time.Time,
	physicalSlotState map[string]pg.PhysicalReplicationSlot,
	knownDBUIDs map[string]struct{},
	now time.Time,
) (bool, string) {
	if memberSlotTTL <= 0 {
		return true, "ttl_disabled"
	}

	if common.IsHysteronName(slot) {
		slotUID := common.NameFromHysteronName(slot)
		if _, known := knownDBUIDs[slotUID]; known {
			if _, tracked := orphanMemberSlots[slot]; !tracked {
				return false, "awaiting_orphan_tracking"
			}
		}
	}

	orphanSince, orphanTracked := orphanMemberSlots[slot]
	if !orphanTracked {
		return true, "not_tracked_orphan"
	}
	if now.Sub(orphanSince) < memberSlotTTL {
		return false, "ttl_not_elapsed"
	}

	slotState, ok := physicalSlotState[slot]
	if !ok {
		return false, "slot_state_missing"
	}
	if slotState.Active {
		return false, "slot_active"
	}
	if slotState.HasXmin {
		return false, "slot_has_xmin"
	}

	return true, "ttl_elapsed"
}

func staleSlotsWithXmin(
	slots []pg.PhysicalReplicationSlot,
	managedSlots, ignoredSlots map[string]struct{},
) []string {
	stale := []string{}
	for _, slot := range slots {
		if !common.IsHysteronName(slot.Name) {
			continue
		}
		if slot.Active || !slot.HasXmin {
			continue
		}
		if _, ignored := ignoredSlots[slot.Name]; ignored {
			continue
		}
		if _, managed := managedSlots[slot.Name]; managed {
			continue
		}
		stale = append(stale, slot.Name)
	}
	slices.Sort(stale)
	return stale
}

type managedLogicalSlotsDecision struct {
	create   []cluster.ManagedLogicalReplicationSlot
	drop     []string
	mismatch []string
	active   []string
}

type managedLogicalSlotReadiness struct {
	missing  []string
	mismatch []string
}

type logicalSlotAdvanceOperation struct {
	Name      string
	Database  string
	TargetLSN uint64
}

func shouldEmitLogicalSlotGateNotice(enabled, alreadyEmitted bool) bool {
	return enabled && !alreadyEmitted
}

func managedLogicalSlotReadinessSignature(
	readiness managedLogicalSlotReadiness,
) string {
	parts := make([]string, 0, len(readiness.missing)+len(readiness.mismatch))
	for _, slot := range readiness.missing {
		parts = append(parts, "missing:"+slot)
	}
	for _, slot := range readiness.mismatch {
		parts = append(parts, "mismatch:"+slot)
	}
	if len(parts) == 0 {
		return ""
	}
	slices.Sort(parts)
	return strings.Join(parts, "|")
}

func shouldReconcileManagedLogicalSlots(
	desired []cluster.ManagedLogicalReplicationSlot,
	currentPGParameters cluster.PGParameters,
) (bool, string) {
	if len(desired) == 0 {
		return false, "not_configured"
	}
	walLevel := strings.ToLower(strings.TrimSpace(currentPGParameters["wal_level"]))
	if walLevel != "logical" {
		return false, "wal_level_not_logical"
	}
	return true, "enabled"
}

func shouldUseNativeLogicalSlotFailover(enableLogicalSlotFailover bool, pgMajor int) bool {
	return enableLogicalSlotFailover && pgMajor >= 17
}

func shouldUseStandbyLogicalSlotAdvance(enableLogicalSlotFailover bool, pgMajor int) bool {
	return enableLogicalSlotFailover && pgMajor >= 16
}

func logicalSlotAdvanceRetryKey(slotName, database string) string {
	return slotName + "@" + database
}

func shouldAttemptLogicalSlotAdvance(
	retryAfter map[string]time.Time,
	key string,
	now time.Time,
) bool {
	if retryAfter == nil {
		return true
	}
	next, ok := retryAfter[key]
	if !ok {
		return true
	}
	return !now.Before(next)
}

func markLogicalSlotAdvanceFailure(
	retryAfter map[string]time.Time,
	key string,
	now time.Time,
	retryDelay time.Duration,
) {
	if retryAfter == nil {
		return
	}
	retryAfter[key] = now.Add(retryDelay)
}

func clearLogicalSlotAdvanceFailure(
	retryAfter map[string]time.Time,
	key string,
) {
	if retryAfter == nil {
		return
	}
	delete(retryAfter, key)
}

func computeLogicalSlotAdvanceTarget(
	desiredLSN uint64,
	replayLSN uint64,
	currentConfirmedFlushLSN uint64,
) (uint64, bool) {
	if desiredLSN == 0 || replayLSN == 0 {
		return 0, false
	}
	target := desiredLSN
	if replayLSN < target {
		target = replayLSN
	}
	if target <= currentConfirmedFlushLSN {
		return 0, false
	}
	return target, true
}

func masterManagedLogicalSlotLSN(
	dbs cluster.DBs,
) map[string]uint64 {
	for _, db := range dbs {
		if db == nil || db.Spec == nil {
			continue
		}
		if db.Spec.Role != common.RoleMaster {
			continue
		}
		return db.Status.ManagedLogicalSlots
	}
	return nil
}

func logicalSlotLSNMap(
	current []pg.LogicalReplicationSlot,
) map[string]uint64 {
	if len(current) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(current))
	for _, slot := range current {
		out[slot.Name] = slot.ConfirmedFlushLSN
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func evaluateManagedLogicalSlotAdvanceOperations(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []pg.LogicalReplicationSlot,
	masterLSN map[string]uint64,
	replayLSN uint64,
) []logicalSlotAdvanceOperation {
	if len(desired) == 0 || len(current) == 0 || len(masterLSN) == 0 || replayLSN == 0 {
		return nil
	}
	currentByName := make(map[string]pg.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}
	ops := make([]logicalSlotAdvanceOperation, 0, len(desired))
	for _, desiredSlot := range desired {
		desiredLSN, ok := masterLSN[desiredSlot.Name]
		if !ok {
			continue
		}
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			continue
		}
		target, shouldAdvance := computeLogicalSlotAdvanceTarget(
			desiredLSN,
			replayLSN,
			currentSlot.ConfirmedFlushLSN,
		)
		if !shouldAdvance {
			continue
		}
		ops = append(ops, logicalSlotAdvanceOperation{
			Name:      desiredSlot.Name,
			Database:  desiredSlot.Database,
			TargetLSN: target,
		})
	}
	return ops
}

func enforceHotStandbyFeedbackForLogicalSlotFailover(
	parameters common.Parameters,
	enableLogicalSlotFailover bool,
) {
	if !enableLogicalSlotFailover {
		return
	}
	parameters["hot_standby_feedback"] = "on"
}

func evaluateManagedLogicalSlotsDecision(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []pg.LogicalReplicationSlot,
) managedLogicalSlotsDecision {
	decision := managedLogicalSlotsDecision{
		create:   make([]cluster.ManagedLogicalReplicationSlot, 0),
		drop:     make([]string, 0),
		mismatch: make([]string, 0),
		active:   make([]string, 0),
	}

	desiredByName := make(map[string]cluster.ManagedLogicalReplicationSlot, len(desired))
	for _, slot := range desired {
		desiredByName[slot.Name] = slot
	}

	currentByName := make(map[string]pg.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}

	for _, desiredSlot := range desired {
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			decision.create = append(decision.create, desiredSlot)
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			decision.mismatch = append(decision.mismatch, desiredSlot.Name)
		}
	}

	for _, currentSlot := range current {
		if _, ok := desiredByName[currentSlot.Name]; ok {
			continue
		}
		// Safety-first: clean up only reserved hysteron namespace slots.
		if !common.IsHysteronName(currentSlot.Name) {
			continue
		}
		if currentSlot.Active {
			decision.active = append(decision.active, currentSlot.Name)
			continue
		}
		decision.drop = append(decision.drop, currentSlot.Name)
	}

	slices.Sort(decision.mismatch)
	slices.Sort(decision.drop)
	slices.Sort(decision.active)
	return decision
}

func evaluateManagedLogicalSlotReadiness(
	desired []cluster.ManagedLogicalReplicationSlot,
	current []pg.LogicalReplicationSlot,
) managedLogicalSlotReadiness {
	readiness := managedLogicalSlotReadiness{
		missing:  make([]string, 0),
		mismatch: make([]string, 0),
	}

	currentByName := make(map[string]pg.LogicalReplicationSlot, len(current))
	for _, slot := range current {
		currentByName[slot.Name] = slot
	}

	for _, desiredSlot := range desired {
		currentSlot, ok := currentByName[desiredSlot.Name]
		if !ok {
			readiness.missing = append(readiness.missing, desiredSlot.Name)
			continue
		}
		if currentSlot.Database != desiredSlot.Database || currentSlot.Plugin != desiredSlot.Plugin {
			readiness.mismatch = append(readiness.mismatch, desiredSlot.Name)
		}
	}

	slices.Sort(readiness.missing)
	slices.Sort(readiness.mismatch)
	return readiness
}

func (p *PostgresKeeper) refreshReplicationSlots(
	cspec *cluster.ClusterSpec,
	db *cluster.DB,
	dbs cluster.DBs,
) error {
	var currentReplicationSlots []string
	currentReplicationSlots, err := p.pgm.GetReplicationSlots()
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("failed to get replication slots")
		return err
	}

	followersUIDs := db.Spec.Followers
	managedSlots, ignoredSlots := managedReplicationSlots(
		db.UID,
		followersUIDs,
		db.Spec.AdditionalReplicationSlots,
		db.Spec.IgnoreReplicationSlots,
	)
	physicalSlots, err := p.pgm.GetPhysicalReplicationSlots()
	if err != nil {
		p.baseLog().
			Debug().
			Err(err).
			Msg("failed to inspect physical replication slots")
		physicalSlots = nil
	}
	physicalSlotState := map[string]pg.PhysicalReplicationSlot{}
	for _, slot := range physicalSlots {
		physicalSlotState[slot.Name] = slot
	}
	memberSlotTTL := time.Duration(0)
	if cspec != nil && cspec.MemberReplicationSlotTTL != nil {
		memberSlotTTL = cspec.MemberReplicationSlotTTL.Duration
	}
	knownDBUIDs := map[string]struct{}{}
	for dbUID := range dbs {
		knownDBUIDs[dbUID] = struct{}{}
	}

	if err = p.updateReplSlots(
		currentReplicationSlots,
		db.UID,
		followersUIDs,
		db.Spec.AdditionalReplicationSlots,
		db.Spec.IgnoreReplicationSlots,
		memberSlotTTL,
		db.Status.OrphanMemberSlots,
		physicalSlotState,
		knownDBUIDs,
	); err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("error updating replication slots")
		return err
	}

	if stale := staleSlotsWithXmin(physicalSlots, managedSlots, ignoredSlots); len(stale) > 0 {
		p.baseLog().
			Warn().
			Strs("stale_slots", stale).
			Msg("detected inactive unmanaged hysteron physical slots with xmin; consider cleanup to avoid vacuum horizon retention")
	}

	reconcileLogicalSlots, reason := shouldReconcileManagedLogicalSlots(
		db.Spec.ManagedLogicalReplicationSlots,
		db.Status.PGParameters,
	)
	if shouldEmitLogicalSlotGateNotice(
		db.Spec.EnableLogicalSlotFailover,
		p.logicalSlotGateNoticeEmitted,
	) {
		p.baseLog().
			Warn().
			Msg("enableLogicalSlotFailover is experimental: standby path is readiness-only; no standby logical slot create/drop before promotion")
		p.logicalSlotGateNoticeEmitted = true
	}
	if !db.Spec.EnableLogicalSlotFailover {
		p.logicalSlotGateNoticeEmitted = false
	}
	if !reconcileLogicalSlots {
		if reason == "wal_level_not_logical" {
			p.baseLog().
				Warn().
				Str("wal_level", db.Status.PGParameters["wal_level"]).
				Msg("managed logical replication slots configured but wal_level is not logical; skipping logical slot reconcile")
		}
		return nil
	}

	currentLogicalSlots, err := p.pgm.GetLogicalReplicationSlots()
	if err != nil {
		p.baseLog().
			Debug().
			Err(err).
			Msg("failed to inspect logical replication slots")
		return nil
	}

	if db.Spec.Role != common.RoleMaster {
		if db.Spec.EnableLogicalSlotFailover {
			pgMajor, _, versionErr := p.pgm.BinaryVersion()
			if versionErr != nil {
				p.baseLog().
					Debug().
					Err(versionErr).
					Msg("failed to detect PostgreSQL binary version for standby logical-slot advance")
			}
			if versionErr == nil && shouldUseStandbyLogicalSlotAdvance(
				db.Spec.EnableLogicalSlotFailover,
				pgMajor,
			) {
				p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = false
				masterLSN := masterManagedLogicalSlotLSN(dbs)
				ops := evaluateManagedLogicalSlotAdvanceOperations(
					db.Spec.ManagedLogicalReplicationSlots,
					currentLogicalSlots,
					masterLSN,
					db.Status.XLogPos,
				)
				if len(ops) > 0 {
					p.baseLog().
						Debug().
						Int("advance_ops", len(ops)).
						Uint64("replay_lsn", db.Status.XLogPos).
						Msg("planned managed logical slot standby advance operations")
				}
				for _, op := range ops {
					now := time.Now()
					retryKey := logicalSlotAdvanceRetryKey(op.Name, op.Database)
					if !shouldAttemptLogicalSlotAdvance(
						p.logicalSlotStandbyAdvanceRetryAfter,
						retryKey,
						now,
					) {
						logicalSlotStandbyAdvanceSkippedBackoffTotal.Inc()
						continue
					}
					logicalSlotStandbyAdvanceAttemptsTotal.Inc()
					if err := p.pgm.AdvanceLogicalReplicationSlot(
						op.Name,
						op.Database,
						op.TargetLSN,
					); err != nil {
						markLogicalSlotAdvanceFailure(
							p.logicalSlotStandbyAdvanceRetryAfter,
							retryKey,
							now,
							p.logicalSlotStandbyAdvanceRetryDelay,
						)
						logicalSlotStandbyAdvanceRetrySlots.Set(
							float64(len(p.logicalSlotStandbyAdvanceRetryAfter)),
						)
						logicalSlotStandbyAdvanceFailuresTotal.Inc()
						p.baseLog().
							Warn().
							Err(err).
							Str("slot", op.Name).
							Uint64("desired_lsn", masterLSN[op.Name]).
							Uint64("replay_lsn", db.Status.XLogPos).
							Uint64("target_lsn", op.TargetLSN).
							Msg("failed to advance managed logical replication slot on standby")
						continue
					}
					clearLogicalSlotAdvanceFailure(
						p.logicalSlotStandbyAdvanceRetryAfter,
						retryKey,
					)
					logicalSlotStandbyAdvanceRetrySlots.Set(
						float64(len(p.logicalSlotStandbyAdvanceRetryAfter)),
					)
					logicalSlotStandbyAdvanceSuccessTotal.Inc()
				}
				logicalSlotStandbyAdvanceRetrySlots.Set(
					float64(len(p.logicalSlotStandbyAdvanceRetryAfter)),
				)
			} else if versionErr == nil && !p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted {
				p.baseLog().
					Warn().
					Int("pg_major", pgMajor).
					Msg("logical slot failover gate enabled but standby logical-slot advance is unavailable on PostgreSQL < 16")
				p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = true
			}

			readiness := evaluateManagedLogicalSlotReadiness(
				db.Spec.ManagedLogicalReplicationSlots,
				currentLogicalSlots,
			)
			currentSignature := managedLogicalSlotReadinessSignature(readiness)
			if currentSignature != p.logicalSlotReadinessLast {
				p.logicalSlotReadinessLast = currentSignature
				for _, slot := range readiness.missing {
					p.baseLog().
						Warn().
						Str("slot", slot).
						Msg("logical slot failover gate enabled: standby readiness missing managed logical slot")
				}
				for _, slot := range readiness.mismatch {
					p.baseLog().
						Warn().
						Str("slot", slot).
						Msg("logical slot failover gate enabled: standby logical slot mismatch")
				}
			}
		} else {
			p.logicalSlotReadinessLast = ""
			p.logicalSlotStandbyAdvanceUnavailableNoticeEmitted = false
		}
		return nil
	}
	p.logicalSlotReadinessLast = ""

	logicalDecision := evaluateManagedLogicalSlotsDecision(
		db.Spec.ManagedLogicalReplicationSlots,
		currentLogicalSlots,
	)
	createFailoverSlot := false
	if db.Spec.EnableLogicalSlotFailover {
		pgMajor, _, versionErr := p.pgm.BinaryVersion()
		if versionErr != nil {
			p.baseLog().
				Warn().
				Err(versionErr).
				Msg("failed to detect PostgreSQL binary version; creating logical slots without native failover flag")
		} else if shouldUseNativeLogicalSlotFailover(db.Spec.EnableLogicalSlotFailover, pgMajor) {
			createFailoverSlot = true
			if !p.logicalSlotNativeModeNoticeEmitted {
				p.baseLog().
					Info().
					Int("pg_major", pgMajor).
					Msg("logical slot failover gate enabled: using PostgreSQL native logical failover slots")
				p.logicalSlotNativeModeNoticeEmitted = true
			}
			p.logicalSlotLegacyModeNoticeEmitted = false
		} else {
			if !p.logicalSlotLegacyModeNoticeEmitted {
				p.baseLog().
					Warn().
					Int("pg_major", pgMajor).
					Msg("enableLogicalSlotFailover is enabled on PostgreSQL < 17; native logical slot failover is unavailable and behavior is experimental")
				p.logicalSlotLegacyModeNoticeEmitted = true
			}
			p.logicalSlotNativeModeNoticeEmitted = false
		}
	} else {
		p.logicalSlotLegacyModeNoticeEmitted = false
		p.logicalSlotNativeModeNoticeEmitted = false
	}
	for _, slot := range logicalDecision.mismatch {
		p.baseLog().
			Warn().
			Str("slot", slot).
			Msg("managed logical replication slot exists with different database or plugin; skipping destructive action")
	}
	for _, slot := range logicalDecision.active {
		p.baseLog().
			Warn().
			Str("slot", slot).
			Msg("logical replication slot scheduled for cleanup is active; skipping drop")
	}
	for _, desiredSlot := range logicalDecision.create {
		p.baseLog().Info().
			Str("slot", desiredSlot.Name).
			Str("database", desiredSlot.Database).
			Str("plugin", desiredSlot.Plugin).
			Msg("creating managed logical replication slot")
		if err := p.pgm.CreateLogicalReplicationSlot(
			desiredSlot.Name,
			desiredSlot.Database,
			desiredSlot.Plugin,
			createFailoverSlot,
		); err != nil {
			return fmt.Errorf(
				"failed to create managed logical replication slot %q: %w",
				desiredSlot.Name,
				err,
			)
		}
	}
	for _, slot := range logicalDecision.drop {
		p.baseLog().Info().
			Str("slot", slot).
			Msg("dropping unmanaged hysteron logical replication slot")
		if err := p.pgm.DropLogicalReplicationSlot(slot); err != nil {
			return fmt.Errorf("failed to drop logical replication slot %q: %w", slot, err)
		}
	}

	return nil
}

func (p *PostgresKeeper) postgresKeeperSM(pctx context.Context) {
	e := p.e
	pgm := p.pgm

	cd, _, err := e.GetClusterData(pctx)
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("error retrieving cluster data")
		return
	}
	p.baseLog().
		Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data snapshot before state machine step")

	if cd == nil {
		p.baseLog().
			Info().
			Str("store_backend", p.cfg.Store.Backend).
			Msg("cluster data not in store yet; waiting before managing PostgreSQL")
		return
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		p.baseLog().
			Error().
			Uint64("version", cd.FormatVersion).
			Msg("unsupported clusterdata format version")
		return
	}
	if err = cd.Cluster.Spec.Validate(); err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("clusterdata validation failed")
		return
	}

	// Mark that the clusterdata we've received is valid. We'll use this metric to detect
	// when our store is failing to serve a valid clusterdata, so it's important we only
	// update the metric here.
	clusterdataLastValidUpdateSeconds.SetToCurrentTime()

	if cd.Cluster != nil {
		p.applyRuntimeConfigFromClusterData(cd)

		if p.keeperLocalState.ClusterUID != cd.Cluster.UID {
			p.keeperLocalState.ClusterUID = cd.Cluster.UID
			if err = p.saveKeeperLocalState(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save keeper local state")
				return
			}
		}
	}

	k, ok := cd.Keepers[p.keeperLocalState.UID]
	if !ok {
		p.baseLog().
			Info().
			Str(slog.FieldKeeperUID, p.keeperLocalState.UID).
			Msg("this keeper is not listed in cluster data yet; waiting")
		return
	}

	db := cd.FindDB(k)
	if db == nil {
		p.baseLog().
			Info().
			Str(slog.FieldKeeperUID, k.UID).
			Msg("no database is assigned to this keeper yet; stopping PostgreSQL if it is running")
		if err = p.stopPostgresIfStarted(pgm, db); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
		}
		return
	}

	if p.bootUUID != k.Status.BootUUID {
		p.baseLog().Info().
			Str("local_boot_uuid", p.bootUUID).
			Str("cluster_boot_uuid", k.Status.BootUUID).
			Msg("boot UID from local process differs from cluster data; stopping PostgreSQL until sentinel updates cluster state")
		if err = p.stopPostgresIfStarted(pgm, db); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
		}
		return
	}

	// Generate hba auth from clusterData
	pgm.SetHba(p.generateHBA(cd, db, p.waitSyncStandbysSynced))

	p.baseLog().Debug().
		Str(slog.FieldDBUID, db.UID).
		Int64("db_generation", db.Generation).
		Int64("db_status_generation", db.Status.CurrentGeneration).
		Str("db_role", string(db.Spec.Role)).
		Str("db_init_mode", string(db.Spec.InitMode)).
		Msg("reconciling assigned database: applying cluster specification to local PostgreSQL (state machine tick)")

	var pgParameters common.Parameters

	dbls := p.dbLocalStateCopy()
	if dbls.Initializing {
		// If we are here this means that the db initialization or
		// resync has failed so we have to clean up stale data
		p.baseLog().Error().Msg("db failed to initialize or resync")

		if err = p.stopPostgresIfStarted(pgm, db); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to stop pg instance")
			return
		}

		// Clean up cluster db datadir
		if err = pgm.RemoveAll(); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to remove the postgres data dir")
			return
		}
		// Reset current db local state since it's not valid anymore
		ndbls := &DBLocalState{
			UID:          "",
			Generation:   cluster.NoGeneration,
			Initializing: false,
		}
		if err = p.saveDBLocalState(ndbls); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to save db local state")
			return
		}
	}

	if p.dbLocalState.UID != db.UID {
		var initialized bool
		initialized, err = pgm.IsInitialized()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to detect if instance is initialized")
			return
		}
		p.baseLog().Info().
			Str("local_db_uid", p.dbLocalState.UID).
			Str(slog.FieldDBUID, db.UID).
			Msg("local database UID does not match cluster assignment; will re-initialize or resync as required")

		pgm.SetRecoveryOptions(nil)
		p.waitSyncStandbysSynced = false

		switch db.Spec.InitMode {
		case cluster.DBInitModeNew:
			p.baseLog().Info().Msg("initializing the database cluster")
			ndbls := &DBLocalState{
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(ndbls); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgm postgres parameters
			pgm.SetParameters(pgParameters)

			initConfig := &pg.InitConfig{}

			if db.Spec.NewConfig != nil {
				initConfig.Locale = db.Spec.NewConfig.Locale
				initConfig.Encoding = db.Spec.NewConfig.Encoding
				initConfig.DataChecksums = db.Spec.NewConfig.DataChecksums
			}

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgm.RemoveAll(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to remove the postgres data dir")
				return
			}
			if err = pgm.Init(initConfig); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to initialize postgres database cluster")
				return
			}

			if err = pgm.StartTmpMerged(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}
			if err = pgm.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
			if db.Spec.IncludeConfig {
				pgParameters, err = pgm.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				ndbls.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(ndbls); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}

			p.baseLog().
				Info().
				Msg("database files created; creating replication and application roles")
			if err = pgm.SetupRoles(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to setup roles")
				return
			}

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
		case cluster.DBInitModePITR:
			p.baseLog().
				Info().
				Msg("starting point-in-time recovery / restore into new data directory")
			ndbls := &DBLocalState{
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(ndbls); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgm postgres parameters
			pgm.SetParameters(pgParameters)

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgm.RemoveAll(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to remove the postgres data dir")
				return
			}
			p.baseLog().
				Info().
				Msg("running archive restore command from cluster specification")
			if err = pgm.Restore(db.Spec.PITRConfig.DataRestoreCommand); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to restore postgres database cluster")
				return
			}

			recoveryMode := pg.RecoveryModeRecovery
			var standbySettings *cluster.StandbySettings
			if db.Spec.FollowConfig != nil &&
				db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
				recoveryMode = pg.RecoveryModeStandby
				standbySettings = db.Spec.FollowConfig.StandbySettings
			}

			pgm.SetRecoveryOptions(
				p.createRecoveryOptions(
					recoveryMode,
					standbySettings,
					db.Spec.PITRConfig.ArchiveRecoverySettings,
					db.Spec.PITRConfig.RecoveryTargetSettings,
				),
			)

			if err = pgm.StartTmpMerged(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}

			if recoveryMode == pg.RecoveryModeRecovery {
				// wait for the db having replyed all the wals
				p.baseLog().
					Info().
					Str(slog.FieldDBUID, db.UID).
					Msg("waiting for PostgreSQL to finish replaying WAL (PITR)")
				if err = pgm.WaitRecoveryDone(cd.Cluster.DefSpec().SyncTimeout.Duration); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Str(slog.FieldDBUID, db.UID).
						Msg("point-in-time recovery did not finish within the configured timeout")
					return
				}
				p.baseLog().
					Info().
					Str(slog.FieldDBUID, db.UID).
					Msg("point-in-time recovery replay completed")
			}
			if err = pgm.WaitReady(cd.Cluster.DefSpec().SyncTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}

			if db.Spec.IncludeConfig {
				pgParameters, err = pgm.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				ndbls.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(ndbls); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}

		case cluster.DBInitModeResync:
			p.baseLog().Info().Msg("database resync requested")
			ndbls := &DBLocalState{
				// replace our current db uid with the required one.
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: true,
			}
			if err = p.saveDBLocalState(ndbls); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgm postgres parameters
			pgm.SetParameters(pgParameters)

			var systemID string
			if !initialized {
				p.baseLog().Info().Msg("database cluster is not initialized")
			} else {
				systemID, err = pgm.GetSystemdID()
				if err != nil {
					p.baseLog().Error().Err(err).Msg("error retrieving systemd ID")
					return
				}
			}

			followedUID := db.Spec.FollowConfig.DBUID
			followedDB, ok := cd.DBs[followedUID]
			if !ok {
				p.baseLog().
					Error().
					Str("followed_db", followedUID).
					Msg("followed database is missing from cluster data")
				return
			}

			masterDB, ok := cd.DBs[cd.Cluster.Status.Master]
			masterOlderWal := ""
			if ok {
				masterOlderWal = masterDB.Status.OlderWalFile
			}
			decision := evaluatePgrewindDecision(
				initialized,
				systemID,
				followedDB.Status.SystemID,
				ok,
				db.Status.XLogPos,
				masterOlderWal,
			)
			tryPgrewind := decision.try
			switch decision.reason {
			case pgrewindReasonNotInitialized:
				p.baseLog().Info().Msg("pg_rewind disabled because local database is not initialized")
			case pgrewindReasonSystemIDDiff:
				p.baseLog().Warn().Msg("pg_rewind disabled because local and followed system IDs differ")
			case pgrewindReasonNoMaster:
				p.baseLog().Warn().Msg("pg_rewind disabled because no master database is available")
			case pgrewindReasonWalCheckErr:
				p.baseLog().Warn().
					Err(decision.walCheckErr).
					Str("older_master_wal", masterOlderWal).
					Msg("cannot verify required WAL availability for pg_rewind path")
			case pgrewindReasonWalMissing:
				p.baseLog().Info().
					Str("required_wal", decision.requiredWal).
					Str("older_master_wal", decision.olderWal).
					Msg("pg_rewind disabled because required WAL is no longer available on master")
			}

			// pg_rewind can leave a node on a diverged branch in edge cases.
			// Verify branch alignment after rewind and force full resync when
			// divergence is still detected.

			// A rewinded standby needs WAL from the master starting from the
			// common ancestor. If those WAL files are unavailable or startup
			// still stalls, fall back to full resync with pg_basebackup.
			if err = p.resync(db, masterDB, followedDB, tryPgrewind); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to resync from followed instance")
				return
			}
			if err = pgm.Start(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}

			if tryPgrewind {
				fullResync := false
				// if not accepting connection assume that it's blocked waiting for missing wal
				// (see above TODO), so do a full resync using pg_basebackup.
				if err = pgm.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("standby did not become ready after pg_rewind, forcing full resync")
					fullResync = true
				} else {
					// Check again if it was really synced
					var pgState *cluster.PostgresState
					pgState, err = p.GetPGState(pctx)
					if err != nil {
						p.baseLog().Error().Err(err).Msg("cannot get current pgstate")
						return
					}
					if p.isDifferentTimelineBranch(followedDB, pgState) {
						fullResync = true
					}
				}

				if fullResync {
					if err = p.stopPostgresIfStarted(pgm, db); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to stop pg instance")
						return
					}
					if err = p.resync(db, masterDB, followedDB, false); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to resync from followed instance")
						return
					}
				}
			}

		case cluster.DBInitModeExisting:
			ndbls := &DBLocalState{
				// replace our current db uid with the required one.
				UID: db.UID,
				// Set a no generation since we aren't already converged.
				Generation:   cluster.NoGeneration,
				Initializing: false,
			}
			if err = p.saveDBLocalState(ndbls); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to save db local state")
				return
			}

			// create postgres parameters with empty InitPGParameters
			pgParameters = p.createPGParameters(db)
			// update pgm postgres parameters
			pgm.SetParameters(pgParameters)

			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
			if err = pgm.StartTmpMerged(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start instance")
				return
			}
			if err = pgm.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
			if db.Spec.IncludeConfig {
				pgParameters, err = pgm.GetConfigFilePGParameters()
				if err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to retrieve postgres parameters")
					return
				}
				ndbls.InitPGParameters = pgParameters
				if err = p.saveDBLocalState(ndbls); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to save db local state")
					return
				}
			}
			if err = p.stopPostgresIfStarted(pgm, db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to stop pg instance")
				return
			}
		case cluster.DBInitModeNone:
			p.baseLog().
				Error().
				Msg("local database state invariant broken: init mode is none")
			return
		default:
			p.baseLog().
				Error().
				Str("db_init_mode", string(db.Spec.InitMode)).
				Msg("unknown database init mode")
			return
		}
	}

	initialized, err := pgm.IsInitialized()
	if err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("failed to detect if instance is initialized")
		return
	}

	if initialized {
		var started bool
		started, err = pgm.IsStarted()
		if err != nil {
			// log error getting instance state but go ahead.
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve instance status")
		}
		p.baseLog().
			Debug().
			Bool("initialized", true).
			Bool("pg_started", started).
			Msg("database instance status")
	} else {
		p.baseLog().
			Debug().
			Bool("initialized", false).
			Bool("pg_started", false).
			Msg("database instance status")
	}

	// create postgres parameters
	pgParameters = p.createPGParameters(db)
	// update pgm postgres parameters
	pgm.SetParameters(pgParameters)

	var localRole common.Role
	if !initialized {
		p.baseLog().Info().Msg("database cluster is not initialized")
		localRole = common.RoleUndefined
	} else {
		localRole, err = pgm.GetRole()
		if err != nil {
			p.baseLog().Error().Err(err).Msg("error retrieving current pg role")
			return
		}
	}

	targetRole := db.Spec.Role
	p.baseLog().
		Debug().
		Str("target_role", string(targetRole)).
		Msg("applying target PostgreSQL role")

	// Set metrics to power alerts about mismatched roles
	setRole(localRoleGauge, &localRole)
	setRole(targetRoleGauge, &targetRole)

	switch targetRole {
	case common.RoleMaster:
		// We are the elected master
		p.baseLog().Info().Msg("applying requested master role")
		if localRole == common.RoleUndefined {
			p.baseLog().
				Error().
				Msg("master role requested but data directory is uninitialized")
			return
		}

		pgm.SetRecoveryOptions(nil)

		started, err := pgm.IsStarted()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve instance status")
			return
		}
		if !started {
			// if we have syncrepl enabled and the postgres instance is stopped, before opening connections to normal users wait for having the defined synchronousStandbys in sync state.
			if db.Spec.SynchronousReplication {
				p.waitSyncStandbysSynced = true
				p.baseLog().
					Info().
					Msg("restricting normal users in pg_hba until synchronous standbys catch up")
				pgm.SetHba(p.generateHBA(cd, db, true))
			}

			if err = pgm.Start(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to start postgres")
				return
			}
			if err = pgm.WaitReady(cd.Cluster.DefSpec().DBWaitReadyTimeout.Duration); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("timeout waiting for instance to be ready")
				return
			}
		}

		if localRole == common.RoleStandby {
			if err = p.runPrePromoteHook(db); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Str(slog.FieldDBUID, db.UID).
					Msg("pre-promote hook failed; refusing promote")
				return
			}
			p.baseLog().Info().Msg("promoting standby to master")
			if err = pgm.Promote(); err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to promote instance")
				return
			}
		} else {
			p.baseLog().Info().Msg("PostgreSQL is already primary")
		}

		if err := p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("error updating replication slots")
			return
		}

	case common.RoleStandby:
		// We are a standby
		var standbySettings *cluster.StandbySettings
		switch db.Spec.FollowConfig.Type {
		case cluster.FollowTypeInternal:
			followedUID := db.Spec.FollowConfig.DBUID
			p.baseLog().
				Info().
				Str("followed_db", followedUID).
				Msg("applying requested standby role")
			followedDB, ok := cd.DBs[followedUID]
			if !ok {
				p.baseLog().
					Error().
					Str("followed_db", followedUID).
					Msg("followed database is missing from cluster data")
				return
			}
			replConnParams := p.getReplConnParams(db, followedDB)
			standbySettings = &cluster.StandbySettings{
				PrimaryConninfo: replConnParams.ConnString(),
				PrimarySlotName: common.HysteronName(db.UID),
			}
		case cluster.FollowTypeExternal:
			standbySettings = db.Spec.FollowConfig.StandbySettings
		default:
			p.baseLog().
				Error().
				Str("follow_type", string(db.Spec.FollowConfig.Type)).
				Msg("unknown follow type")
			return
		}
		switch localRole {
		case common.RoleMaster:
			p.baseLog().
				Error().
				Msg("refusing invalid transition from master to standby")
			return
		case common.RoleStandby:
			p.baseLog().Info().Msg("PostgreSQL is already standby")
			started, err := pgm.IsStarted()
			if err != nil {
				p.baseLog().
					Error().
					Err(err).
					Msg("failed to retrieve instance status")
				return
			}
			if !started {
				pgm.SetRecoveryOptions(
					p.createRecoveryOptions(
						pg.RecoveryModeStandby,
						standbySettings,
						nil,
						nil,
					),
				)
				if err = pgm.Start(); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to start postgres")
					return
				}
			}

			// Update our primary_conninfo if replConnString changed
			switch db.Spec.FollowConfig.Type {
			case cluster.FollowTypeInternal:
				followedUID := db.Spec.FollowConfig.DBUID
				followedDB, ok := cd.DBs[followedUID]
				if !ok {
					p.baseLog().
						Error().
						Str("followed_db", followedUID).
						Msg("followed database is missing from cluster data")
					return
				}
				newReplConnParams := p.getReplConnParams(db, followedDB)
				p.baseLog().
					Debug().
					Fields(pg.LogSummaryConnParams(newReplConnParams)).
					Msg("standby replication connection parameters updated")

				standbySettings := &cluster.StandbySettings{
					PrimaryConninfo: newReplConnParams.ConnString(),
					PrimarySlotName: common.HysteronName(db.UID),
				}

				curRecoveryOptions := pgm.CurRecoveryOptions()
				newRecoveryOptions := p.createRecoveryOptions(
					pg.RecoveryModeStandby,
					standbySettings,
					nil,
					nil,
				)

				// Update recovery conf if parameters has changed
				if !curRecoveryOptions.RecoveryParameters.Equals(
					newRecoveryOptions.RecoveryParameters,
				) {
					p.baseLog().Info().
						Interface("recovery_prev", pg.LogSummaryRecoveryParameters(curRecoveryOptions.RecoveryParameters)).
						Interface("recovery_new", pg.LogSummaryRecoveryParameters(newRecoveryOptions.RecoveryParameters)).
						Msg("recovery parameters changed; restarting PostgreSQL")
					pgm.SetRecoveryOptions(newRecoveryOptions)
					p.runBeforeStopHook(db)

					if err = pgm.Restart(true); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to restart postgres instance")
						return
					}
				}

				if err = p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("error updating replication slots")
				}

			case cluster.FollowTypeExternal:
				curRecoveryOptions := pgm.CurRecoveryOptions()
				newRecoveryOptions := p.createRecoveryOptions(
					pg.RecoveryModeStandby,
					db.Spec.FollowConfig.StandbySettings,
					db.Spec.FollowConfig.ArchiveRecoverySettings,
					nil,
				)

				// Update recovery conf if parameters has changed
				if !curRecoveryOptions.RecoveryParameters.Equals(
					newRecoveryOptions.RecoveryParameters,
				) {
					p.baseLog().Info().
						Interface("recovery_prev", pg.LogSummaryRecoveryParameters(curRecoveryOptions.RecoveryParameters)).
						Interface("recovery_new", pg.LogSummaryRecoveryParameters(newRecoveryOptions.RecoveryParameters)).
						Msg("recovery parameters changed; restarting PostgreSQL")
					pgm.SetRecoveryOptions(newRecoveryOptions)
					p.runBeforeStopHook(db)

					if err = pgm.Restart(true); err != nil {
						p.baseLog().
							Error().
							Err(err).
							Msg("failed to restart postgres instance")
						return
					}
				}

				if err = p.refreshReplicationSlots(cd.Cluster.DefSpec(), db, cd.DBs); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("error updating replication slots")
				}
			}

			p.ensureStandbyWALReplayRunning(pgm, db.UID)

		case common.RoleUndefined:
			p.baseLog().Info().Msg("current database role is undefined")
			return
		}
	case common.RoleUndefined:
		p.baseLog().Info().Msg("target database role is undefined")
		return
	}

	// update pg parameters
	pgParameters = p.createPGParameters(db)

	// Log synchronous replication changes
	prevSyncStandbyNames := pgm.CurParameters()["synchronous_standby_names"]
	syncStandbyNames := pgParameters["synchronous_standby_names"]
	if db.Spec.SynchronousReplication {
		if prevSyncStandbyNames != syncStandbyNames {
			p.baseLog().Info().
				Str("sync_standby_names_prev", prevSyncStandbyNames).
				Str("sync_standby_names_new", syncStandbyNames).
				Msg("synchronous standby names changed")
		}
	} else {
		if prevSyncStandbyNames != "" {
			p.baseLog().Info().
				Str("sync_standby_names_cleared", prevSyncStandbyNames).
				Msg("synchronous replication disabled, clearing synchronous standbys")
		}
	}

	needsReload := false

	if !pgParameters.Equals(pgm.CurParameters()) {
		p.baseLog().Info().Msg("postgres parameters changed, reloading postgres instance")
		pgm.SetParameters(pgParameters)
		needsReload = true
	} else {
		// for tests
		p.baseLog().Debug().Msg("postgres parameters not changed")
	}

	// Generate hba auth from clusterData

	// if we have syncrepl enabled and the postgres instance is stopped, before opening connections to normal users wait for having the defined synchronousStandbys in sync state.
	if db.Spec.SynchronousReplication && p.waitSyncStandbysSynced {
		inSyncStandbys, err := p.GetInSyncStandbys()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to retrieve current in sync standbys from instance")
			return
		}
		if !slicesutil.CompareStringSliceNoOrder(
			inSyncStandbys,
			db.Spec.SynchronousStandbys,
		) {
			p.baseLog().
				Info().
				Msg("waiting for synchronous standbys before allowing normal users")
		} else {
			p.waitSyncStandbysSynced = false
		}
	} else {
		p.waitSyncStandbysSynced = false
	}
	newHBA := p.generateHBA(cd, db, p.waitSyncStandbysSynced)
	if !reflect.DeepEqual(newHBA, pgm.CurHba()) {
		p.baseLog().Info().Msg("pg_hba changed, reloading postgres instance")
		pgm.SetHba(newHBA)
		needsReload = true
	} else {
		// for tests
		p.baseLog().Debug().Msg("pg_hba not changed")
	}

	if needsReload {
		needsReloadGauge.Set(1) // mark as reload needed
		if err := pgm.Reload(); err != nil {
			p.baseLog().Error().Err(err).Msg("failed to reload postgres instance")
		} else {
			needsReloadGauge.Set(0) // successful reload implies no longer required
		}
	}

	{
		clusterSpec := cd.Cluster.DefSpec()
		automaticPgRestartEnabled := *clusterSpec.AutomaticPgRestart

		restartRequirement, err := pgm.IsRestartRequiredDetailed()
		if err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to check if restart is required")
		}

		if restartRequirement != nil && restartRequirement.Required {
			needsRestartGauge.Set(1) // mark as restart needed
			p.baseLog().
				Warn().
				Strs("pending_restart_params", restartRequirement.PendingParams).
				Msg("PostgreSQL reports pending restart parameters")
			if automaticPgRestartEnabled {
				p.baseLog().Info().Msg("automatic PostgreSQL restart scheduled")
				p.runBeforeStopHook(db)
				if err := pgm.Restart(true); err != nil {
					p.baseLog().
						Error().
						Err(err).
						Msg("failed to restart postgres instance")
				} else {
					needsRestartGauge.Set(0) // successful restart implies no longer required
				}
			}
		}
	}

	// If we are here, then all went well and we can update the db generation and save it locally
	ndbls := p.dbLocalStateCopy()
	ndbls.Generation = db.Generation
	ndbls.Initializing = false
	if err := p.saveDBLocalState(ndbls); err != nil {
		p.baseLog().
			Error().
			Err(err).
			Msg("failed to save db local state")
		return
	}

	// We want to set this only if no error has occurred. We should be able to identify
	// keeper issues by watching for this value becoming stale.
	lastSyncSuccessSeconds.SetToCurrentTime()
}

func (p *PostgresKeeper) ensureStandbyWALReplayRunning(replay standbyReplayController, dbUID string) {
	paused, err := replay.IsWALReplayPaused()
	if err != nil {
		p.baseLog().
			Warn().
			Err(err).
			Str(slog.FieldDBUID, dbUID).
			Msg("failed to check WAL replay pause status")
		return
	}
	if !paused {
		return
	}

	p.baseLog().
		Warn().
		Str(slog.FieldDBUID, dbUID).
		Msg("WAL replay is paused on standby; attempting resume")
	if err := replay.ResumeWALReplay(); err != nil {
		p.baseLog().
			Warn().
			Err(err).
			Str(slog.FieldDBUID, dbUID).
			Msg("failed to resume paused WAL replay on standby")
		return
	}
	p.baseLog().
		Info().
		Str(slog.FieldDBUID, dbUID).
		Msg("resumed paused WAL replay on standby")
}

func (p *PostgresKeeper) stopPostgresIfStarted(pgm *pg.Manager, db *cluster.DB) error {
	p.runBeforeStopHook(db)
	return pgm.StopIfStarted(true)
}

func (p *PostgresKeeper) runBeforeStopHook(db *cluster.DB) {
	if db == nil || db.Spec == nil {
		return
	}
	_ = p.runHookCommand(db, db.Spec.BeforeStopCommand, "before-stop")
}

func (p *PostgresKeeper) runPrePromoteHook(db *cluster.DB) error {
	if db == nil || db.Spec == nil {
		return nil
	}
	return p.runHookCommand(db, db.Spec.PrePromoteCommand, "pre-promote")
}

func (p *PostgresKeeper) runHookCommand(
	db *cluster.DB,
	command string,
	hookName string,
) error {
	if db == nil || db.Spec == nil {
		return nil
	}
	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}

	p.baseLog().
		Info().
		Str(slog.FieldDBUID, db.UID).
		Str("hook", hookName).
		Str("hook_command", command).
		Msg("executing keeper hook command")

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/C", command)
	default:
		cmd = exec.Command("/bin/sh", "-c", command)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		p.baseLog().
			Warn().
			Err(err).
			Str(slog.FieldDBUID, db.UID).
			Str("hook", hookName).
			Str("hook_command", command).
			Msg("keeper hook command failed")
		return err
	}

	p.baseLog().
		Info().
		Str(slog.FieldDBUID, db.UID).
		Str("hook", hookName).
		Msg("keeper hook command completed")
	return nil
}

func (p *PostgresKeeper) keeperLocalStateFilePath() string {
	return filepath.Join(p.cfg.DataDir, "keeperstate")
}

func (p *PostgresKeeper) loadKeeperLocalState() error {
	sj, err := os.ReadFile(p.keeperLocalStateFilePath())
	if err != nil {
		return err
	}
	var s *LocalState
	if err := json.Unmarshal(sj, &s); err != nil {
		return err
	}
	p.keeperLocalState = s
	return nil
}

func (p *PostgresKeeper) saveKeeperLocalState() error {
	sj, err := json.Marshal(p.keeperLocalState)
	if err != nil {
		return err
	}
	return fs.WriteFileAtomic(
		p.keeperLocalStateFilePath(),
		0600,
		sj,
	)
}

func (p *PostgresKeeper) dbLocalStateFilePath() string {
	return filepath.Join(p.cfg.DataDir, "dbstate")
}

func (p *PostgresKeeper) loadDBLocalState() error {
	sj, err := os.ReadFile(p.dbLocalStateFilePath())
	if err != nil {
		return err
	}
	var s *DBLocalState
	if err := json.Unmarshal(sj, &s); err != nil {
		return err
	}
	p.dbLocalState = s
	return nil
}

// saveDBLocalState saves on disk the dbLocalState and only if successful
// updates the current in memory state
func (p *PostgresKeeper) saveDBLocalState(dbls *DBLocalState) error {
	sj, err := json.Marshal(dbls)
	if err != nil {
		return err
	}
	if err = fs.WriteFileAtomic(p.dbLocalStateFilePath(), 0600, sj); err != nil {
		return err
	}

	p.localStateMutex.Lock()
	p.dbLocalState = dbls.DeepCopy()
	p.localStateMutex.Unlock()

	return nil
}

// IsMaster return if the db is the cluster master db.
// A master is a db that:
// * Has a master db role
// or
// * Has a standby db role with followtype external
func IsMaster(db *cluster.DB) bool {
	switch db.Spec.Role {
	case common.RoleMaster:
		return true
	case common.RoleStandby:
		if db.Spec.FollowConfig.Type == cluster.FollowTypeExternal {
			return true
		}
		return false
	default:
		common.MustNotMsg(true, "invalid db role in db Spec")
		return false
	}
}

// generateHBA generates the instance hba entries depending on the value of
// DefaultSUReplAccessMode.
// When onlyInternal is true only rules needed for replication will be setup
// and the traffic should be permitted only for pgSUUsername standard
// connections and pgReplUsername replication connections.
func (p *PostgresKeeper) generateHBA(
	cd *cluster.ClusterData,
	db *cluster.DB,
	onlyInternal bool,
) []string {
	// Minimal entries for local normal and replication connections needed by the hysteron keeper
	// Matched local connections are for postgres database and suUsername user with md5 auth
	// Matched local replication connections are for replUsername user with md5 auth
	computedHBA := []string{
		fmt.Sprintf(
			"local postgres %s %s",
			p.pgSUUsername,
			p.pgSUAuthMethod,
		),
		fmt.Sprintf(
			"local replication %s %s",
			p.pgReplUsername,
			p.pgReplAuthMethod,
		),
	}

	switch *cd.Cluster.DefSpec().DefaultSUReplAccessMode {
	case cluster.SUReplAccessAll:
		// all the keepers will accept connections from every host
		computedHBA = append(
			computedHBA,
			fmt.Sprintf(
				"host all %s %s %s",
				p.pgSUUsername,
				"0.0.0.0/0",
				p.pgSUAuthMethod,
			),
			fmt.Sprintf(
				"host all %s %s %s",
				p.pgSUUsername,
				"::0/0",
				p.pgSUAuthMethod,
			),
			fmt.Sprintf(
				"host replication %s %s %s",
				p.pgReplUsername,
				"0.0.0.0/0",
				p.pgReplAuthMethod,
			),
			fmt.Sprintf(
				"host replication %s %s %s",
				p.pgReplUsername,
				"::0/0",
				p.pgReplAuthMethod,
			),
		)
	case cluster.SUReplAccessStrict:
		// only the master keeper (primary instance or standby of a remote primary when in standby cluster mode) will accept connections only from the other standby keepers IPs
		if IsMaster(db) {
			addresses := []string{}
			for _, dbElt := range cd.DBs {
				if dbElt.UID != db.UID {
					addresses = append(
						addresses,
						dbElt.Status.ListenAddress,
					)
				}
			}
			sort.Strings(addresses)
			for _, address := range addresses {
				computedHBA = append(
					computedHBA,
					fmt.Sprintf(
						"host all %s %s/32 %s",
						p.pgSUUsername,
						address,
						p.pgReplAuthMethod,
					),
					fmt.Sprintf(
						"host replication %s %s/32 %s",
						p.pgReplUsername,
						address,
						p.pgReplAuthMethod,
					),
				)
			}
		}
	}

	if !onlyInternal {
		// By default, if no custom pg_hba entries are provided, accept
		// connections for all databases and users with md5 auth
		if db.Spec.PGHBA != nil {
			computedHBA = append(computedHBA, db.Spec.PGHBA...)
		} else {
			computedHBA = append(
				computedHBA,
				"host all all 0.0.0.0/0 md5",
				"host all all ::0/0 md5",
			)
		}
	}

	// return generated Hba merged with user Hba
	return computedHBA
}

func sigHandler(sigs chan os.Signal, cancel context.CancelFunc) {
	s := <-sigs
	keeperRootLog(
		nil,
	).Debug().
		Str("signal", s.String()).
		Msg("shutdown signal received")
	shutdownSeconds.SetToCurrentTime()
	cancel()
}

// newParser creates a parser for runtime keeper options. Built-in helper
// commands remain available, but the keeper itself is a daemon so
// subcommand selection is optional.
func newParser() *flags.Parser {
	parser := runtimecommon.NewParser("hysteron keeper", "HYSTERON", &cfg, 0)
	parser.SubcommandsOptional = true
	return parser
}

// Run starts keeper with externally prepared common config and optional
// keeper-specific CLI arguments.
func Run(commonConfig stconfig.CommonConfig, args []string) error {
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	parser := newParser()
	if _, err := parser.ParseArgs(args); err != nil {
		return err
	}
	if parser.Active != nil {
		return nil
	}
	return runKeeper(parser)
}

func runKeeper(parser *flags.Parser) error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	kl := keeperRootLog(&cfg)
	defer runtimecommon.CloseLogging(closer, kl)
	pg.SetLogger(slog.L())

	var (
		listenAddFlag = "pg-advertise-address"
	)

	option := (*flags.Option)(nil)
	if parser != nil {
		option = parser.FindOptionByLongName("pg-su-username")
	}
	if option == nil || !option.IsSet() {
		// set the pgSuUsername to the current user
		var user string
		user, err = osuser.GetUser()
		if err != nil {
			return fmt.Errorf("failed to get current user: %w", err)
		}
		cfg.PG.SU.Username = user
	}

	if cfg.DataDir == "" {
		return errors.New("data directory is required")
	}

	if err = runtimecommon.CheckClusterName(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid cluster name: %w", err)
	}
	if err = runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}

	runtimecommon.SetMetrics(&cfg.CommonConfig, "keeper")

	if err = os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory %q: %w", cfg.DataDir, err)
	}

	if cfg.PG.ListenAddress == "" {
		return errors.New("postgresql listen address is required")
	}

	if cfg.PG.AdvertiseAddress == "" {
		listenAddFlag = "pg-listen-address"
		cfg.PG.AdvertiseAddress = cfg.PG.ListenAddress
	}

	if cfg.PG.AdvertisePort == "" {
		cfg.PG.AdvertisePort = cfg.PG.Port
	}

	ip := net.ParseIP(cfg.PG.AdvertiseAddress)
	if ip == nil {
		kl.Warn().
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("PostgreSQL advertise address is not an IP address")
	}

	ipAddr, err := net.ResolveIPAddr("ip", cfg.PG.AdvertiseAddress)
	if err != nil {
		kl.Warn().
			Err(err).
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("failed to resolve PostgreSQL advertise address")
	} else if ipAddr.IP.IsLoopback() {
		kl.Warn().
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("PostgreSQL advertise address is a loopback address")
	}

	if cfg.PG.Repl.Username == "" {
		return errors.New("postgresql replication username is required")
	}
	if cfg.PG.Repl.AuthMethod == "trust" {
		kl.Warn().Msg("PostgreSQL replication user uses trust authentication")
		if cfg.PG.Repl.Password != "" ||
			cfg.PG.Repl.PasswordFile != "" {
			return errors.New("postgresql replication password cannot be set when trust authentication is used")
		}
	} else if cfg.PG.Repl.Password == "" && cfg.PG.Repl.PasswordFile == "" {
		return errors.New("postgresql replication password is required")
	}
	if cfg.PG.SU.AuthMethod == "trust" {
		kl.Warn().Msg("PostgreSQL superuser uses trust authentication")
		if cfg.PG.SU.Password != "" || cfg.PG.SU.PasswordFile != "" {
			return errors.New("postgresql superuser password cannot be set when trust authentication is used")
		}
	} else if cfg.PG.SU.Password == "" && cfg.PG.SU.PasswordFile == "" {
		return errors.New("postgresql superuser password is required")
	}

	if cfg.PG.Repl.PasswordFile != "" {
		cfg.PG.Repl.Password, err = readPasswordFromFile(
			cfg.PG.Repl.PasswordFile,
		)
		if err != nil {
			return fmt.Errorf("failed to read PostgreSQL replication password file: %w", err)
		}
	}
	if cfg.PG.SU.PasswordFile != "" {
		cfg.PG.SU.Password, err = readPasswordFromFile(
			cfg.PG.SU.PasswordFile,
		)
		if err != nil {
			return fmt.Errorf("failed to read PostgreSQL superuser password file: %w", err)
		}
	}

	// Trim trailing new lines from passwords
	tp := strings.TrimRight(cfg.PG.SU.Password, "\r\n")
	if cfg.PG.SU.Password != tp {
		kl.Warn().Msg("superuser password contains a trailing newline, removing it")
		if tp == "" {
			return errors.New("superuser password is empty after removing trailing newlines")
		}
		cfg.PG.SU.Password = tp
	}

	tp = strings.TrimRight(cfg.PG.Repl.Password, "\r\n")
	if cfg.PG.Repl.Password != tp {
		kl.Warn().Msg("replication user password contains a trailing newline, removing it")
		if tp == "" {
			return errors.New("replication user password is empty after removing trailing newlines")
		}
		cfg.PG.Repl.Password = tp
	}

	if cfg.PG.SU.Username == cfg.PG.Repl.Username {
		kl.Warn().Msg("provided superuser name and replication user name are the same")
		if cfg.PG.Repl.AuthMethod != cfg.PG.SU.AuthMethod {
			return errors.New("provided superuser name and replication user name are the same but provided authentication methods are different")
		}
		if cfg.PG.SU.Password != cfg.PG.Repl.Password &&
			cfg.PG.SU.AuthMethod != "trust" &&
			cfg.PG.Repl.AuthMethod != "trust" {
			return errors.New("provided superuser name and replication user name are the same but provided passwords are different")
		}
	}

	// Open (and create if needed) the lock file.
	// There is no need to clean up this file since we don't use the file as an
	// actual lock. We get a lock on the file. So the lock get released when
	// our process stops (or log.Fatalfs).
	var lockFile *os.File
	if !cfg.DisableDataDirLocking {
		lockFileName := filepath.Join(cfg.DataDir, "lock")
		lockFile, err = os.OpenFile(
			lockFileName,
			os.O_RDWR|os.O_CREATE,
			0600,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to open data directory lock file %q: %w",
				lockFileName,
				err,
			)
		}

		if err := takeDataDirLock(lockFile); err != nil {
			return fmt.Errorf(
				"cannot take exclusive lock on data dir %q: %w",
				lockFileName,
				err,
			)
		}

		kl.Info().
			Str("data_dir", cfg.DataDir).
			Msg("exclusive lock on data dir taken")
	}

	if cfg.UID != "" {
		if !pg.IsValidReplSlotName(cfg.UID) {
			return fmt.Errorf("keeper uid %q is not a valid replication slot name", cfg.UID)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go sigHandler(sigs, cancel)

	if cfg.Metrics.ListenAddress != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsServer := http.Server{
			Addr:              cfg.Metrics.ListenAddress,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			err = metricsServer.ListenAndServe()
			if err != nil {
				kl.Error().
					Err(err).
					Str("addr", cfg.Metrics.ListenAddress).
					Msg("metrics HTTP server failed")
				cancel()
			}
		}()
	}

	p, err := NewPostgresKeeper(&cfg, end)
	if err != nil {
		return fmt.Errorf("failed to create postgres keeper: %w", err)
	}
	go p.Start(ctx)

	if err := <-end; err != nil {
		p.baseLog().Error().Err(err).Msg("keeper run failed")
	}

	if !cfg.DisableDataDirLocking {
		if err := lockFile.Close(); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to close data directory lock file")
		}
	}
	return nil
}
