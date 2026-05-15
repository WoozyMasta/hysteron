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

package postgresql

import (
	"errors"
	"maps"
	"path/filepath"
	"sync"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/log"

	"github.com/rs/zerolog"
)

//go:generate mockgen -destination=../mock/postgresql/postgresql.go -package=mocks -source=$GOFILE

const (
	postgresConf           = "postgresql.conf"
	postgresStandbySignal  = "standby.signal"
	postgresRecoverySignal = "recovery.signal"
	postgresAutoConf       = "postgresql.auto.conf"
	tmpPostgresConf        = "hysteron-temp-postgresql.conf"

	startTimeout = 60 * time.Second
)

var (
	// ErrUnknownState reports an unrecognized PostgreSQL state value.
	ErrUnknownState = errors.New("unknown postgres state")
)

var pgLog *zerolog.Logger

func zl() *zerolog.Logger {
	if pgLog != nil {
		return pgLog
	}
	return log.L()
}

// PGManager exposes PostgreSQL manager methods required by other packages.
type PGManager interface {
	GetTimelinesHistory(timeline uint64) ([]*TimelineHistory, error)
}

// Manager manages one local PostgreSQL instance lifecycle.
type Manager struct {
	// Desired PostgreSQL parameters.
	parameters common.Parameters
	// Desired recovery options.
	recoveryOptions *RecoveryOptions
	// Last applied PostgreSQL parameters.
	curParameters common.Parameters
	// Last applied recovery options.
	curRecoveryOptions *RecoveryOptions
	// Local administrative connection parameters.
	localConnParams ConnParams
	// Replication connection parameters.
	replConnParams ConnParams
	// PostgreSQL binaries directory path.
	pgBinPath string
	// Managed PostgreSQL data directory path.
	dataDir string
	// Managed PostgreSQL WAL directory path.
	walDir string
	// Whether WAL directory is explicitly configured.
	walDirConfigured bool
	// Superuser auth method.
	suAuthMethod string
	// Superuser username.
	suUsername string
	// Superuser password.
	suPassword string
	// Replication user auth method.
	replAuthMethod string
	// Replication username.
	replUsername string
	// Replication password.
	replPassword string
	// Desired pg_hba entries.
	hba []string
	// Last applied pg_hba entries.
	curHba []string
	// Request timeout for PostgreSQL operations.
	requestTimeout time.Duration
	// Guards request timeout updates/read across concurrent keeper loops.
	requestTimeoutMu sync.RWMutex
}

// RestartRequirement describes whether a PostgreSQL restart is required and
// which settings currently require it.
type RestartRequirement struct {
	PendingParams []string
	Required      bool
}

// PhysicalReplicationSlot describes one physical replication slot status.
type PhysicalReplicationSlot struct {
	Name    string
	Active  bool
	HasXmin bool
}

// LogicalReplicationSlot describes one logical replication slot status.
type LogicalReplicationSlot struct {
	Name              string
	Database          string
	Plugin            string
	ConfirmedFlushLSN uint64
	Active            bool
	Failover          bool
	Synced            bool
}

// RecoveryMode defines PostgreSQL startup recovery mode.
type RecoveryMode int

const (
	// RecoveryModeNone disables recovery-specific startup behavior.
	RecoveryModeNone RecoveryMode = iota
	// RecoveryModeStandby starts PostgreSQL in standby mode.
	RecoveryModeStandby
	// RecoveryModeRecovery starts PostgreSQL in recovery mode.
	RecoveryModeRecovery
)

// RecoveryOptions configures recovery mode and recovery parameters.
type RecoveryOptions struct {
	RecoveryParameters common.Parameters
	RecoveryMode       RecoveryMode
}

// NewRecoveryOptions builds empty recovery options.
func NewRecoveryOptions() *RecoveryOptions {
	return &RecoveryOptions{RecoveryParameters: make(common.Parameters)}
}

// DeepCopy returns an independent copy of recovery options.
func (r *RecoveryOptions) DeepCopy() *RecoveryOptions {
	if r == nil {
		return nil
	}
	next := *r
	if r.RecoveryParameters != nil {
		next.RecoveryParameters = make(common.Parameters, len(r.RecoveryParameters))
		maps.Copy(next.RecoveryParameters, r.RecoveryParameters)
	}
	return &next
}

// SystemData contains PostgreSQL system identifier and position data.
type SystemData struct {
	SystemID   string
	TimelineID uint64
	XLogPos    uint64
}

// StandbyStatus contains standby WAL receiver/replay state.
type StandbyStatus struct {
	ReceiveLSN       uint64
	ReplayLSN        uint64
	ReplayLagSeconds float64
}

// TimelineHistory is one timeline history record from PostgreSQL.
type TimelineHistory struct {
	Reason      string
	TimelineID  uint64
	SwitchPoint uint64
}

// InitConfig configures initdb options.
type InitConfig struct {
	Locale        string
	Encoding      string
	DataChecksums bool
}

// SetLogger sets the package logger used by PostgreSQL helpers.
func SetLogger(l *zerolog.Logger) {
	pgLog = l
}

// NewManager creates a PostgreSQL manager bound to one data directory.
func NewManager(
	pgBinPath string,
	dataDir string,
	walDir string,
	localConnParams,
	replConnParams ConnParams,
	suAuthMethod,
	suUsername,
	suPassword,
	replAuthMethod,
	replUsername,
	replPassword string,
	requestTimeout time.Duration,
) *Manager {
	pgDataDir := filepath.Join(dataDir, "postgres")
	effectiveWALDir := filepath.Join(pgDataDir, "pg_wal")
	walDirConfigured := walDir != ""
	if walDirConfigured {
		effectiveWALDir = walDir
	}

	return &Manager{
		pgBinPath:          pgBinPath,
		dataDir:            pgDataDir,
		walDir:             effectiveWALDir,
		walDirConfigured:   walDirConfigured,
		parameters:         make(common.Parameters),
		recoveryOptions:    NewRecoveryOptions(),
		curParameters:      make(common.Parameters),
		curRecoveryOptions: NewRecoveryOptions(),
		replConnParams:     replConnParams,
		localConnParams:    localConnParams,
		suAuthMethod:       suAuthMethod,
		suUsername:         suUsername,
		suPassword:         suPassword,
		replAuthMethod:     replAuthMethod,
		replUsername:       replUsername,
		replPassword:       replPassword,
		requestTimeout:     requestTimeout,
	}
}
