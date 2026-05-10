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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
	slog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/utils/fs"

	"github.com/mitchellh/copystructure"
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
	return slog.L()
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
	Active            bool
	ConfirmedFlushLSN uint64
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
	nr, err := copystructure.Copy(r)
	common.MustNot(err, "recovery options deep copy")
	return nr.(*RecoveryOptions)
}

// SystemData contains PostgreSQL system identifier and position data.
type SystemData struct {
	SystemID   string
	TimelineID uint64
	XLogPos    uint64
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
func NewManager(pgBinPath string, dataDir string, localConnParams, replConnParams ConnParams, suAuthMethod, suUsername, suPassword, replAuthMethod, replUsername, replPassword string, requestTimeout time.Duration) *Manager {
	return &Manager{
		pgBinPath:          pgBinPath,
		dataDir:            filepath.Join(dataDir, "postgres"),
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

// SetParameters sets desired PostgreSQL configuration parameters.
func (p *Manager) SetParameters(parameters common.Parameters) {
	p.parameters = parameters
}

// CurParameters returns the current tracked PostgreSQL parameters.
func (p *Manager) CurParameters() common.Parameters {
	return p.curParameters
}

// SetRecoveryOptions sets desired recovery options.
func (p *Manager) SetRecoveryOptions(recoveryOptions *RecoveryOptions) {
	if recoveryOptions == nil {
		p.recoveryOptions = NewRecoveryOptions()
		return
	}

	p.recoveryOptions = recoveryOptions
}

// CurRecoveryOptions returns the current tracked recovery options.
func (p *Manager) CurRecoveryOptions() *RecoveryOptions {
	return p.curRecoveryOptions
}

// SetHba sets desired pg_hba entries.
func (p *Manager) SetHba(hba []string) {
	p.hba = hba
}

// SetRequestTimeout updates timeout used by PostgreSQL operations.
func (p *Manager) SetRequestTimeout(timeout time.Duration) {
	p.requestTimeoutMu.Lock()
	p.requestTimeout = timeout
	p.requestTimeoutMu.Unlock()
}

func (p *Manager) requestTimeoutValue() time.Duration {
	p.requestTimeoutMu.RLock()
	timeout := p.requestTimeout
	p.requestTimeoutMu.RUnlock()
	return timeout
}

// CurHba returns the current tracked pg_hba entries.
func (p *Manager) CurHba() []string {
	return p.curHba
}

// UpdateCurParameters snapshots desired parameters as current parameters.
func (p *Manager) UpdateCurParameters() error {
	n, err := copystructure.Copy(p.parameters)
	if err != nil {
		return fmt.Errorf("snapshot pg parameters: %w", err)
	}
	p.curParameters = n.(common.Parameters)
	return nil
}

// UpdateCurRecoveryOptions snapshots desired recovery options.
func (p *Manager) UpdateCurRecoveryOptions() {
	p.curRecoveryOptions = p.recoveryOptions.DeepCopy()
}

// UpdateCurHba snapshots desired pg_hba entries.
func (p *Manager) UpdateCurHba() error {
	n, err := copystructure.Copy(p.hba)
	if err != nil {
		return fmt.Errorf("snapshot pg_hba: %w", err)
	}
	p.curHba = n.([]string)
	return nil
}

// Init initializes a new PostgreSQL data directory via initdb.
func (p *Manager) Init(initConfig *InitConfig) error {
	// os.CreateTemp creates files with 0600 permissions.
	pwfile, err := os.CreateTemp("", "pwfile")
	if err != nil {
		return err
	}
	defer ignoreRemove(pwfile.Name())
	defer ignoreClose(pwfile)

	if _, err = pwfile.WriteString(p.suPassword); err != nil {
		return err
	}

	name := filepath.Join(p.pgBinPath, "initdb")
	cmd := exec.Command(name, "-D", p.dataDir, "-U", p.suUsername)
	if p.suAuthMethod == "md5" {
		cmd.Args = append(cmd.Args, "--pwfile", pwfile.Name())
	}
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	if initConfig.Locale != "" {
		cmd.Args = append(cmd.Args, "--locale", initConfig.Locale)
	}
	if initConfig.Encoding != "" {
		cmd.Args = append(cmd.Args, "--encoding", initConfig.Encoding)
	}
	if initConfig.DataChecksums {
		cmd.Args = append(cmd.Args, "--data-checksums")
	}

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err = cmd.Run(); err != nil {
		err = fmt.Errorf("error: %v", err)
	}
	// remove the dataDir, so we don't end with an half initialized database
	if err != nil {
		if removeErr := os.RemoveAll(p.dataDir); removeErr != nil {
			return errors.Join(err, fmt.Errorf("remove data dir %q: %w", p.dataDir, removeErr))
		}
		return err
	}
	return nil
}

// Restore restores a data directory using a configured restore command.
func (p *Manager) Restore(command string) error {
	var err error
	var cmd *exec.Cmd

	command = expand(command, p.dataDir)

	if err = os.MkdirAll(p.dataDir, 0700); err != nil {
		err = fmt.Errorf("cannot create data dir: %v", err)
		goto out
	}
	cmd = exec.Command("/bin/sh", "-c", command)
	zl().Debug().Str("path", cmd.Path).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err = cmd.Run(); err != nil {
		err = fmt.Errorf("error: %v", err)
		goto out
	}
	// On every error remove the dataDir, so we don't end with an half initialized database
out:
	if err != nil {
		if removeErr := os.RemoveAll(p.dataDir); removeErr != nil {
			return errors.Join(err, fmt.Errorf("remove data dir %q: %w", p.dataDir, removeErr))
		}
		return err
	}
	return nil
}

// StartTmpMerged starts postgres with a conf file different than
// postgresql.conf, including it at the start of the conf if it exists
// StartTmpMerged starts PostgreSQL with temporary merged configuration.
func (p *Manager) StartTmpMerged() error {
	if err := p.writeConfs(true); err != nil {
		return err
	}
	tmpPostgresConfPath := filepath.Join(p.dataDir, tmpPostgresConf)

	return p.start("-c", "config_file="+tmpPostgresConfPath)
}

// Start starts PostgreSQL with the configured runtime settings.
func (p *Manager) Start() error {
	if err := p.writeConfs(false); err != nil {
		return err
	}
	return p.start()
}

// start starts the instance. A success means that the instance has been
// successfully started BUT doesn't mean that the instance is ready to accept
// connections (i.e. it's waiting for some missing wals etc...).
// Note that also on error an instance may still be active and, if needed,
// should be manually stopped calling Stop.
func (p *Manager) start(args ...string) error {
	// We intentionally start postgres directly here to distinguish between:
	// - failed startup (process exits)
	// - started but not yet ready (for example waiting for WAL)
	// This gives deterministic behavior across supported PostgreSQL majors.

	// A difference between directly calling postgres instead of pg_ctl is that
	// the instance parent is the keeper instead of the defined system reaper
	// (since pg_ctl forks and then exits leaving the postmaster orphaned).

	if err := p.createPostgresqlAutoConf(); err != nil {
		return err
	}

	zl().Info().Msg("starting database")
	name := filepath.Join(p.pgBinPath, "postgres")
	args = append([]string{"-D", p.dataDir, "-c", "unix_socket_directories=" + common.PgUnixSocketDirectories}, args...)
	cmd := exec.Command(name, args...)
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")
	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error: %v", err)
	}

	// execute child wait in a goroutine so we'll wait for it to exit without
	// leaving zombie childs
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	pid := cmd.Process.Pid

	// Wait for the correct pid file to appear or for the process to exit
	ok := false
	start := time.Now()
	for time.Since(start) < startTimeout {
		fh, err := os.Open(filepath.Join(p.dataDir, "postmaster.pid"))
		if err == nil {
			scanner := bufio.NewScanner(fh)
			scanner.Split(bufio.ScanLines)
			if scanner.Scan() {
				fpid := scanner.Text()
				if fpid == strconv.Itoa(pid) {
					ok = true
					if err := fh.Close(); err != nil {
						return fmt.Errorf("close postmaster pid file: %v", err)
					}
					break
				}
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read postmaster pid file: %v", err)
			}
			if err := fh.Close(); err != nil {
				return fmt.Errorf("close postmaster pid file: %v", err)
			}
		}

		select {
		case <-exited:
			return errors.New("postgres exited unexpectedly")
		default:
		}

		time.Sleep(200 * time.Millisecond)
	}

	if !ok {
		return errors.New("instance still starting")
	}

	if err := p.UpdateCurParameters(); err != nil {
		return err
	}
	p.UpdateCurRecoveryOptions()
	if err := p.UpdateCurHba(); err != nil {
		return err
	}

	return nil
}

// Stop tries to stop an instance. An error will be returned if the instance isn't started, stop fails or
// times out (60 second).
// Stop stops PostgreSQL.
func (p *Manager) Stop(fast bool) error {
	zl().Info().Msg("stopping database")
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "stop", "-w", "-D", p.dataDir, "-o", "-c unix_socket_directories="+common.PgUnixSocketDirectories)
	if fast {
		cmd.Args = append(cmd.Args, "-m", "fast")
	}
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error: %v", err)
	}
	return nil
}

// IsStarted reports whether PostgreSQL is currently running.
func (p *Manager) IsStarted() (bool, error) {
	if _, err := os.Stat(p.dataDir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("cannot stat data dir: %w", err)
	}

	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "status", "-D", p.dataDir, "-o", "-c unix_socket_directories="+common.PgUnixSocketDirectories)
	_, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			status := cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
			if status == 3 {
				return false, nil
			}
			if status == 4 {
				return false, ErrUnknownState
			}
		}
		return false, fmt.Errorf("cannot get instance state: %v", err)
	}
	return true, nil
}

// Reload requests PostgreSQL to reload its configuration.
func (p *Manager) Reload() error {
	zl().Info().Msg("reloading database configuration")

	if err := p.writeConfs(false); err != nil {
		return err
	}

	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "reload", "-D", p.dataDir, "-o", "-c unix_socket_directories="+common.PgUnixSocketDirectories)
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error: %v", err)
	}

	if err := p.UpdateCurParameters(); err != nil {
		return err
	}
	p.UpdateCurRecoveryOptions()
	if err := p.UpdateCurHba(); err != nil {
		return err
	}

	return nil
}

// StopIfStarted checks if the instance is started, then calls stop and
// then check if the instance is really stopped
// StopIfStarted stops PostgreSQL only when it is currently running.
func (p *Manager) StopIfStarted(fast bool) error {
	// Stop will return an error if the instance isn't started, so first check
	// if it's started
	started, err := p.IsStarted()
	if err != nil {
		if err == ErrUnknownState {
			// if IsStarted returns an unknown state error then assume that the
			// instance is stopped
			return nil
		}
		return err
	}
	if !started {
		return nil
	}
	if err = p.Stop(fast); err != nil {
		return err
	}
	started, err = p.IsStarted()
	if err != nil {
		return err
	}
	if started {
		return errors.New("failed to stop")
	}
	return nil
}

// Restart restarts PostgreSQL.
func (p *Manager) Restart(fast bool) error {
	zl().Info().Msg("restarting database")
	if err := p.StopIfStarted(fast); err != nil {
		return err
	}
	if err := p.Start(); err != nil {
		return err
	}
	return nil
}

// WaitReady waits until PostgreSQL accepts local connections.
func (p *Manager) WaitReady(timeout time.Duration) error {
	start := time.Now()
	for timeout == 0 || time.Since(start) < timeout {
		if err := p.Ping(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("timeout waiting for db ready")
}

// WaitRecoveryDone waits until recovery mode exits.
func (p *Manager) WaitRecoveryDone(timeout time.Duration) error {
	start := time.Now()
	for timeout == 0 || time.Since(start) < timeout {
		_, err := os.Stat(filepath.Join(p.dataDir, postgresRecoverySignal))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if os.IsNotExist(err) {
			return nil
		}
		time.Sleep(1 * time.Second)
	}

	return errors.New("timeout waiting for db recovery")
}

// Promote promotes a standby to primary.
func (p *Manager) Promote() error {
	zl().Info().Msg("promoting database")

	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "promote", "-w", "-D", p.dataDir)
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error: %v", err)
	}

	if err := p.writeConfs(false); err != nil {
		return err
	}

	return nil
}

// SetupRoles ensures required superuser and replication roles exist.
func (p *Manager) SetupRoles() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()

	if p.suUsername == p.replUsername {
		zl().Info().Msg("adding replication role to superuser")
		if p.suAuthMethod == "trust" {
			if err := alterPasswordlessRole(ctx, p.localConnParams, p.suUsername); err != nil {
				return fmt.Errorf("error adding replication role to superuser: %v", err)
			}
		} else {
			if err := alterRole(ctx, p.localConnParams, p.suUsername, p.suPassword); err != nil {
				return fmt.Errorf("error adding replication role to superuser: %v", err)
			}
		}
		zl().Info().Msg("replication role added to superuser")
	} else {
		// Configure superuser role password if auth method is not trust
		if p.suAuthMethod != "trust" && p.suPassword != "" {
			zl().Info().Msg("setting superuser password")
			if err := setPassword(ctx, p.localConnParams, p.suUsername, p.suPassword); err != nil {
				return fmt.Errorf("error setting superuser password: %v", err)
			}
			zl().Info().Msg("superuser password set")
		}
		zl().Info().Msg("creating replication role")
		if p.replAuthMethod != "trust" {
			if err := createRole(ctx, p.localConnParams, p.replUsername, p.replPassword); err != nil {
				return fmt.Errorf("error creating replication role: %v", err)
			}
		} else {
			if err := createPasswordlessRole(ctx, p.localConnParams, p.replUsername); err != nil {
				return fmt.Errorf("error creating replication role: %v", err)
			}
		}
		zl().Info().Str("role", p.replUsername).Msg("replication role created")
	}
	return nil
}

// GetSyncStandbys returns synchronous standby names currently reported by PostgreSQL.
func (p *Manager) GetSyncStandbys() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getSyncStandbys(ctx, p.localConnParams)
}

// GetReplicationSlots returns replication slot names currently present.
func (p *Manager) GetReplicationSlots() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getReplicationSlots(ctx, p.localConnParams)
}

// GetPhysicalReplicationSlots returns non-temporary physical replication slots.
func (p *Manager) GetPhysicalReplicationSlots() ([]PhysicalReplicationSlot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getPhysicalReplicationSlots(ctx, p.localConnParams)
}

// CreateReplicationSlot creates a physical replication slot.
func (p *Manager) CreateReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return createReplicationSlot(ctx, p.localConnParams, name)
}

// DropReplicationSlot removes a replication slot.
func (p *Manager) DropReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return dropReplicationSlot(ctx, p.localConnParams, name)
}

// GetLogicalReplicationSlots returns non-temporary logical replication slots.
func (p *Manager) GetLogicalReplicationSlots() ([]LogicalReplicationSlot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getLogicalReplicationSlots(ctx, p.localConnParams)
}

// CreateLogicalReplicationSlot creates a logical replication slot.
func (p *Manager) CreateLogicalReplicationSlot(name, database, plugin string, failover bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return createLogicalReplicationSlot(ctx, p.localConnParams, name, database, plugin, failover)
}

// DropLogicalReplicationSlot removes a logical replication slot.
func (p *Manager) DropLogicalReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return dropLogicalReplicationSlot(ctx, p.localConnParams, name)
}

// IsWALReplayPaused reports whether WAL replay is currently paused.
func (p *Manager) IsWALReplayPaused() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return isWALReplayPaused(ctx, p.localConnParams)
}

// ResumeWALReplay resumes WAL replay when recovery is paused.
func (p *Manager) ResumeWALReplay() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return resumeWALReplay(ctx, p.localConnParams)
}

// BinaryVersion returns the PostgreSQL binary major and minor version.
func (p *Manager) BinaryVersion() (int, int, error) {
	name := filepath.Join(p.pgBinPath, "postgres")
	cmd := exec.Command(name, "-V")
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf("error: %v, output: %s", err, string(out))
	}

	return ParseBinaryVersion(string(out))
}

// PGDataVersion returns the data directory PostgreSQL major and minor version.
func (p *Manager) PGDataVersion() (int, int, error) {
	fh, err := os.Open(filepath.Join(p.dataDir, "PG_VERSION"))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read PG_VERSION: %v", err)
	}
	defer ignoreClose(fh)

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()

	version := scanner.Text()
	return ParseVersion(version)
}

// IsInitialized reports whether the data directory is initialized.
func (p *Manager) IsInitialized() (bool, error) {
	// List of required files or directories relative to postgres data dir
	// based on PostgreSQL storage layout, with additions used by Hysteron.
	// Keep this list aligned with currently supported PostgreSQL majors.
	exists, err := fileExists(filepath.Join(p.dataDir, "PG_VERSION"))
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	requiredFiles := make([]string, 0, 18)
	requiredFiles = append(requiredFiles,
		"PG_VERSION",
		"base",
		"global",
		"pg_dynshmem",
		"pg_logical",
		"pg_multixact",
		"pg_notify",
		"pg_replslot",
		"pg_serial",
		"pg_snapshots",
		"pg_stat",
		"pg_stat_tmp",
		"pg_subtrans",
		"pg_tblspc",
		"pg_twophase",
		"global/pg_control",
		"pg_xact",
		"pg_wal",
	)
	for _, f := range requiredFiles {
		exists, err := fileExists(filepath.Join(p.dataDir, f))
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// GetRole return the current instance role
func (p *Manager) GetRole() (common.Role, error) {
	// if standby.signal file exists then consider it as a standby
	_, err := os.Stat(filepath.Join(p.dataDir, postgresStandbySignal))
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("error determining if %q file exists: %v", postgresStandbySignal, err)
	}
	if os.IsNotExist(err) {
		return common.RoleMaster, nil
	}
	return common.RoleStandby, nil
}

func (p *Manager) writeConfs(useTmpPostgresConf bool) error {
	if err := p.writeConf(useTmpPostgresConf); err != nil {
		return fmt.Errorf("error writing %s file: %v", postgresConf, err)
	}
	if err := p.writePgHba(); err != nil {
		return fmt.Errorf("error writing pg_hba.conf file: %v", err)
	}
	if err := p.writeStandbySignal(); err != nil {
		return fmt.Errorf("error writing %s file: %v", postgresStandbySignal, err)
	}
	if err := p.writeRecoverySignal(); err != nil {
		return fmt.Errorf("error writing %s file: %v", postgresRecoverySignal, err)
	}
	return nil
}

func (p *Manager) writeConf(useTmpPostgresConf bool) error {
	confFile := postgresConf
	if useTmpPostgresConf {
		confFile = tmpPostgresConf
	}

	return fs.WriteFileAtomicFunc(filepath.Join(p.dataDir, confFile), 0600,
		func(f io.Writer) error {
			if useTmpPostgresConf {
				// include postgresql.conf if it exists
				_, err := os.Stat(filepath.Join(p.dataDir, postgresConf))
				if err != nil && !os.IsNotExist(err) {
					return err
				}
				if !os.IsNotExist(err) {
					if _, err := fmt.Fprintf(f, "include '%s'\n", postgresConf); err != nil {
						return err
					}
				}
			}
			for k, v := range p.parameters {
				// Single quotes needs to be doubled
				ev := strings.ReplaceAll(v, `'`, `''`)
				if _, err := fmt.Fprintf(f, "%s = '%s'\n", k, ev); err != nil {
					return err
				}
			}

			// write recovery parameters only if recoveryMode is not none
			if p.recoveryOptions.RecoveryMode != RecoveryModeNone {
				for n, v := range p.recoveryOptions.RecoveryParameters {
					if _, err := fmt.Fprintf(f, "%s = '%s'\n", n, v); err != nil {
						return err
					}
				}
			}

			return nil
		})
}

func (p *Manager) writeStandbySignal() error {
	// write standby.signal only if recoveryMode is standby
	if p.recoveryOptions.RecoveryMode != RecoveryModeStandby {
		return nil
	}

	zl().Info().Msg("writing standby signal file")

	return fs.WriteFileAtomicFunc(filepath.Join(p.dataDir, postgresStandbySignal), 0600,
		func(_ io.Writer) error {
			return nil
		})
}

func (p *Manager) writeRecoverySignal() error {
	// write standby.signal only if recoveryMode is recovery
	if p.recoveryOptions.RecoveryMode != RecoveryModeRecovery {
		return nil
	}

	zl().Info().Msg("writing recovery signal file")

	return fs.WriteFileAtomicFunc(filepath.Join(p.dataDir, postgresRecoverySignal), 0600,
		func(_ io.Writer) error {
			return nil
		})
}

func (p *Manager) writePgHba() error {
	return fs.WriteFileAtomicFunc(filepath.Join(p.dataDir, "pg_hba.conf"), 0600,
		func(f io.Writer) error {
			if p.hba != nil {
				for _, e := range p.hba {
					if _, err := f.Write([]byte(e + "\n")); err != nil {
						return err
					}
				}
			}
			return nil
		})
}

// createPostgresqlAutoConf creates postgresql.auto.conf as a symlink to
// /dev/null to block alter systems commands (they'll return an error)
func (p *Manager) createPostgresqlAutoConf() error {
	pgAutoConfPath := filepath.Join(p.dataDir, postgresAutoConf)
	if err := os.Remove(pgAutoConfPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing postgresql.auto.conf file: %v", err)
	}
	if err := os.Symlink("/dev/null", pgAutoConfPath); err != nil {
		return fmt.Errorf("error symlinking postgresql.auto.conf file to /dev/null: %v", err)
	}
	return nil
}

// SyncFromFollowedPGRewind synchronizes from a source using pg_rewind.
func (p *Manager) SyncFromFollowedPGRewind(followedConnParams ConnParams, password string) error {
	// Remove postgresql.auto.conf since pg_rewind will error if it's a symlink to /dev/null
	pgAutoConfPath := filepath.Join(p.dataDir, postgresAutoConf)
	if err := os.Remove(pgAutoConfPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing postgresql.auto.conf file: %v", err)
	}

	// os.CreateTemp creates files with 0600 permissions.
	pgpass, err := os.CreateTemp("", "pgpass")
	if err != nil {
		return err
	}
	defer ignoreRemove(pgpass.Name())
	defer ignoreClose(pgpass)

	host := followedConnParams.Get("host")
	port := followedConnParams.Get("port")
	user := followedConnParams.Get("user")
	if _, err := fmt.Fprintf(pgpass, "%s:%s:*:%s:%s\n", host, port, user, password); err != nil {
		return err
	}

	// Disable synchronous commits. pg_rewind needs to create a
	// temporary table on the master but if synchronous replication is
	// enabled and there're no active standbys it will hang.
	followedConnParams.Set("options", "-c synchronous_commit=off")
	followedConnString := followedConnParams.ConnString()

	zl().Info().Msg("running pg_rewind")
	name := filepath.Join(p.pgBinPath, "pg_rewind")
	cmd := exec.Command(name, "--debug", "-D", p.dataDir, "--source-server="+followedConnString)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+pgpass.Name())
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error: %v", err)
	}
	return nil
}

// SyncFromFollowed synchronizes from a source using pg_basebackup.
func (p *Manager) SyncFromFollowed(followedConnParams ConnParams, replSlot string) error {
	fcp := followedConnParams.Copy()

	// os.CreateTemp creates files with 0600 permissions.
	pgpass, err := os.CreateTemp("", "pgpass")
	if err != nil {
		return err
	}
	defer ignoreRemove(pgpass.Name())
	defer ignoreClose(pgpass)

	host := fcp.Get("host")
	port := fcp.Get("port")
	user := fcp.Get("user")
	password := fcp.Get("password")
	if _, err = fmt.Fprintf(pgpass, "%s:%s:*:%s:%s\n", host, port, user, password); err != nil {
		return err
	}

	// Remove password from the params passed to pg_basebackup
	fcp.Del("password")

	// Disable synchronous commits. pg_basebackup calls
	// pg_start_backup()/pg_stop_backup() on the master but if synchronous
	// replication is enabled and there're no active standbys they will hang.
	fcp.Set("options", "-c synchronous_commit=off")
	followedConnString := fcp.ConnString()

	zl().Info().Msg("running pg_basebackup")
	name := filepath.Join(p.pgBinPath, "pg_basebackup")
	args := []string{"-R", "-v", "-P", "-Xs", "-D", p.dataDir, "-d", followedConnString}
	if replSlot != "" {
		args = append(args, "--slot", replSlot)
	}
	cmd := exec.Command(name, args...)

	cmd.Env = append(os.Environ(), "PGPASSFILE="+pgpass.Name())
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")

	// Pipe pg_basebackup's stderr to our stderr.
	// We do this indirectly so that pg_basebackup doesn't think it's connected to a tty.
	// This ensures that it doesn't print any bare line feeds, which could corrupt other
	// logs.
	// pg_basebackup uses stderr for diagnostic messages and stdout for streaming the backup
	// itself (in some modes; we don't use this). As a result we only need to deal with
	// stderr.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	go func() {
		if _, err := io.Copy(os.Stderr, stderr); err != nil {
			zl().Error().Err(err).Msg("pg_basebackup failed to copy stderr")
		}
	}()

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

// RemoveAll removes the managed PostgreSQL data directory.
func (p *Manager) RemoveAll() error {
	initialized, err := p.IsInitialized()
	if err != nil {
		return fmt.Errorf("failed to retrieve instance state: %v", err)
	}
	started := false
	if initialized {
		var err error
		started, err = p.IsStarted()
		if err != nil {
			return fmt.Errorf("failed to retrieve instance state: %v", err)
		}
	}
	if started {
		return errors.New("cannot remove postregsql database. Instance is active")
	}
	return os.RemoveAll(p.dataDir)
}

// GetSystemData returns current PostgreSQL system data.
func (p *Manager) GetSystemData() (*SystemData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return GetSystemData(ctx, p.replConnParams)
}

// GetTimelinesHistory returns timeline history records up to timeline.
func (p *Manager) GetTimelinesHistory(timeline uint64) ([]*TimelineHistory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getTimelinesHistory(ctx, timeline, p.replConnParams)
}

// GetConfigFilePGParameters returns PostgreSQL parameters read from config files.
func (p *Manager) GetConfigFilePGParameters() (common.Parameters, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getConfigFilePGParameters(ctx, p.localConnParams)
}

// Ping checks PostgreSQL readiness through a local connection.
func (p *Manager) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return ping(ctx, p.localConnParams)
}

// OlderWalFile returns the oldest WAL filename needed by configured replication.
func (p *Manager) OlderWalFile() (string, error) {
	walDir := "pg_wal"
	f, err := os.Open(filepath.Join(p.dataDir, walDir))
	if err != nil {
		return "", err
	}
	names, err := f.Readdirnames(-1)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	sort.Strings(names)

	for _, name := range names {
		if IsWalFileName(name) {
			fi, err := os.Stat(filepath.Join(p.dataDir, walDir, name))
			if err != nil {
				return "", err
			}
			// if the file size is different from the currently supported one
			// (16Mib) return without checking other possible wal files
			if fi.Size() != WalSegSize {
				return "", fmt.Errorf("wal file has unsupported size: %d", fi.Size())
			}
			return name, nil
		}
	}

	return "", nil
}

// IsRestartRequired returns if a postgres restart is necessary.
func (p *Manager) IsRestartRequired() (bool, error) {
	requirement, err := p.IsRestartRequiredDetailed()
	if err != nil {
		return false, err
	}
	return requirement.Required, nil
}

// IsRestartRequiredDetailed returns whether a restart is required plus the
// list of pending-restart parameter names currently reported by PostgreSQL.
func (p *Manager) IsRestartRequiredDetailed() (*RestartRequirement, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return restartRequirementUsingPendingRestart(ctx, p.localConnParams)
}

func ignoreClose(c io.Closer) {
	_ = c.Close()
}

func ignoreRemove(name string) {
	_ = os.Remove(name)
}
