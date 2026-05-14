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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/woozymasta/hysteron/internal/common"
)

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
	args = append(
		[]string{"-D", p.dataDir, "-c", "unix_socket_directories=" + common.PgUnixSocketDirectories},
		args...,
	)
	cmd := exec.Command(name, args...)
	zl().Debug().Str("path", name).Strs("args", cmd.Args).Msg("execing cmd")
	// Pipe command's std[err|out] to parent.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error: %v", err)
	}

	// Execute child wait in a goroutine so we'll wait for it to exit without
	// leaving zombie childs.
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	pid := cmd.Process.Pid

	// Wait for the correct pid file to appear or for the process to exit.
	ok := false
	start := time.Now()
	for time.Since(start) < startTimeout {
		fh, err := os.Open(filepath.Join(p.dataDir, "postmaster.pid"))
		if err == nil {
			scanner := bufio.NewScanner(fh)
			scanner.Split(bufio.ScanLines)
			if scanner.Scan() {
				filePID := scanner.Text()
				if filePID == strconv.Itoa(pid) {
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

// StopIfStarted stops PostgreSQL only when it is currently running.
func (p *Manager) StopIfStarted(fast bool) error {
	// Stop will return an error if the instance isn't started, so first check
	// if it's started.
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
		// Configure superuser role password if auth method is not trust.
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
