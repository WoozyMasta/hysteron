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
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
)

// stopPostgresIfStarted executes before-stop hook and then stops PostgreSQL if
// a local instance is currently running.
func (p *PostgresKeeper) stopPostgresIfStarted(pgm *postgresql.Manager, db *cluster.DB) error {
	p.runBeforeStopHook(db)
	return pgm.StopIfStarted(true)
}

// runBeforeStopHook executes the optional per-DB command before keeper stops
// the local PostgreSQL instance.
func (p *PostgresKeeper) runBeforeStopHook(db *cluster.DB) {
	if db == nil || db.Spec == nil {
		return
	}
	_ = p.runHookCommand(db, db.Spec.BeforeStopCommand, "before-stop")
}

// runPrePromoteHook executes the optional per-DB command before promotion and
// returns hook execution errors to the caller.
func (p *PostgresKeeper) runPrePromoteHook(db *cluster.DB) error {
	if db == nil || db.Spec == nil {
		return nil
	}
	return p.runHookCommand(db, db.Spec.PrePromoteCommand, "pre-promote")
}

// runHookCommand runs a configured keeper hook through the OS shell:
// `cmd /C` on Windows and `/bin/sh -c` on Unix-like systems.
// Empty hook commands are treated as no-op.
func (p *PostgresKeeper) runHookCommand(db *cluster.DB, command string, hookName string) error {
	if db == nil || db.Spec == nil {
		return nil
	}

	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}

	p.baseLog().
		Info().
		Str(log.FieldDBUID, db.UID).
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
			Str(log.FieldDBUID, db.UID).
			Str("hook", hookName).
			Str("hook_command", command).
			Msg("keeper hook command failed")
		return err
	}

	p.baseLog().
		Info().
		Str(log.FieldDBUID, db.UID).
		Str("hook", hookName).
		Msg("keeper hook command completed")
	return nil
}
