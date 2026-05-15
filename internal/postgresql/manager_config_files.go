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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/woozymasta/hysteron/internal/utils/fs"
)

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
		func(writer io.Writer) error {
			if useTmpPostgresConf {
				// include postgresql.conf if it exists
				_, err := os.Stat(filepath.Join(p.dataDir, postgresConf))
				if err != nil && !os.IsNotExist(err) {
					return err
				}
				if !os.IsNotExist(err) {
					if _, err := fmt.Fprintf(writer, "include '%s'\n", postgresConf); err != nil {
						return err
					}
				}
			}
			for key, value := range p.parameters {
				// Single quotes needs to be doubled
				escapedValue := strings.ReplaceAll(value, `'`, `''`)
				if _, err := fmt.Fprintf(writer, "%s = '%s'\n", key, escapedValue); err != nil {
					return err
				}
			}

			// write recovery parameters only if recoveryMode is not none
			if p.recoveryOptions.RecoveryMode != RecoveryModeNone {
				for key, value := range p.recoveryOptions.RecoveryParameters {
					if _, err := fmt.Fprintf(writer, "%s = '%s'\n", key, value); err != nil {
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
		func(writer io.Writer) error {
			if p.hba != nil {
				for _, entry := range p.hba {
					if _, err := writer.Write([]byte(entry + "\n")); err != nil {
						return err
					}
				}
			}
			return nil
		})
}

// createPostgresqlAutoConf creates postgresql.auto.conf as a symlink to
// /dev/null to block alter systems commands (they'll return an error).
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
