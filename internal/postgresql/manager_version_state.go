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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/woozymasta/hysteron/internal/common"
)

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

	for _, requiredFile := range requiredFiles {
		exists, err := fileExists(filepath.Join(p.dataDir, requiredFile))
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}

	return true, nil
}

// GetRole return the current instance role.
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
