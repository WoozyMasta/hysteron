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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/woozymasta/hysteron/internal/common"
)

// RemoveAll removes the managed PostgreSQL data directory.
func (p *Manager) RemoveAll() error {
	initialized, err := p.IsInitialized()
	if err != nil {
		return fmt.Errorf("failed to retrieve instance state: %v", err)
	}

	started := false
	if initialized {
		started, err = p.IsStarted()
		if err != nil {
			return fmt.Errorf("failed to retrieve instance state: %v", err)
		}
	}

	if started {
		return errors.New("cannot remove postregsql database. Instance is active")
	}

	return p.removeManagedDirs()
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
	directory, err := os.Open(p.walDir)
	if err != nil {
		return "", err
	}
	names, err := directory.Readdirnames(-1)
	if closeErr := directory.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	sort.Strings(names)

	for _, name := range names {
		if IsWalFileName(name) {
			fileInfo, err := os.Stat(filepath.Join(p.walDir, name))
			if err != nil {
				return "", err
			}
			// if the file size is different from the currently supported one
			// (16Mib) return without checking other possible wal files
			if fileInfo.Size() != WalSegSize {
				return "", fmt.Errorf("wal file has unsupported size: %d", fileInfo.Size())
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

// GetStandbyStatus returns WAL receiver/replay state for standby instances.
func (p *Manager) GetStandbyStatus() (*StandbyStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getStandbyStatus(ctx, p.localConnParams)
}
