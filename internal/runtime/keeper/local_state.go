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
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/woozymasta/hysteron/internal/utils/fs"
)

// keeperLocalStateFilePath returns the local keeper state file path in data dir.
func (p *PostgresKeeper) keeperLocalStateFilePath() string {
	return filepath.Join(p.cfg.DataDir, "keeperstate")
}

// loadKeeperLocalState reads persisted keeper identity/state from disk into
// in-memory keeper state.
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

// saveKeeperLocalState persists keeper identity/state atomically to disk.
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

// dbLocalStateFilePath returns the local DB state file path in keeper data dir.
func (p *PostgresKeeper) dbLocalStateFilePath() string {
	return filepath.Join(p.cfg.DataDir, "dbstate")
}

// loadDBLocalState reads persisted per-DB keeper state from disk.
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

// saveDBLocalState persists the provided DB local state and updates in-memory
// copy only after successful atomic write.
func (p *PostgresKeeper) saveDBLocalState(nextDBLocalState *DBLocalState) error {
	sj, err := json.Marshal(nextDBLocalState)
	if err != nil {
		return err
	}
	if err = fs.WriteFileAtomic(p.dbLocalStateFilePath(), 0600, sj); err != nil {
		return err
	}

	p.localStateMutex.Lock()
	p.dbLocalState = nextDBLocalState.DeepCopy()
	p.localStateMutex.Unlock()

	return nil
}

// dbLocalStateCopy returns a deep-copied snapshot of in-memory DB local state.
func (p *PostgresKeeper) dbLocalStateCopy() *DBLocalState {
	p.localStateMutex.Lock()
	defer p.localStateMutex.Unlock()
	return p.dbLocalState.DeepCopy()
}
