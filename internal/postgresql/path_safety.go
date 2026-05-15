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
	"os"
	"path/filepath"
	"strings"
)

func (p *Manager) validateManagedDirs() error {
	if err := validateRemovablePath(p.dataDir, "data dir"); err != nil {
		return err
	}
	if err := validateRemovablePath(p.walDir, "wal dir"); err != nil {
		return err
	}

	defaultWALDir := filepath.Join(p.dataDir, "pg_wal")
	if p.walDirConfigured {
		// Explicit WAL directory must not overlap with PGDATA unless it's the
		// canonical pg_wal location.
		if p.walDir != defaultWALDir && hasPathPrefix(p.walDir, p.dataDir) {
			return fmt.Errorf("configured wal dir %q cannot be inside postgres data dir %q", p.walDir, p.dataDir)
		}
		if hasPathPrefix(p.dataDir, p.walDir) {
			return fmt.Errorf("postgres data dir %q cannot be inside configured wal dir %q", p.dataDir, p.walDir)
		}
	}
	return nil
}

func validateRemovablePath(path string, kind string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("%s is empty", kind)
	}
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("%s %q must be absolute", kind, path)
	}
	if cleaned == "." {
		return fmt.Errorf("%s %q is unsafe", kind, path)
	}
	if filepath.Dir(cleaned) == cleaned {
		return fmt.Errorf("%s %q points to filesystem root and is unsafe", kind, path)
	}
	return nil
}

func hasPathPrefix(path, prefix string) bool {
	rel, err := filepath.Rel(prefix, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (p *Manager) removeManagedDirs() error {
	if err := p.validateManagedDirs(); err != nil {
		return err
	}
	if err := osRemoveAll(p.dataDir); err != nil {
		return err
	}
	if p.walDirConfigured && p.walDir != p.dataDir {
		if err := osRemoveAll(p.walDir); err != nil {
			return err
		}
	}
	return nil
}

var osRemoveAll = func(path string) error {
	return os.RemoveAll(path)
}
