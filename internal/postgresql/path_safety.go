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
	"slices"
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
	for _, root := range p.tablespaceDirRoots {
		if err := validateRemovablePath(root, "tablespace dir root"); err != nil {
			return err
		}
		if hasPathPrefix(root, p.dataDir) || hasPathPrefix(p.dataDir, root) {
			return fmt.Errorf("tablespace dir root %q cannot overlap postgres data dir %q", root, p.dataDir)
		}
		if hasPathPrefix(root, p.walDir) || hasPathPrefix(p.walDir, root) {
			return fmt.Errorf("tablespace dir root %q cannot overlap wal dir %q", root, p.walDir)
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
	if err := p.removeManagedTablespaceTargets(); err != nil {
		return err
	}
	if err := os.RemoveAll(p.dataDir); err != nil {
		return err
	}
	if p.walDirConfigured && p.walDir != p.dataDir {
		if err := os.RemoveAll(p.walDir); err != nil {
			return err
		}
	}
	return nil
}

func (p *Manager) removeManagedTablespaceTargets() error {
	if len(p.tablespaceDirRoots) == 0 {
		return nil
	}
	pgTblspcDir := filepath.Join(p.dataDir, "pg_tblspc")
	entries, err := os.ReadDir(pgTblspcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read pg_tblspc dir: %w", err)
	}

	seen := make([]string, 0, len(entries))
	for _, entry := range entries {
		linkPath := filepath.Join(pgTblspcDir, entry.Name())
		target, err := os.Readlink(linkPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read tablespace symlink %q: %w", linkPath, err)
		}
		resolved := target
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(pgTblspcDir, resolved)
		}
		resolved = filepath.Clean(resolved)
		if !p.isManagedTablespacePath(resolved) {
			continue
		}
		if slices.Contains(seen, resolved) {
			continue
		}
		if err := os.RemoveAll(resolved); err != nil {
			return fmt.Errorf("remove managed tablespace dir %q: %w", resolved, err)
		}
		seen = append(seen, resolved)
	}
	return nil
}

func (p *Manager) isManagedTablespacePath(path string) bool {
	keeperOwnedPrefix := filepath.Base(filepath.Dir(p.dataDir))
	for _, root := range p.tablespaceDirRoots {
		ownedRoot := filepath.Join(root, keeperOwnedPrefix)
		if hasPathPrefix(path, ownedRoot) {
			return true
		}
	}
	return false
}
