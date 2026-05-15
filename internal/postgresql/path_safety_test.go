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
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateRemovablePath(t *testing.T) {
	t.Run("rejects empty", func(t *testing.T) {
		if err := validateRemovablePath("", "data dir"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("rejects relative", func(t *testing.T) {
		if err := validateRemovablePath("relative/path", "data dir"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("rejects root", func(t *testing.T) {
		root := string(filepath.Separator)
		if runtime.GOOS == "windows" {
			root = filepath.VolumeName(os.Getenv("SystemRoot")) + `\`
		}
		if err := validateRemovablePath(root, "data dir"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestManagerValidateManagedDirs(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data", "postgres")
	defaultWALDir := filepath.Join(dataDir, "pg_wal")

	t.Run("accepts default wal dir", func(t *testing.T) {
		p := &Manager{
			dataDir:          dataDir,
			walDir:           defaultWALDir,
			walDirConfigured: false,
		}
		if err := p.validateManagedDirs(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("rejects configured wal dir under pgdata", func(t *testing.T) {
		p := &Manager{
			dataDir:          dataDir,
			walDir:           filepath.Join(dataDir, "custom_wal"),
			walDirConfigured: true,
		}
		if err := p.validateManagedDirs(); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestRemoveManagedDirs(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data", "postgres")
	walDir := filepath.Join(base, "wal")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(walDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("18"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "x"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	p := &Manager{
		dataDir:          dataDir,
		walDir:           walDir,
		walDirConfigured: true,
	}
	if err := p.removeManagedDirs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("expected data dir removed, got: %v", err)
	}
	if _, err := os.Stat(walDir); !os.IsNotExist(err) {
		t.Fatalf("expected wal dir removed, got: %v", err)
	}
}

func TestRemoveManagedDirsKeepsTablespaceTargets(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data", "postgres")
	walDir := filepath.Join(base, "wal")
	managedRoot := filepath.Join(base, "managed-tblspc")
	managedTblspc := filepath.Join(managedRoot, "ts1")
	unmanagedRoot := filepath.Join(base, "unmanaged-tblspc")
	unmanagedTblspc := filepath.Join(unmanagedRoot, "ts2")
	pgTblspc := filepath.Join(dataDir, "pg_tblspc")

	for _, path := range []string{
		dataDir, walDir, managedTblspc, unmanagedTblspc, pgTblspc,
	} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.Symlink(managedTblspc, filepath.Join(pgTblspc, "12345")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(unmanagedTblspc, filepath.Join(pgTblspc, "12346")); err != nil {
		t.Fatal(err)
	}

	p := &Manager{
		dataDir:            dataDir,
		walDir:             walDir,
		walDirConfigured:   true,
		tablespaceDirRoots: []string{managedRoot},
	}
	if err := p.removeManagedTablespaceDirs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(managedTblspc); err != nil {
		t.Fatalf("expected managed tablespace dir to remain, got: %v", err)
	}
	if _, err := os.Stat(unmanagedTblspc); err != nil {
		t.Fatalf("expected unmanaged tablespace dir to remain, got: %v", err)
	}
}
