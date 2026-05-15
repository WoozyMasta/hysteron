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
	"path/filepath"
	"strings"
	"testing"
)

func TestTablespaceMappedPathIsKeeperScopedAndStable(t *testing.T) {
	p := &Manager{
		dataDir: filepath.Join("C:\\tmp", "stkeeper01", "postgres"),
	}

	root := filepath.Join("D:\\pg", "tablespaces")
	oldPath := filepath.Join("D:\\pg", "shared", "ts-main")

	mapped1 := p.tablespaceMappedPath(root, oldPath)
	mapped2 := p.tablespaceMappedPath(root, oldPath)
	if mapped1 != mapped2 {
		t.Fatalf("expected stable mapping, got %q and %q", mapped1, mapped2)
	}
	if !strings.Contains(mapped1, "stkeeper01") {
		t.Fatalf("expected keeper scope in mapped path, got %q", mapped1)
	}
	if !strings.HasPrefix(mapped1, root) {
		t.Fatalf("expected mapped path under root %q, got %q", root, mapped1)
	}
}

func TestSanitizePathComponent(t *testing.T) {
	got := sanitizePathComponent(`ts:name with spaces/and\slashes`)
	if strings.ContainsAny(got, `\/: `) {
		t.Fatalf("expected sanitized path component, got %q", got)
	}
}
