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
	"testing"
)

func TestWritePgIdent(t *testing.T) {
	dataDir := t.TempDir()
	m := &Manager{
		dataDir: dataDir,
		ident: []string{
			"mymap  postgres   postgres",
			"mymap  repl_user  postgres",
		},
	}

	if err := m.writePgIdent(); err != nil {
		t.Fatalf("writePgIdent() error: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(dataDir, "pg_ident.conf"))
	if err != nil {
		t.Fatalf("read pg_ident.conf: %v", err)
	}

	want := "mymap  postgres   postgres\nmymap  repl_user  postgres\n"
	if string(content) != want {
		t.Fatalf("pg_ident.conf mismatch\nwant:\n%s\ngot:\n%s", want, string(content))
	}
}
