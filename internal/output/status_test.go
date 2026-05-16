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

package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/woozymasta/hysteron/internal/app"
)

func TestWriteStatusJSON(t *testing.T) {
	var b bytes.Buffer
	status := app.Status{
		Cluster: app.ClusterStatus{
			Available:       true,
			MasterKeeperUID: "keeper-1",
			MasterDBUID:     "db-1",
		},
	}
	if err := WriteStatus(&b, FormatJSON, status); err != nil {
		t.Fatalf("write status: %v", err)
	}
	out := b.String()
	if !strings.Contains(out, "\"cluster\"") {
		t.Fatalf("expected cluster key, got %q", out)
	}
	if !strings.Contains(out, "\"master_keeper_uid\": \"keeper-1\"") {
		t.Fatalf("expected master keeper uid, got %q", out)
	}
}

func TestWriteStatusPlain(t *testing.T) {
	var b bytes.Buffer
	status := app.Status{
		Cluster: app.ClusterStatus{
			Available:       true,
			MasterKeeperUID: "keeper-1",
			MasterDBUID:     "db-1",
		},
		Sentinels: []app.SentinelStatus{{UID: "s1", Leader: true}},
		Proxies: []app.ProxyStatus{{
			UID:        "p1",
			Mode:       "write+read",
			Listeners:  "writable=127.0.0.1:5432(up), read-only=127.0.0.1:6432(up)",
			Generation: 4,
		}},
		Keepers: []app.KeeperStatus{{
			UID:                 "keeper-1",
			ListenAddress:       "10.0.0.1:5432",
			MasterPriority:      200,
			Healthy:             true,
			PgHealthy:           true,
			PgWantedGeneration:  3,
			PgCurrentGeneration: 3,
		}},
		KeeperTree: []app.KeeperTreeNode{{Label: "keeper-1 (master)", Level: 0}},
	}
	if err := WriteStatus(&b, FormatPlain, status); err != nil {
		t.Fatalf("write status: %v", err)
	}
	out := b.String()
	if !strings.Contains(out, " Cluster ") {
		t.Fatalf("expected cluster section, got %q", out)
	}
	if !strings.Contains(out, " Keeper Tree ") {
		t.Fatalf("expected keeper tree section, got %q", out)
	}
	if !strings.Contains(out, "keeper-1 (master)") {
		t.Fatalf("expected tree line, got %q", out)
	}
	if !strings.Contains(out, "write+read") {
		t.Fatalf("expected proxy mode in output, got %q", out)
	}
	if !strings.Contains(out, "MASTER PRIORITY") || !strings.Contains(out, "200") {
		t.Fatalf("expected master priority in output, got %q", out)
	}
	if !strings.Contains(out, "ROWS") {
		t.Fatalf("expected footer rows, got %q", out)
	}
}

func TestWriteStatusUnsupportedFormat(t *testing.T) {
	err := WriteStatus(&bytes.Buffer{}, Format("toml"), app.Status{})
	if !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
}
