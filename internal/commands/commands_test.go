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

package commands

import (
	"errors"
	"strings"
	"testing"

	"github.com/woozymasta/flags"
	"github.com/woozymasta/hysteron/internal/app"
)

func TestKeeperCommandReturnsValidationError(t *testing.T) {
	tests := []struct {
		args []string
	}{
		{args: []string{"keeper", "etcd"}},
		{args: []string{"keeper", "etcdv3"}},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.args, "_"), func(t *testing.T) {
			parser := newTestParser()
			_, err := parser.ParseArgs(tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "data directory is required") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestSentinelWithoutClusterNameFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"sentinel", "kubernetes"})
	if err == nil {
		t.Fatal("expected sentinel error")
	}
	if !strings.Contains(err.Error(), "cluster name required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProxyWithoutClusterNameFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"proxy", "k8s"})
	if err == nil {
		t.Fatal("expected proxy error")
	}
	if !strings.Contains(err.Error(), "cluster name required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuntimeCommandPassthroughArgsToComponentParser(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"keeper", "etcd",
		"--cluster-name", "test",
		"--etcd-endpoints", "127.0.0.1:2379",
		"--",
		"--not-real-keeper-flag",
	})
	if err == nil {
		t.Fatal("expected keeper passthrough parse error")
	}
	if !strings.Contains(err.Error(), "unknown flag `not-real-keeper-flag`") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuntimeCommandPassthroughKnownKeeperFlag(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"keeper", "etcd",
		"--cluster-name", "test",
		"--etcd-endpoints", "127.0.0.1:2379",
		"--",
		"--data-dir", t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected keeper validation error")
	}
	if strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected passthrough parsing failure: %v", err)
	}
	if !strings.Contains(err.Error(), "postgresql listen address is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterPromoteRequiresConfirmation(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "promote"})
	if err == nil {
		t.Fatal("expected promote error")
	}
	if !errors.Is(err, app.ErrConfirmationRequired) {
		t.Fatalf("expected ErrConfirmationRequired, got %v", err)
	}
}

func TestClusterKeeperRemoveWithoutStoreBackendFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test",
		"keeper", "remove", "--keeper-uid", "keeper-01",
	})
	if err == nil {
		t.Fatal("expected remove keeper error")
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFailoverKeeperWithoutStoreBackendFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"failover", "--cluster-name", "test",
		"keeper", "--keeper-uid", "keeper-01",
	})
	if err == nil {
		t.Fatal("expected failover keeper error")
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMissingCommandFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{})
	if err == nil {
		t.Fatal("expected missing command error")
	}
}

func TestClusterKeeperRemoveRequiresUID(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "keeper", "remove"})
	if err == nil {
		t.Fatal("expected required keeper-uid error")
	}
}

func TestClusterListWithoutStoreBackendFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "list"})
	if err == nil {
		t.Fatal("expected list error")
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterStatusWithoutStoreBackendFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "--cluster-name", "test", "status"})
	if err == nil {
		t.Fatal("expected status error")
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterSpecificationWithoutStoreBackendFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "--cluster-name", "test", "specification"})
	if err == nil {
		t.Fatal("expected specification error")
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterDataReadRequiresClusterName(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{"cluster", "data", "read"})
	if err == nil {
		t.Fatal("expected cluster name error")
	}
	if !strings.Contains(err.Error(), "cluster name required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func newTestParser() *flags.Parser {
	cfg = newRootCommand()
	parser := NewParser()
	parser.Options &^= flags.PrintErrors
	return parser
}
