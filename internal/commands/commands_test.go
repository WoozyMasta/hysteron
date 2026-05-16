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

func TestKeeperUnknownFlagFails(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"keeper", "etcd",
		"--data-dir", t.TempDir(),
		"--pg-listen-address", "127.0.0.1",
		"--pg-repl-username", "repl",
		"--pg-repl-password", "replpw",
		"--pg-su-password", "supw",
		"--cluster-name", "test",
		"--etcd-endpoints", "127.0.0.1:2379",
		"--not-real-keeper-flag",
	})
	if err == nil {
		t.Fatal("expected unknown keeper flag error")
	}
	if !strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestKeeperRunsWithoutPassthroughSeparator(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"keeper", "etcd",
		"--data-dir", t.TempDir(),
		"--pg-listen-address", "127.0.0.1",
		"--pg-repl-username", "repl",
		"--pg-repl-password", "replpw",
		"--pg-su-password", "supw",
		"--cluster-name", "test",
		"--etcd-endpoints", "127.0.0.1:2379",
	})
	if err != nil && strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected keeper flag parse failure: %v", err)
	}
	if err != nil && !strings.Contains(err.Error(), "failed to get postgres binary version") {
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

func TestClusterInitializeSkipIfPresentFlagIsAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test", "initialize", "--skip-if-present",
	})
	if err == nil {
		t.Fatal("expected initialize error")
	}
	if strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected unknown flag error: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterPauseFlagsAreAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test", "pause",
		"--reason", "maintenance", "--ttl", "30m",
	})
	if err == nil {
		t.Fatal("expected pause error")
	}
	if strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected unknown flag error: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterResumeCommandIsAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test", "resume",
	})
	if err == nil {
		t.Fatal("expected resume error")
	}
	if strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected unknown command error: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterSwitchoverCommandIsAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test", "switchover", "--keeper-uid", "keeper-01",
	})
	if err == nil {
		t.Fatal("expected switchover error")
	}
	if strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected unknown command error: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFailoverTargetCommandIsAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"failover", "--cluster-name", "test", "target", "--keeper-uid", "keeper-01",
	})
	if err == nil {
		t.Fatal("expected failover target error")
	}
	if strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected unknown command error: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown store backend") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterReinitCommandIsAccepted(t *testing.T) {
	parser := newTestParser()
	_, err := parser.ParseArgs([]string{
		"cluster", "--cluster-name", "test", "reinit", "--keeper-uid", "keeper-01",
	})
	if err == nil {
		t.Fatal("expected reinit error")
	}
	if strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected unknown command error: %v", err)
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
