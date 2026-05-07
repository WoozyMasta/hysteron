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

package runtime

import (
	"errors"
	"strings"
	"testing"

	stconfig "github.com/woozymasta/hysteron/internal/config"
)

func TestRunRequiresCommonConfig(t *testing.T) {
	err := Run(Target{
		Backend:   "etcd",
		Component: "proxy",
	})
	if !errors.Is(err, ErrCommonConfigRequired) {
		t.Fatalf("expected ErrCommonConfigRequired, got %v", err)
	}
}

func TestRunRejectsUnsupportedBackend(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{},
		Backend:      "consul",
		Component:    "proxy",
	})
	if err == nil {
		t.Fatal("expected unsupported backend error")
	}
	if !strings.Contains(err.Error(), `unsupported runtime backend "consul"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunRejectsBackendMismatch(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{
			Store: stconfig.StoreOptions{
				Backend: "kubernetes",
			},
		},
		Backend:   "etcd",
		Component: "proxy",
	})
	if err == nil {
		t.Fatal("expected backend mismatch error")
	}
	if !strings.Contains(err.Error(), `runtime backend mismatch`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunRejectsUnsupportedComponent(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{},
		Backend:      "etcd",
		Component:    "unknown",
	})
	if err == nil {
		t.Fatal("expected unsupported component error")
	}
	if !strings.Contains(err.Error(), `unsupported runtime component "unknown"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunAcceptsEtcdFlagAliasWhenBackendMatches(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{
			Store: stconfig.StoreOptions{
				Backend: "etcd",
			},
		},
		Backend:   "etcdv3",
		Component: "unknown",
	})
	if err == nil {
		t.Fatal("expected unsupported component error")
	}
	if strings.Contains(err.Error(), "runtime backend mismatch") {
		t.Fatalf("unexpected backend mismatch for etcd alias: %v", err)
	}
	if !strings.Contains(err.Error(), `unsupported runtime component "unknown"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunAcceptsK8sFlagAliasWhenBackendMatches(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{
			Store: stconfig.StoreOptions{
				Backend: "k8s",
			},
		},
		Backend:   "kubernetes",
		Component: "unknown",
	})
	if err == nil {
		t.Fatal("expected unsupported component error")
	}
	if strings.Contains(err.Error(), "runtime backend mismatch") {
		t.Fatalf("unexpected backend mismatch for k8s alias: %v", err)
	}
	if !strings.Contains(err.Error(), `unsupported runtime component "unknown"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunRejectsUnknownFlagBackendValue(t *testing.T) {
	err := Run(Target{
		CommonConfig: &stconfig.CommonConfig{
			Store: stconfig.StoreOptions{
				Backend: "invalid",
			},
		},
		Backend:   "etcd",
		Component: "unknown",
	})
	if err == nil {
		t.Fatal("expected backend mismatch error")
	}
	if !strings.Contains(err.Error(), `flag="invalid"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeBackendAliases(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "etcd", want: "etcdv3"},
		{input: "etcdv3", want: "etcdv3"},
		{input: "kubernetes", want: "kubernetes"},
		{input: "k8s", want: "kubernetes"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := normalizeBackend(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected normalize result: got %q want %q", got, tt.want)
			}
		})
	}
}
