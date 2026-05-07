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

package runtimecommon

import (
	"errors"
	"reflect"
	"testing"

	stconfig "github.com/sorintlab/stolon/internal/config"
	"github.com/woozymasta/flags"
)

func TestCommonConfigClusterNamesList(t *testing.T) {
	cfg := &CommonConfig{
		ClusterNames: []string{"one,two", " three ", "", "four"},
	}

	got := cfg.ClusterNamesList()
	want := []string{"one", "two", "three", "four"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ClusterNamesList() = %v, want %v", got, want)
	}
}

func TestCheckClusterNameRequiresExactlyOneCluster(t *testing.T) {
	tests := []struct {
		name string
		cfg  CommonConfig
		err  bool
	}{
		{
			name: "one cluster",
			cfg:  CommonConfig{ClusterNames: []string{"one"}},
		},
		{
			name: "no clusters",
			cfg:  CommonConfig{},
			err:  true,
		},
		{
			name: "multiple clusters",
			cfg:  CommonConfig{ClusterNames: []string{"one", "two"}},
			err:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckClusterName(&tt.cfg)
			if tt.err && err == nil {
				t.Fatal("expected error")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCheckClusterNamesRejectsDuplicates(t *testing.T) {
	cfg := &CommonConfig{ClusterNames: []string{"one", "two", "one"}}
	if _, err := CheckClusterNames(cfg); err == nil {
		t.Fatal("expected duplicate cluster name error")
	}
}

func TestCheckClusterNamesRequiresKubeResourceNamePlaceholderForMultipleClusters(t *testing.T) {
	cfg := &CommonConfig{ClusterNames: []string{"one", "two"}}
	cfg.Store.Backend = "kubernetes"
	cfg.Kube.ResourceName = "shared-name"

	if _, err := CheckClusterNames(cfg); err == nil {
		t.Fatal("expected kubernetes resource name placeholder error")
	}
}

func TestKubeResourceNameForCluster(t *testing.T) {
	cfg := &CommonConfig{}
	cfg.Kube.ResourceName = "pg-{cluster}"

	got, err := KubeResourceNameForCluster(cfg, "prod")
	if err != nil {
		t.Fatalf("KubeResourceNameForCluster() error = %v", err)
	}
	if got != "pg-prod" {
		t.Fatalf("KubeResourceNameForCluster() = %q, want pg-prod", got)
	}
}

func TestKubeResourceNameForClusterRejectsInvalidName(t *testing.T) {
	cfg := &CommonConfig{}
	cfg.Kube.ResourceName = "PG_{cluster}"

	if _, err := KubeResourceNameForCluster(cfg, "prod"); err == nil {
		t.Fatal("expected invalid kubernetes resource name error")
	}
}

func TestCommonParserAcceptsStoreBackendAliases(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    string
		wantErr bool
	}{
		{
			name: "etcd alias",
			args: []string{"--store-backend=etcd"},
			want: "etcd",
		},
		{
			name: "k8s alias",
			args: []string{"--store-backend=k8s"},
			want: "k8s",
		},
		{
			name:    "invalid backend",
			args:    []string{"--store-backend=invalid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := CommonConfig{}
			parser := NewParser("test", "TEST", &cfg, 0)
			parseArgs := append(append([]string{}, tt.args...), "help")
			_, err := parser.ParseArgs(parseArgs)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}
			if err != nil {
				var ferr *flags.Error
				if !errors.As(err, &ferr) || ferr.Type != flags.ErrHelp {
					t.Fatalf("unexpected parse error: %v", err)
				}
			}
			if cfg.Store.Backend != tt.want {
				t.Fatalf("unexpected backend value: got %q want %q", cfg.Store.Backend, tt.want)
			}
		})
	}
}

func TestCheckCommonConfigWithK8sAlias(t *testing.T) {
	cfg := &CommonConfig{}
	cfg.Store.Backend = "k8s"

	err := CheckCommonConfig(cfg)
	if !errors.Is(err, stconfig.ErrKubernetesResourceKindRequired) {
		t.Fatalf("expected ErrKubernetesResourceKindRequired, got: %v", err)
	}

	cfg.Kube.ResourceKind = "configmap"
	err = CheckCommonConfig(cfg)
	if !errors.Is(err, stconfig.ErrKubernetesResourceNameRequired) {
		t.Fatalf("expected ErrKubernetesResourceNameRequired, got: %v", err)
	}

	cfg.Kube.ResourceName = "stolon-{cluster}"
	if err := CheckCommonConfig(cfg); err != nil {
		t.Fatalf("expected k8s alias config to pass, got: %v", err)
	}
}
