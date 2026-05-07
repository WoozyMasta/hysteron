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

package config

import (
	"errors"
	"testing"
)

func TestCheckCommonConfigK8sAlias(t *testing.T) {
	cfg := &CommonConfig{
		Store: StoreOptions{
			Backend: "k8s",
		},
	}

	err := CheckCommonConfig(cfg)
	if !errors.Is(err, ErrKubernetesResourceKindRequired) {
		t.Fatalf("expected ErrKubernetesResourceKindRequired, got: %v", err)
	}
}

func TestCheckCommonConfigK8sAliasWithRequiredFields(t *testing.T) {
	cfg := &CommonConfig{
		Store: StoreOptions{
			Backend: "k8s",
		},
		K8s: K8sOptions{
			ResourceKind: "configmap",
			ResourceName: "stolon-{cluster}",
		},
	}

	if err := CheckCommonConfig(cfg); err != nil {
		t.Fatalf("expected k8s alias config to pass, got: %v", err)
	}
}

func TestCheckClusterNamesK8sAliasRequiresClusterTokenForMultiCluster(t *testing.T) {
	cfg := &CommonConfig{
		Store: StoreOptions{
			Backend: "k8s",
		},
		K8s: K8sOptions{
			ResourceName: "stolon",
		},
		ClusterNames: []string{"alpha", "beta"},
	}

	_, err := CheckClusterNames(cfg)
	if err == nil {
		t.Fatal("expected error for multi-cluster k8s resource name without {cluster}")
	}
}
