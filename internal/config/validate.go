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
	"fmt"
	"strings"
)

// CheckCommonConfig validates backend-dependent config values.
func CheckCommonConfig(cfg *CommonConfig) error {
	if NormalizeStoreBackend(cfg.Store.Backend) == storeBackendKubernetes && cfg.K8s.ResourceKind == "" {
		return ErrKubernetesResourceKindRequired
	}
	if NormalizeStoreBackend(cfg.Store.Backend) == storeBackendKubernetes && cfg.K8s.ResourceName == "" {
		return ErrKubernetesResourceNameRequired
	}
	return nil
}

// CheckClusterName validates that exactly one cluster name is provided.
func CheckClusterName(cfg *CommonConfig) error {
	names := cfg.ClusterNamesList()
	if len(names) == 0 {
		return ErrClusterNameRequired
	}
	if len(names) > 1 {
		return ErrExactlyOneClusterNameRequired
	}
	return nil
}

// CheckClusterNames validates one or more distinct cluster names.
func CheckClusterNames(cfg *CommonConfig) ([]string, error) {
	names := cfg.ClusterNamesList()
	if len(names) == 0 {
		return nil, ErrClusterNameRequired
	}
	if NormalizeStoreBackend(cfg.Store.Backend) == storeBackendKubernetes &&
		len(names) > 1 &&
		!strings.Contains(cfg.K8s.ResourceName, "{cluster}") {
		return nil, errors.New("kubernetes resource name must include {cluster} when multiple cluster names are configured")
	}
	seen := map[string]struct{}{}
	for _, name := range names {
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate cluster name %q", name)
		}
		seen[name] = struct{}{}
	}
	return names, nil
}
