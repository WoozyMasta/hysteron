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
	"fmt"

	stconfig "github.com/woozymasta/hysteron/internal/config"
	keepercmd "github.com/woozymasta/hysteron/internal/runtime/keeper"
	proxycmd "github.com/woozymasta/hysteron/internal/runtime/proxy"
	sentinelcmd "github.com/woozymasta/hysteron/internal/runtime/sentinel"
)

// Run executes a runtime component for the selected backend.
func Run(target Target) error {
	if target.CommonConfig == nil {
		return ErrCommonConfigRequired
	}
	backend, err := normalizeBackend(target.Backend)
	if err != nil {
		return err
	}
	if target.CommonConfig.Store.Backend != "" {
		flagBackendInput := target.CommonConfig.Store.Backend
		flagBackend, err := normalizeBackend(stconfig.NormalizeStoreBackend(flagBackendInput))
		if err != nil {
			return fmt.Errorf("runtime backend mismatch: flag=%q: %w", flagBackendInput, err)
		}
		if flagBackend != backend {
			return fmt.Errorf(
				"runtime backend mismatch: command=%q flag=%q",
				backend,
				flagBackendInput,
			)
		}
	}
	target.CommonConfig.Store.Backend = backend

	switch target.Component {
	case "sentinel":
		return sentinelcmd.Run(*target.CommonConfig, target.ExtraArgs)
	case "proxy":
		return proxycmd.Run(*target.CommonConfig, target.ExtraArgs)
	case "keeper":
		return keepercmd.Run(*target.CommonConfig, target.ExtraArgs)
	default:
		return fmt.Errorf("unsupported runtime component %q", target.Component)
	}
}

func normalizeBackend(backend string) (string, error) {
	switch backend {
	case "etcd", "etcdv3":
		return "etcdv3", nil
	case "kubernetes", "k8s":
		return "kubernetes", nil
	default:
		return "", fmt.Errorf("unsupported runtime backend %q", backend)
	}
}
