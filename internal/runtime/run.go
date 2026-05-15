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
		if target.Sentinel == nil {
			return fmt.Errorf("missing runtime options for component %q", target.Component)
		}
		return sentinelcmd.RunWithOptions(*target.CommonConfig, sentinelcmd.RunOptions{
			InitialClusterSpecFile:         target.Sentinel.InitialClusterSpecFile,
			ClusterSpecFiles:               target.Sentinel.ClusterSpecFiles,
			WebListenAddress:               target.Sentinel.WebListenAddress,
			WebBasePath:                    target.Sentinel.WebBasePath,
			WebAuthUsername:                target.Sentinel.WebAuthUsername,
			WebAuthPassword:                target.Sentinel.WebAuthPassword,
			WebReadTimeout:                 target.Sentinel.WebReadTimeout,
			WebWriteTimeout:                target.Sentinel.WebWriteTimeout,
			WebAllowUnsafeAdminWithoutAuth: target.Sentinel.WebAllowUnsafeAdminWithoutAuth,
		})

	case "proxy":
		if target.Proxy == nil {
			return fmt.Errorf("missing runtime options for component %q", target.Component)
		}
		return proxycmd.RunWithOptions(*target.CommonConfig, proxycmd.RunOptions{
			ListenAddress:           target.Proxy.ListenAddress,
			Port:                    target.Proxy.Port,
			DisableWritableListener: target.Proxy.DisableWritableListener,
			ReadOnlyListenAddress:   target.Proxy.ReadOnlyListenAddress,
			ReadOnlyPort:            target.Proxy.ReadOnlyPort,
			ReadOnlyReplicaPriority: target.Proxy.ReadOnlyReplicaPriority,
			ReadOnlyMaxLagBytes:     target.Proxy.ReadOnlyMaxLagBytes,
			ReadOnlyNoFallback:      target.Proxy.ReadOnlyNoFallback,
			ReadOnlyIncludePrimary:  target.Proxy.ReadOnlyIncludePrimary,
		})

	case "keeper":
		if target.Keeper == nil {
			return fmt.Errorf("missing runtime options for component %q", target.Component)
		}
		return keepercmd.RunWithOptions(*target.CommonConfig, keepercmd.RunOptions{
			UID:                     target.Keeper.UID,
			DataDir:                 target.Keeper.DataDir,
			CanBeMaster:             target.Keeper.CanBeMaster,
			CanBeSynchronousReplica: target.Keeper.CanBeSynchronousReplica,
			DisableDataDirLocking:   target.Keeper.DisableDataDirLocking,
			AllowNewerPG:            target.Keeper.AllowNewerPG,
			PG: keepercmd.RunPostgresOptions{
				ListenAddress:    target.Keeper.PG.ListenAddress,
				AdvertiseAddress: target.Keeper.PG.AdvertiseAddress,
				Port:             target.Keeper.PG.Port,
				AdvertisePort:    target.Keeper.PG.AdvertisePort,
				BinPath:          target.Keeper.PG.BinPath,
				WALDir:           target.Keeper.PG.WALDir,
				Repl: keepercmd.RunPostgresReplOptions{
					AuthMethod:   target.Keeper.PG.Repl.AuthMethod,
					Username:     target.Keeper.PG.Repl.Username,
					Password:     target.Keeper.PG.Repl.Password,
					PasswordFile: target.Keeper.PG.Repl.PasswordFile,
				},
				SU: keepercmd.RunPostgresSUOptions{
					AuthMethod:   target.Keeper.PG.SU.AuthMethod,
					Username:     target.Keeper.PG.SU.Username,
					Password:     target.Keeper.PG.SU.Password,
					PasswordFile: target.Keeper.PG.SU.PasswordFile,
				},
			},
		})

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
