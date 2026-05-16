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
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/woozymasta/hysteron/internal/app"
	"github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/output"
	"github.com/woozymasta/hysteron/internal/runtime"
)

// Execute stores keeper runtime command context for backend leaf execution.
func (c *keeperRuntimeCommand) Execute(_ []string) error {
	c.Etcd.Common = c.Common
	c.Etcd.Keeper = c.Keeper
	c.Etcd.Component = c.Component
	c.Kubernetes.Common = c.Common
	c.Kubernetes.Keeper = c.Keeper
	c.Kubernetes.Component = c.Component
	return nil
}

// Execute stores proxy runtime command context for backend leaf execution.
func (c *proxyRuntimeCommand) Execute(_ []string) error {
	c.Etcd.Common = c.Common
	c.Etcd.Proxy = c.Proxy
	c.Etcd.Component = c.Component
	c.Kubernetes.Common = c.Common
	c.Kubernetes.Proxy = c.Proxy
	c.Kubernetes.Component = c.Component
	return nil
}

// Execute stores sentinel runtime command context for backend leaf execution.
func (c *sentinelRuntimeCommand) Execute(_ []string) error {
	c.Etcd.Common = c.Common
	c.Etcd.Sentinel = c.Sentinel
	c.Etcd.Web = c.Web
	c.Etcd.Component = c.Component
	c.Kubernetes.Common = c.Common
	c.Kubernetes.Sentinel = c.Sentinel
	c.Kubernetes.Web = c.Web
	c.Kubernetes.Component = c.Component
	return nil
}

// Execute runs `hysteron keeper etcd`.
func (c *keeperRuntimeEtcdCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "etcd", runtimeEtcdConfig(c.Common, c.Etcd), args, app.RuntimeTarget{
		Keeper: keeperRuntimeOpts(c.Keeper),
	})
}

// Execute runs `hysteron keeper kubernetes`.
func (c *keeperRuntimeKubernetesCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "kubernetes", runtimeKubernetesConfig(c.Common, c.K8s), args, app.RuntimeTarget{
		Keeper: keeperRuntimeOpts(c.Keeper),
	})
}

// Execute runs `hysteron proxy etcd`.
func (c *proxyRuntimeEtcdCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "etcd", runtimeEtcdConfig(c.Common, c.Etcd), args, app.RuntimeTarget{
		Proxy: proxyRuntimeOpts(c.Proxy),
	})
}

// Execute runs `hysteron proxy kubernetes`.
func (c *proxyRuntimeKubernetesCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "kubernetes", runtimeKubernetesConfig(c.Common, c.K8s), args, app.RuntimeTarget{
		Proxy: proxyRuntimeOpts(c.Proxy),
	})
}

// Execute runs `hysteron sentinel etcd`.
func (c *sentinelRuntimeEtcdCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "etcd", runtimeEtcdConfig(c.Common, c.Etcd), args, app.RuntimeTarget{
		Sentinel: sentinelRuntimeOpts(c.Sentinel, c.Web),
	})
}

// Execute runs `hysteron sentinel kubernetes`.
func (c *sentinelRuntimeKubernetesCommand) Execute(args []string) error {
	return runTypedRuntime(c.Component, "kubernetes", runtimeKubernetesConfig(c.Common, c.K8s), args, app.RuntimeTarget{
		Sentinel: sentinelRuntimeOpts(c.Sentinel, c.Web),
	})
}

// Execute runs `hysteron cluster initialize`.
func (c *clusterInitializeCommand) Execute(_ []string) error {
	specData, err := readCommandInput(c.File, c.Args.Spec, false)
	if err != nil {
		return fmt.Errorf("read cluster spec input: %w", err)
	}
	return app.InitializeCluster(
		commandContext(),
		clusterConfig(),
		specData,
		c.Yes,
		c.SkipIfPresent,
	)
}

// Execute runs `hysteron cluster update`.
func (c *clusterUpdateCommand) Execute(_ []string) error {
	specData, err := readCommandInput(c.File, c.Args.Spec, true)
	if err != nil {
		return fmt.Errorf("read cluster spec input: %w", err)
	}
	return app.UpdateClusterSpecification(
		commandContext(),
		clusterConfig(),
		specData,
		c.Patch,
	)
}

// Execute runs `hysteron cluster status`.
func (c *clusterStatusCommand) Execute(_ []string) error {
	status, err := app.GetClusterStatus(commandContext(), clusterConfig())
	if err != nil {
		return err
	}
	return output.WriteStatus(os.Stdout, c.Output.Selected(), status)
}

// Execute runs `hysteron cluster specification`.
func (c *clusterSpecificationCommand) Execute(_ []string) error {
	spec, err := app.ClusterSpecification(
		commandContext(),
		clusterConfig(),
		c.Defaults,
	)
	if err != nil {
		return err
	}
	return output.WriteValue(os.Stdout, c.Output.Selected(), spec)
}

// Execute runs `hysteron cluster list`.
func (c *clusterListCommand) Execute(_ []string) error {
	clusterNames, err := app.ListClusters(commandContext(), clusterConfig())
	if err != nil {
		return err
	}
	return output.WriteClusterList(os.Stdout, c.Output.Selected(), clusterNames)
}

// Execute runs `hysteron cluster data read`.
func (c *clusterDataReadCommand) Execute(_ []string) error {
	cd, err := app.ReadClusterData(commandContext(), clusterConfig())
	if err != nil {
		return err
	}
	return output.WriteValue(os.Stdout, c.Output.Selected(), cd)
}

// Execute runs `hysteron cluster data write`.
func (c *clusterDataWriteCommand) Execute(_ []string) error {
	data, err := readDataInput(c.File)
	if err != nil {
		return fmt.Errorf("read cluster data input: %w", err)
	}
	return app.WriteClusterData(commandContext(), clusterConfig(), data, c.Yes)
}

// Execute runs `hysteron cluster data patch`.
func (c *clusterDataPatchCommand) Execute(_ []string) error {
	data, err := readDataInput(c.File)
	if err != nil {
		return fmt.Errorf("read cluster data patch input: %w", err)
	}
	return app.PatchClusterData(commandContext(), clusterConfig(), data)
}

// Execute runs `hysteron cluster promote`.
func (c *clusterPromoteCommand) Execute(_ []string) error {
	if !c.Yes {
		return app.ErrConfirmationRequired
	}
	return app.PromoteCluster(commandContext(), clusterConfig())
}

// Execute runs `hysteron cluster pause`.
func (c *clusterPauseCommand) Execute(_ []string) error {
	return app.PauseCluster(commandContext(), clusterConfig(), c.Reason, c.TTL)
}

// Execute runs `hysteron cluster resume`.
func (c *clusterResumeCommand) Execute(_ []string) error {
	return app.ResumeCluster(commandContext(), clusterConfig())
}

// Execute runs `hysteron cluster switchover`.
func (c *clusterSwitchoverCommand) Execute(_ []string) error {
	return app.RequestManualSwitchover(
		commandContext(),
		clusterConfig(),
		c.KeeperUID,
	)
}

// Execute runs `hysteron cluster reinit`.
func (c *clusterReinitCommand) Execute(_ []string) error {
	return app.ReinitializeReplica(commandContext(), clusterConfig(), c.KeeperUID)
}

// Execute runs `hysteron cluster keeper remove`.
func (c *clusterKeeperRemoveCommand) Execute(_ []string) error {
	return app.RemoveKeeper(commandContext(), clusterConfig(), c.KeeperUID)
}

// Execute runs `hysteron failover keeper`.
func (c *failoverKeeperCommand) Execute(_ []string) error {
	return app.FailKeeper(commandContext(), failoverConfig(), c.KeeperUID)
}

// Execute runs `hysteron failover force`.
func (c *failoverForceCommand) Execute(_ []string) error {
	return app.ForceFailover(commandContext(), failoverConfig())
}

// Execute runs `hysteron failover target`.
func (c *failoverTargetCommand) Execute(_ []string) error {
	return app.RequestManualFailover(
		commandContext(),
		failoverConfig(),
		c.KeeperUID,
	)
}

func runTypedRuntime(
	component string,
	backend string,
	commonConfig *config.CommonConfig,
	args []string,
	target app.RuntimeTarget,
) error {
	if len(args) > 0 {
		return ErrTooManyCommandArguments
	}
	if component == "" {
		return ErrRuntimeCommandContextMissing
	}
	target.CommonConfig = commonConfig
	target.Backend = backend
	target.Component = component
	return app.RunRuntime(target)
}

func keeperRuntimeOpts(opts keeperRuntimeOptions) *runtime.KeeperOptions {
	return &runtime.KeeperOptions{
		UID:                     opts.UID,
		DataDir:                 opts.DataDir,
		CanBeMaster:             boolValueOrDefault(opts.CanBeMaster, true),
		CanBeSynchronousReplica: boolValueOrDefault(opts.CanBeSynchronousReplica, true),
		DisableDataDirLocking:   opts.DisableDataDirLocking,
		AllowNewerPG:            opts.AllowNewerPG,
		PG: runtime.KeeperPostgresOptions{
			ListenAddress:    opts.PG.ListenAddress,
			AdvertiseAddress: opts.PG.AdvertiseAddress,
			Port:             opts.PG.Port,
			AdvertisePort:    opts.PG.AdvertisePort,
			BinPath:          opts.PG.BinPath,
			WALDir:           opts.PG.WALDir,
			TablespaceDirs:   opts.PG.TablespaceDirs,
			Repl: runtime.KeeperPostgresReplOptions{
				AuthMethod:   opts.PG.Repl.AuthMethod,
				Username:     opts.PG.Repl.Username,
				Password:     opts.PG.Repl.Password,
				PasswordFile: opts.PG.Repl.PasswordFile,
			},
			SU: runtime.KeeperPostgresSUOptions{
				AuthMethod:   opts.PG.SU.AuthMethod,
				Username:     opts.PG.SU.Username,
				Password:     opts.PG.SU.Password,
				PasswordFile: opts.PG.SU.PasswordFile,
			},
		},
	}
}

func proxyRuntimeOpts(opts proxyRuntimeOptions) *runtime.ProxyOptions {
	return &runtime.ProxyOptions{
		ListenAddress:           opts.ListenAddress,
		Port:                    opts.Port,
		DisableWritableListener: opts.DisableWritableListener,
		ReadOnlyListenAddress:   opts.ReadOnlyListenAddress,
		ReadOnlyPort:            opts.ReadOnlyPort,
		ReadOnlyReplicaPriority: string(opts.ReadOnly.ReplicaPriority),
		ReadOnlyMaxLagBytes:     uint64(opts.ReadOnly.MaxLag),
		ReadOnlyNoFallback:      opts.ReadOnly.NoFallback,
		ReadOnlyIncludePrimary:  opts.ReadOnly.IncludePrimary,
	}
}

func sentinelRuntimeOpts(
	runtimeOpts sentinelRuntimeOptions,
	webOpts sentinelWebOptions,
) *runtime.SentinelOptions {
	return &runtime.SentinelOptions{
		InitialClusterSpecFile: runtimeOpts.InitialClusterSpecFile,
		ClusterSpecFiles:       runtimeOpts.ClusterSpecFiles,
		WebListenAddress:       webOpts.ListenAddress,
		WebBasePath:            webOpts.BasePath,
		WebAuthUsername:        webOpts.AuthUsername,
		WebAuthPassword:        webOpts.AuthPassword,
		WebReadTimeout:         webOpts.ReadTimeout.String(),
		WebWriteTimeout:        webOpts.WriteTimeout.String(),
		WebUnsafeNoAuth:        webOpts.UnsafeNoAuth,
	}
}

func commandContext() context.Context {
	return context.Background()
}

func readDataInput(file string) ([]byte, error) {
	if file == "" || file == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(file)
}

func readCommandInput(file string, inline string, required bool) ([]byte, error) {
	inline = strings.TrimSpace(inline)
	if file != "" && inline != "" {
		return nil, ErrCommandInputConflict
	}
	if inline != "" {
		return []byte(inline), nil
	}
	if file == "" {
		if !required {
			return nil, nil
		}
		return nil, ErrCommandInputRequired
	}
	return readDataInput(file)
}

func boolValueOrDefault(value *bool, def bool) bool {
	if value == nil {
		return def
	}
	return *value
}
