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

	"github.com/woozymasta/hysteron/internal/app"
	"github.com/woozymasta/hysteron/internal/output"
)

// Execute stores runtime command context for backend leaf execution.
func (c *runtimeCommand) Execute(_ []string) error {
	c.Etcd.Common = c.Common
	c.Etcd.Component = c.Component
	c.Kubernetes.Common = c.Common
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

// Execute runs `hysteron <component> etcd`.
func (c *runtimeEtcdCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeEtcdConfig(c.Common, c.Etcd),
		Backend:      "etcd",
		Component:    c.Component,
		ExtraArgs:    args,
	})
}

// Execute runs `hysteron <component> kubernetes`.
func (c *runtimeKubernetesCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeKubernetesConfig(c.Common, c.K8s),
		Backend:      "kubernetes",
		Component:    c.Component,
		ExtraArgs:    args,
	})
}

// Execute runs `hysteron proxy etcd`.
func (c *proxyRuntimeEtcdCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeEtcdConfig(c.Common, c.Etcd),
		Backend:      "etcd",
		Component:    c.Component,
		ExtraArgs:    append(proxyRuntimeExtraArgs(c.Proxy), args...),
	})
}

// Execute runs `hysteron proxy kubernetes`.
func (c *proxyRuntimeKubernetesCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeKubernetesConfig(c.Common, c.K8s),
		Backend:      "kubernetes",
		Component:    c.Component,
		ExtraArgs:    append(proxyRuntimeExtraArgs(c.Proxy), args...),
	})
}

// Execute runs `hysteron sentinel etcd`.
func (c *sentinelRuntimeEtcdCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeEtcdConfig(c.Common, c.Etcd),
		Backend:      "etcd",
		Component:    c.Component,
		ExtraArgs: append(
			sentinelRuntimeExtraArgs(c.Sentinel),
			append(sentinelWebExtraArgs(c.Web), args...)...,
		),
	})
}

// Execute runs `hysteron sentinel kubernetes`.
func (c *sentinelRuntimeKubernetesCommand) Execute(args []string) error {
	if c.Component == "" {
		return ErrRuntimeCommandContextMissing
	}
	return app.RunRuntime(app.RuntimeTarget{
		CommonConfig: runtimeKubernetesConfig(c.Common, c.K8s),
		Backend:      "kubernetes",
		Component:    c.Component,
		ExtraArgs: append(
			sentinelRuntimeExtraArgs(c.Sentinel),
			append(sentinelWebExtraArgs(c.Web), args...)...,
		),
	})
}

// Execute runs `hysteron cluster initialize`.
func (c *clusterInitializeCommand) Execute(_ []string) error {
	specData, err := readOptionalDataInput(c.File)
	if err != nil {
		return fmt.Errorf("read cluster spec input: %w", err)
	}
	return app.InitializeCluster(commandContext(), clusterConfig(), specData, c.Yes)
}

// Execute runs `hysteron cluster update`.
func (c *clusterUpdateCommand) Execute(args []string) error {
	specData, err := readDataInputFromFileOrArg(c.File, args)
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

func commandContext() context.Context {
	return context.Background()
}

func sentinelWebExtraArgs(web sentinelWebOptions) []string {
	args := []string{
		"--web-base-path", web.BasePath,
		"--web-read-timeout", web.ReadTimeout.String(),
		"--web-write-timeout", web.WriteTimeout.String(),
	}
	if web.ListenAddress != "" {
		args = append(args, "--web-listen-address", web.ListenAddress)
	}
	if web.AuthUsername != "" {
		args = append(args, "--web-auth-username", web.AuthUsername)
	}
	if web.AuthPassword != "" {
		args = append(args, "--web-auth-password", web.AuthPassword)
	}
	if web.AllowUnsafeAdminWithoutAuth {
		args = append(args, "--web-allow-unsafe-admin-without-auth")
	}
	return args
}

func sentinelRuntimeExtraArgs(opts sentinelRuntimeOptions) []string {
	args := []string{}
	if opts.InitialClusterSpecFile != "" {
		args = append(args, "--initial-cluster-spec", opts.InitialClusterSpecFile)
	}
	for _, spec := range opts.ClusterSpecFiles {
		if spec != "" {
			args = append(args, "--cluster-spec", spec)
		}
	}
	return args
}

func proxyRuntimeExtraArgs(opts proxyRuntimeOptions) []string {
	args := []string{}
	if opts.ListenAddress != "" {
		args = append(args, "--listen-address", opts.ListenAddress)
	}
	if opts.Port != "" {
		args = append(args, "--port", opts.Port)
	}
	if opts.DisableWritableListener {
		args = append(args, "--disable-writable-listener")
	}
	if opts.ReadOnlyListenAddress != "" {
		args = append(args, "--read-only-listen-address", opts.ReadOnlyListenAddress)
	}
	if opts.ReadOnlyPort != "" {
		args = append(args, "--read-only-port", opts.ReadOnlyPort)
	}
	return args
}

func readDataInput(file string) ([]byte, error) {
	if file == "" || file == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(file)
}

func readOptionalDataInput(file string) ([]byte, error) {
	if file == "" {
		return nil, nil
	}
	return readDataInput(file)
}

func readDataInputFromFileOrArg(file string, args []string) ([]byte, error) {
	if len(args) > 1 {
		return nil, ErrTooManyCommandArguments
	}
	if file != "" && len(args) == 1 {
		return nil, ErrCommandInputConflict
	}
	if file == "" && len(args) == 0 {
		return nil, ErrCommandInputRequired
	}
	if len(args) == 1 {
		return []byte(args[0]), nil
	}
	return readDataInput(file)
}
