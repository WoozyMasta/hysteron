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
	"time"

	"github.com/woozymasta/hysteron/internal/output"
)

type rootCommand struct {
	Cluster  clusterCommand         `command:"cluster"  command-group:"Management Commands" description:"Manage cluster control operations"`
	Global   rootGlobalOptions      `group:"Global"`
	Keeper   keeperRuntimeCommand   `command:"keeper"   command-group:"Runtime Commands"    description:"Run keeper runtime components"`
	Failover failoverCommand        `command:"failover" command-group:"Management Commands" description:"Manage failover operations"`
	Sentinel sentinelRuntimeCommand `command:"sentinel" command-group:"Runtime Commands"    description:"Run sentinel runtime components"`
	Proxy    proxyRuntimeCommand    `command:"proxy"    command-group:"Runtime Commands"    description:"Run proxy runtime components"`
}

type keeperRuntimeCommand struct {
	Kubernetes keeperRuntimeKubernetesCommand `command:"kubernetes" alias:"k8s"    description:"Run component with kubernetes backend"`
	Component  string                         `no-flag:"true"`
	Common     runtimeCommonOptions           `group:"Common"`
	Keeper     keeperRuntimeOptions           `group:"Keeper"`
	Etcd       keeperRuntimeEtcdCommand       `command:"etcd"       alias:"etcdv3" description:"Run component with etcd backend"`
}

type sentinelRuntimeCommand struct {
	Component  string                           `no-flag:"true"`
	Common     runtimeCommonOptions             `group:"Common"`
	Sentinel   sentinelRuntimeOptions           `group:"Sentinel"`
	Etcd       sentinelRuntimeEtcdCommand       `command:"etcd"       alias:"etcdv3" description:"Run component with etcd backend"`
	Kubernetes sentinelRuntimeKubernetesCommand `command:"kubernetes" alias:"k8s"    description:"Run component with kubernetes backend"`
	Web        sentinelWebOptions               `group:"Web"      namespace:"web" env-namespace:"WEB"`
}

type proxyRuntimeCommand struct {
	Component  string                        `no-flag:"true"`
	Kubernetes proxyRuntimeKubernetesCommand `command:"kubernetes" alias:"k8s"    description:"Run component with kubernetes backend"`
	Common     runtimeCommonOptions          `group:"Common"`
	Etcd       proxyRuntimeEtcdCommand       `command:"etcd"       alias:"etcdv3" description:"Run component with etcd backend"`
	Proxy      proxyRuntimeOptions           `group:"Proxy"`
}

type clusterCommand struct {
	Resume        clusterResumeCommand        `command:"resume"        description:"Resume mutating management operations"`
	Keeper        clusterKeeperCommand        `command:"keeper"        description:"Manage keeper records in cluster data"`
	List          clusterListCommand          `command:"list"          description:"List clusters in the configured store"              alias:"ls"`
	Status        clusterStatusCommand        `command:"status"        description:"Display current cluster status"`
	Switchover    clusterSwitchoverCommand    `command:"switchover"    description:"Request planned master switch to target keeper"`
	Data          clusterDataCommand          `command:"data"          description:"Read and mutate cluster data documents"`
	Initialize    clusterInitializeCommand    `command:"initialize"    description:"Initialize a new cluster"                           alias:"init"`
	Update        clusterUpdateCommand        `command:"update"        description:"Replace or patch the current cluster specification"`
	Pause         clusterPauseCommand         `command:"pause"         description:"Pause mutating management operations"`
	Specification clusterSpecificationCommand `command:"specification" description:"Retrieve the current cluster specification"         alias:"spec"`
	Common        managementCommonOptions     `group:"Common"`
	Promote       clusterPromoteCommand       `command:"promote"       description:"Promote a standby cluster to primary"`
}

type failoverCommand struct {
	Force  failoverForceCommand    `command:"force"   description:"Force failover to the best available candidate"`
	Keeper failoverKeeperCommand   `command:"keeper"  description:"Mark a keeper as failed"`
	Target failoverTargetCommand   `command:"target"  description:"Request failover to target keeper"`
	Common managementCommonOptions `group:"Common"`
}

type clusterDataCommand struct {
	Patch clusterDataPatchCommand `command:"patch" description:"Patch current cluster data"`
	Read  clusterDataReadCommand  `command:"read"  description:"Read current cluster data"`
	Write clusterDataWriteCommand `command:"write" description:"Replace current cluster data"`
}

type clusterKeeperCommand struct {
	Remove clusterKeeperRemoveCommand `command:"remove" description:"Remove a keeper from cluster data"`
}

type proxyRuntimeEtcdCommand struct {
	Component string               `no-flag:"true"`
	Common    runtimeCommonOptions `no-flag:"true"`
	Etcd      runtimeEtcdOptions   `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Proxy     proxyRuntimeOptions  `no-flag:"true"`
}

type keeperRuntimeEtcdCommand struct {
	Common    runtimeCommonOptions `no-flag:"true"`
	Keeper    keeperRuntimeOptions `no-flag:"true"`
	Component string               `no-flag:"true"`
	Etcd      runtimeEtcdOptions   `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
}

type keeperRuntimeKubernetesCommand struct {
	K8s       k8sStoreOptions      `group:"Kubernetes" namespace:"k8s" env-namespace:"K8S"`
	Component string               `no-flag:"true"`
	Common    runtimeCommonOptions `no-flag:"true"`
	Keeper    keeperRuntimeOptions `no-flag:"true"`
}

type proxyRuntimeKubernetesCommand struct {
	K8s       k8sStoreOptions      `group:"Kubernetes" namespace:"k8s" env-namespace:"K8S"`
	Component string               `no-flag:"true"`
	Common    runtimeCommonOptions `no-flag:"true"`
	Proxy     proxyRuntimeOptions  `no-flag:"true"`
}

type sentinelRuntimeEtcdCommand struct {
	Component string                 `no-flag:"true"`
	Common    runtimeCommonOptions   `no-flag:"true"`
	Sentinel  sentinelRuntimeOptions `no-flag:"true"`
	Etcd      runtimeEtcdOptions     `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Web       sentinelWebOptions     `no-flag:"true"`
}

type sentinelRuntimeKubernetesCommand struct {
	K8s       k8sStoreOptions        `group:"Kubernetes" namespace:"k8s" env-namespace:"K8S"`
	Component string                 `no-flag:"true"`
	Common    runtimeCommonOptions   `no-flag:"true"`
	Sentinel  sentinelRuntimeOptions `no-flag:"true"`
	Web       sentinelWebOptions     `no-flag:"true"`
}

type clusterInitializeCommand struct {
	File          string                    `short:"f" long:"file" description:"file containing the initial cluster specification"`
	Args          clusterSpecPositionalArgs `positional-args:"true"`
	SkipIfPresent bool                      `long:"skip-if-present" description:"exit successfully without changes when cluster data already exists"`
	confirmationOptions
}

type clusterUpdateCommand struct {
	File  string                    `short:"f" long:"file"  description:"file containing a complete cluster specification or a patch to apply to the current cluster specification"`
	Args  clusterSpecPositionalArgs `positional-args:"true" required:"true"`
	Patch bool                      `short:"p" long:"patch" description:"patch the current cluster specification instead of replacing it"`
}

type clusterStatusCommand struct {
	Output output.FormatOptions `group:"Output"`
}

type clusterSpecificationCommand struct {
	Output   output.FormatOptions `group:"Output"`
	Defaults bool                 `long:"defaults" description:"include default values in output"`
}

type clusterListCommand struct {
	Output output.FormatOptions `group:"Output"`
}

type clusterDataReadCommand struct {
	Output output.FormatOptions `group:"Output"`
}

type clusterDataWriteCommand struct {
	File string `short:"f" long:"file" description:"file containing the replacement cluster data"`
	confirmationOptions
}

type clusterDataPatchCommand struct {
	File string `short:"f" long:"file" description:"file containing the patch to apply"`
}

type clusterPromoteCommand struct {
	confirmationOptions
}

type clusterPauseCommand struct {
	Reason string        `long:"reason" description:"optional pause reason"`
	TTL    time.Duration `long:"ttl" description:"optional pause duration, for example 30m or 2h"`
}

type clusterResumeCommand struct{}

type clusterSwitchoverCommand struct {
	keeperUIDOptions
}

type clusterKeeperRemoveCommand struct {
	keeperUIDOptions
}

type failoverKeeperCommand struct {
	keeperUIDOptions
}

type failoverForceCommand struct{}

type failoverTargetCommand struct {
	keeperUIDOptions
}

type confirmationOptions struct {
	Yes bool `short:"y" long:"yes" description:"do not ask for confirmation"`
}

type keeperUIDOptions struct {
	KeeperUID string `long:"keeper-uid" validate-non-empty:"true" description:"keeper uid"`
}

type clusterSpecPositionalArgs struct {
	Spec string `positional-arg-name:"spec" description:"cluster spec content provided directly as argument"`
}
