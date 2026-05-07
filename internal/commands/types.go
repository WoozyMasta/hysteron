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

import "github.com/sorintlab/stolon/internal/output"

type rootCommand struct {
	Keeper   runtimeCommand    `command:"keeper" command-group:"Runtime Commands" description:"Run keeper runtime components"`
	Sentinel runtimeCommand    `command:"sentinel" command-group:"Runtime Commands" description:"Run sentinel runtime components"`
	Proxy    runtimeCommand    `command:"proxy" command-group:"Runtime Commands" description:"Run proxy runtime components"`
	Cluster  clusterCommand    `command:"cluster" command-group:"Management Commands" description:"Manage cluster control operations"`
	Global   rootGlobalOptions `group:"Global"`
	Failover failoverCommand   `command:"failover" command-group:"Management Commands" description:"Manage failover operations"`
}

type runtimeCommand struct {
	Common     runtimeCommonOptions     `group:"Common"`
	Etcd       runtimeEtcdCommand       `command:"etcd" alias:"etcdv3" description:"Run component with etcd backend"`
	Kubernetes runtimeKubernetesCommand `command:"kubernetes" alias:"k8s" description:"Run component with kubernetes backend"`
	Component  string                   `no-flag:"true"`
}

type clusterCommand struct {
	Keeper        clusterKeeperCommand        `command:"keeper" description:"Manage keeper records in cluster data"`
	List          clusterListCommand          `command:"list" alias:"ls" description:"List clusters in the configured store"`
	Status        clusterStatusCommand        `command:"status" description:"Display current cluster status"`
	Data          clusterDataCommand          `command:"data" description:"Read and mutate cluster data documents"`
	Initialize    clusterInitializeCommand    `command:"initialize" alias:"init" description:"Initialize a new cluster"`
	Update        clusterUpdateCommand        `command:"update" description:"Replace or patch the current cluster specification"`
	Specification clusterSpecificationCommand `command:"specification" alias:"spec" description:"Retrieve the current cluster specification"`
	Common        managementCommonOptions     `group:"Common"`
	Promote       clusterPromoteCommand       `command:"promote" description:"Promote a standby cluster to primary"`
}

type failoverCommand struct {
	Force  failoverForceCommand    `command:"force" description:"Force failover to the best available candidate"`
	Keeper failoverKeeperCommand   `command:"keeper" description:"Mark a keeper as failed"`
	Common managementCommonOptions `group:"Common"`
}

type clusterDataCommand struct {
	Patch clusterDataPatchCommand `command:"patch" description:"Patch current cluster data"`
	Read  clusterDataReadCommand  `command:"read" description:"Read current cluster data"`
	Write clusterDataWriteCommand `command:"write" description:"Replace current cluster data"`
}

type clusterKeeperCommand struct {
	Remove clusterKeeperRemoveCommand `command:"remove" description:"Remove a keeper from cluster data"`
}

type runtimeEtcdCommand struct {
	Common    runtimeCommonOptions `no-flag:"true"`
	Component string               `no-flag:"true"`
	Etcd      runtimeEtcdOptions   `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
}

type runtimeKubernetesCommand struct {
	K8s       k8sStoreOptions      `group:"Kubernetes" namespace:"k8s" env-namespace:"K8S"`
	Common    runtimeCommonOptions `no-flag:"true"`
	Component string               `no-flag:"true"`
}

type clusterInitializeCommand struct {
	File string `short:"f" long:"file" description:"file containing the initial cluster specification"`
	confirmationOptions
}

type clusterUpdateCommand struct {
	File  string `short:"f" long:"file" description:"file containing a complete cluster specification or a patch to apply to the current cluster specification"`
	Patch bool   `short:"p" long:"patch" description:"patch the current cluster specification instead of replacing it"`
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

type clusterKeeperRemoveCommand struct {
	keeperUIDOptions
}

type failoverKeeperCommand struct {
	keeperUIDOptions
}

type failoverForceCommand struct{}

type confirmationOptions struct {
	Yes bool `short:"y" long:"yes" description:"do not ask for confirmation"`
}

type keeperUIDOptions struct {
	KeeperUID string `long:"keeper-uid" validate-non-empty:"true" description:"keeper uid"`
}
