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

import stconfig "github.com/sorintlab/stolon/internal/config"

// FromConfigCommon converts unified config contract to daemon common config.
func FromConfigCommon(commonConfig stconfig.CommonConfig) CommonConfig {
	return CommonConfig{
		Metrics: MetricsOptions{
			ListenAddress: commonConfig.Metrics.ListenAddress,
		},
		Kube: KubeOptions{
			Config:       commonConfig.K8s.Config,
			ResourceKind: commonConfig.K8s.ResourceKind,
			ResourceName: commonConfig.K8s.ResourceName,
			Context:      commonConfig.K8s.Context,
			Namespace:    commonConfig.K8s.Namespace,
		},
		ClusterNames: commonConfig.ClusterNames,
		Log:          commonConfig.Log,
		Store: StoreOptions{
			Backend:       commonConfig.Store.Backend,
			Endpoints:     commonConfig.Store.Endpoints,
			Prefix:        commonConfig.Store.Prefix,
			CertFile:      commonConfig.Store.CertFile,
			KeyFile:       commonConfig.Store.KeyFile,
			CAFile:        commonConfig.Store.CAFile,
			Timeout:       commonConfig.Store.Timeout,
			SkipTLSVerify: commonConfig.Store.SkipTLSVerify,
		},
		Debug: commonConfig.Debug,
	}
}
