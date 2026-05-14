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

package app

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	runtimex "github.com/woozymasta/hysteron/internal/runtime"
	"github.com/woozymasta/hysteron/internal/store"
)

const (
	clusterDataMutateRetries = 3
)

// RunRuntime executes a runtime component for the selected backend.
func RunRuntime(target RuntimeTarget) error {
	return runtimex.Run(target)
}

// ClusterSpecification returns the current cluster spec from the configured
// store. With defaults=true, it returns a fully materialized copy.
func ClusterSpecification(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	defaults bool,
) (*cluster.ClusterSpec, error) {
	cd, err := validatedClusterData(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if cd.Cluster == nil || cd.Cluster.Spec == nil {
		return nil, ErrNoClusterSpec
	}

	if defaults {
		return cd.Cluster.DefSpec(), nil
	}

	return cd.Cluster.Spec, nil
}

// ListClusters returns cluster names visible in the configured store backend.
func ListClusters(ctx context.Context, cfg *stconfig.CommonConfig) ([]string, error) {
	if err := stconfig.CheckCommonConfig(cfg); err != nil {
		return nil, err
	}
	return stconfig.ListClusters(ctx, cfg)
}

// GetClusterStatus returns the current cluster status from the configured
// store.
func GetClusterStatus(ctx context.Context, cfg *stconfig.CommonConfig) (Status, error) {
	status := Status{}
	if err := stconfig.CheckCommonConfig(cfg); err != nil {
		return status, err
	}
	if err := stconfig.CheckClusterName(cfg); err != nil {
		return status, err
	}

	s, err := stconfig.NewStore(cfg, false)
	if err != nil {
		return status, err
	}
	election, err := stconfig.NewElection(cfg, "")
	if err != nil {
		return status, err
	}

	leaderUID, err := election.Leader()
	if err != nil && !errors.Is(err, store.ErrElectionNoLeader) {
		return status, err
	}

	sentinelsInfo, err := s.GetSentinelsInfo(ctx)
	if err != nil {
		return status, err
	}
	sort.Sort(sentinelsInfo)
	status.Sentinels = make([]SentinelStatus, 0, len(sentinelsInfo))
	for _, si := range sentinelsInfo {
		status.Sentinels = append(status.Sentinels, SentinelStatus{
			UID:    si.UID,
			Leader: leaderUID != "" && si.UID == leaderUID,
		})
	}

	proxiesInfo, err := s.GetProxiesInfo(ctx)
	if err != nil {
		return status, err
	}
	proxies := proxiesInfo.ToSlice()
	sort.Sort(proxies)
	status.Proxies = make([]ProxyStatus, 0, len(proxies))
	for _, pi := range proxies {
		status.Proxies = append(status.Proxies, ProxyStatus{
			UID:        pi.UID,
			Mode:       proxyStatusMode(pi.Listeners),
			Listeners:  proxyStatusListeners(pi.Listeners),
			Generation: pi.Generation,
		})
	}

	cd, err := validatedClusterData(ctx, cfg)
	if err != nil {
		return status, err
	}
	status.Keepers = makeKeeperStatus(cd)
	status.Cluster = makeClusterStatus(cd)
	status.Cluster.ProxiesSeen = len(status.Proxies)
	status.KeeperTree = keeperTreeLines(status.Cluster.MasterDBUID, cd)

	return status, nil
}

// ReadClusterData returns validated cluster data from the configured store.
func ReadClusterData(ctx context.Context, cfg *stconfig.CommonConfig) (*cluster.ClusterData, error) {
	cd, err := validatedClusterData(ctx, cfg)
	return cd, err
}

func validatedClusterData(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
) (*cluster.ClusterData, error) {
	_, cd, _, err := validatedClusterDataWithStore(ctx, cfg)
	return cd, err
}

func validatedClusterDataWithStore(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
) (store.Store, *cluster.ClusterData, *store.KVPair, error) {
	if err := stconfig.CheckCommonConfig(cfg); err != nil {
		return nil, nil, nil, err
	}

	if err := stconfig.CheckClusterName(cfg); err != nil {
		return nil, nil, nil, err
	}

	s, err := stconfig.NewStore(cfg, false)
	if err != nil {
		return nil, nil, nil, err
	}

	cd, pair, err := s.GetClusterData(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if cd == nil {
		return nil, nil, nil, ErrNoClusterData
	}

	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		return nil, nil, nil, fmt.Errorf(
			"unsupported cluster data format version %d",
			cd.FormatVersion,
		)
	}
	if cd.Cluster == nil || cd.Cluster.Spec == nil {
		return nil, nil, nil, ErrNoClusterSpec
	}

	if err := cd.Cluster.Spec.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("cluster data validation failed: %w", err)
	}

	if pair == nil {
		return nil, nil, nil, ErrNoClusterData
	}

	return s, cd, pair, nil
}
