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
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/woozymasta/hysteron/internal/cluster"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/configfile"
	runtimex "github.com/woozymasta/hysteron/internal/runtime"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/id"

	"k8s.io/apimachinery/pkg/util/strategicpatch"
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

// PromoteCluster updates cluster role to master.
func PromoteCluster(ctx context.Context, cfg *stconfig.CommonConfig) error {
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		if cd.Cluster == nil || cd.Cluster.Spec == nil {
			return ErrNoClusterSpec
		}
		defaultSpec := cd.Cluster.DefSpec()
		if defaultSpec.Role != nil && *defaultSpec.Role == cluster.ClusterRoleMaster {
			return nil
		}
		cd.Cluster.Spec.Role = cluster.ClusterRoleP(cluster.ClusterRoleMaster)
		if err := cd.Cluster.UpdateSpec(cd.Cluster.Spec); err != nil {
			return fmt.Errorf("cannot update cluster spec: %w", err)
		}

		if _, err := s.AtomicPutClusterData(ctx, cd, pair); err != nil {
			if errors.Is(err, store.ErrKeyModified) {
				continue
			}
			return fmt.Errorf("cannot update cluster data: %w", err)
		}
		return nil
	}
	return fmt.Errorf(
		"failed to update cluster data after %d retries",
		clusterDataMutateRetries,
	)
}

// RemoveKeeper removes keeper entry and assigned DB from cluster data.
func RemoveKeeper(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	keeperUID string,
) error {
	if keeperUID == "" {
		return ErrKeeperUIDRequired
	}
	s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
	if err != nil {
		return err
	}

	newCD := cd.DeepCopy()
	keeperInfo := newCD.Keepers[keeperUID]
	if keeperInfo == nil {
		return ErrKeeperNotFound
	}
	keeperDB := newCD.FindDB(keeperInfo)
	if keeperDB != nil && newCD.Cluster.Status.Master == keeperDB.UID {
		return ErrKeeperAssignedMaster
	}

	delete(newCD.Keepers, keeperUID)
	if keeperDB != nil {
		delete(newCD.DBs, keeperDB.UID)
	}

	if _, err := s.AtomicPutClusterData(ctx, newCD, pair); err != nil {
		return fmt.Errorf("cannot update cluster data: %w", err)
	}
	return nil
}

// FailKeeper marks one keeper as force-failed in cluster data.
func FailKeeper(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	keeperUID string,
) error {
	if keeperUID == "" {
		return ErrKeeperUIDRequired
	}
	s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
	if err != nil {
		return err
	}

	newCD := cd.DeepCopy()
	keeperInfo := newCD.Keepers[keeperUID]
	if keeperInfo == nil {
		return ErrKeeperNotFound
	}
	keeperInfo.Status.ForceFail = true

	if _, err := s.AtomicPutClusterData(ctx, newCD, pair); err != nil {
		return fmt.Errorf("cannot update cluster data: %w", err)
	}
	return nil
}

// WriteClusterData validates and writes a full cluster data document.
func WriteClusterData(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	data []byte,
	force bool,
) error {
	if err := stconfig.CheckCommonConfig(cfg); err != nil {
		return err
	}
	if err := stconfig.CheckClusterName(cfg); err != nil {
		return err
	}
	s, err := stconfig.NewStore(cfg, false)
	if err != nil {
		return err
	}

	clusterData, err := configfile.ClusterData(data)
	if err != nil {
		return fmt.Errorf("invalid cluster data: %w", err)
	}

	existing, _, err := s.GetClusterData(ctx)
	if err != nil {
		return err
	}
	if existing != nil && !force {
		return ErrClusterDataOverwriteRequiresYes
	}

	if err := s.PutClusterData(ctx, clusterData); err != nil {
		return fmt.Errorf("failed to write cluster data: %w", err)
	}
	return nil
}

// InitializeCluster creates a new cluster data document from provided spec.
// When specData is nil, a default "new" init mode spec is used.
func InitializeCluster(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	specData []byte,
	force bool,
) error {
	if err := stconfig.CheckCommonConfig(cfg); err != nil {
		return err
	}
	if err := stconfig.CheckClusterName(cfg); err != nil {
		return err
	}
	s, err := stconfig.NewStore(cfg, false)
	if err != nil {
		return err
	}

	existing, _, err := s.GetClusterData(ctx)
	if err != nil {
		return err
	}
	if existing != nil && !force {
		return ErrClusterDataOverwriteRequiresYes
	}

	clusterSpec, err := parseInitClusterSpec(specData)
	if err != nil {
		return err
	}
	if err := clusterSpec.Validate(); err != nil {
		return fmt.Errorf("invalid cluster spec: %w", err)
	}

	newCluster := cluster.NewCluster(id.UID(), clusterSpec)
	clusterData := cluster.NewClusterData(newCluster)
	if err := s.PutClusterData(ctx, clusterData); err != nil {
		return fmt.Errorf("cannot update cluster data: %w", err)
	}
	return nil
}

// UpdateClusterSpecification replaces or patches the current cluster spec.
func UpdateClusterSpecification(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	specData []byte,
	patch bool,
) error {
	if len(specData) == 0 {
		return ErrClusterSpecDataRequired
	}
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		var nextSpec *cluster.ClusterSpec
		if patch {
			nextSpec, err = patchClusterSpec(cd.Cluster.Spec, specData)
			if err != nil {
				return fmt.Errorf("failed to patch cluster spec: %w", err)
			}
		} else {
			nextSpec, err = configfile.ClusterSpec(specData)
			if err != nil {
				return fmt.Errorf("failed to unmarshal cluster spec: %w", err)
			}
		}
		if err := cd.Cluster.UpdateSpec(nextSpec); err != nil {
			return fmt.Errorf("cannot update cluster spec: %w", err)
		}

		if _, err := s.AtomicPutClusterData(ctx, cd, pair); err != nil {
			if errors.Is(err, store.ErrKeyModified) {
				continue
			}
			return fmt.Errorf("cannot update cluster data: %w", err)
		}
		return nil
	}
	return fmt.Errorf(
		"failed to update cluster data after %d retries",
		clusterDataMutateRetries,
	)
}

// PatchClusterData applies a strategic merge patch to current cluster data.
func PatchClusterData(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	patchData []byte,
) error {
	if len(patchData) == 0 {
		return ErrPatchDataRequired
	}
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		clusterDataJSON, err := json.Marshal(cd)
		if err != nil {
			return fmt.Errorf("marshal current cluster data: %w", err)
		}
		patchedJSON, err := strategicpatch.StrategicMergePatch(
			clusterDataJSON,
			patchData,
			&cluster.ClusterData{},
		)
		if err != nil {
			return fmt.Errorf("merge patch cluster data: %w", err)
		}

		var patched *cluster.ClusterData
		if err := json.Unmarshal(patchedJSON, &patched); err != nil {
			return fmt.Errorf("unmarshal patched cluster data: %w", err)
		}
		if patched == nil {
			return ErrNoClusterData
		}
		if patched.FormatVersion != cluster.CurrentCDFormatVersion {
			return fmt.Errorf(
				"unsupported cluster data format version %d",
				patched.FormatVersion,
			)
		}
		if patched.Cluster == nil || patched.Cluster.Spec == nil {
			return ErrNoClusterSpec
		}
		if err := patched.Cluster.Spec.Validate(); err != nil {
			return fmt.Errorf("cluster data validation failed: %w", err)
		}

		if _, err := s.AtomicPutClusterData(ctx, patched, pair); err != nil {
			if errors.Is(err, store.ErrKeyModified) {
				continue
			}
			return fmt.Errorf("cannot update cluster data: %w", err)
		}
		return nil
	}
	return fmt.Errorf(
		"failed to update cluster data after %d retries",
		clusterDataMutateRetries,
	)
}

// ForceFailover marks the current master keeper as force-failed.
func ForceFailover(ctx context.Context, cfg *stconfig.CommonConfig) error {
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		masterDBUID := cd.Cluster.Status.Master
		if masterDBUID == "" {
			return ErrNoMasterDatabase
		}
		masterDB := cd.DBs[masterDBUID]
		if masterDB == nil {
			return ErrNoMasterDatabase
		}
		masterKeeperUID := masterDB.Spec.KeeperUID
		if masterKeeperUID == "" {
			return ErrNoMasterKeeper
		}

		newCD := cd.DeepCopy()
		keeperInfo := newCD.Keepers[masterKeeperUID]
		if keeperInfo == nil {
			return ErrNoMasterKeeper
		}
		keeperInfo.Status.ForceFail = true

		if _, err := s.AtomicPutClusterData(ctx, newCD, pair); err != nil {
			if errors.Is(err, store.ErrKeyModified) {
				continue
			}
			return fmt.Errorf("cannot update cluster data: %w", err)
		}
		return nil
	}
	return fmt.Errorf(
		"failed to update cluster data after %d retries",
		clusterDataMutateRetries,
	)
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

func parseInitClusterSpec(specData []byte) (*cluster.ClusterSpec, error) {
	if len(specData) == 0 {
		clusterSpec := &cluster.ClusterSpec{}
		clusterSpec.InitMode = cluster.ClusterInitModeP(cluster.ClusterInitModeNew)
		return clusterSpec, nil
	}
	clusterSpec, err := configfile.ClusterSpec(specData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster spec: %w", err)
	}
	return clusterSpec, nil
}

func patchClusterSpec(
	clusterSpec *cluster.ClusterSpec,
	patchData []byte,
) (*cluster.ClusterSpec, error) {
	specJSON, err := json.Marshal(clusterSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster spec: %w", err)
	}
	patchedSpecJSON, err := strategicpatch.StrategicMergePatch(
		specJSON,
		patchData,
		&cluster.ClusterSpec{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to merge patch cluster spec: %w", err)
	}
	var patchedSpec *cluster.ClusterSpec
	if err := json.Unmarshal(patchedSpecJSON, &patchedSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal patched cluster spec: %w", err)
	}
	return patchedSpec, nil
}
