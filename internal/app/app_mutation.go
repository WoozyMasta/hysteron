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

	"github.com/woozymasta/hysteron/internal/cluster"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/configfile"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/id"

	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

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
		if err := ensureClusterNotPaused(
			cd.Cluster.Status.Paused,
			cd.Cluster.Status.PauseUntil,
		); err != nil {
			return err
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
	if err := ensureClusterNotPaused(
		cd.Cluster.Status.Paused,
		cd.Cluster.Status.PauseUntil,
	); err != nil {
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
	if err := ensureClusterNotPaused(
		cd.Cluster.Status.Paused,
		cd.Cluster.Status.PauseUntil,
	); err != nil {
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
func WriteClusterData(ctx context.Context, cfg *stconfig.CommonConfig, data []byte, force bool) error {
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
	if existing != nil && existing.Cluster != nil {
		if err := ensureClusterNotPaused(
			existing.Cluster.Status.Paused,
			existing.Cluster.Status.PauseUntil,
		); err != nil {
			return err
		}
	}
	if existing != nil && !force {
		return ErrClusterDataOverwriteRequiresYes
	}

	var cd *cluster.ClusterData
	if err := json.Unmarshal(data, &cd); err != nil {
		return fmt.Errorf("invalid cluster data: %w", err)
	}
	if cd == nil {
		return ErrNoClusterData
	}
	if cd.Cluster == nil || cd.Cluster.Spec == nil {
		return ErrNoClusterSpec
	}
	if cd.FormatVersion == 0 {
		cd.FormatVersion = cluster.CurrentCDFormatVersion
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		return fmt.Errorf("unsupported cluster data format version %d", cd.FormatVersion)
	}
	if err := cd.Cluster.Spec.Validate(); err != nil {
		return fmt.Errorf("cluster data validation failed: %w", err)
	}

	if err := s.PutClusterData(ctx, cd); err != nil {
		return fmt.Errorf("cannot update cluster data: %w", err)
	}

	return nil
}

// InitializeCluster creates initial cluster data from cluster spec.
func InitializeCluster(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	specData []byte,
	force bool,
	skipIfPresent bool,
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
	if existing != nil && existing.Cluster != nil {
		if err := ensureClusterNotPaused(
			existing.Cluster.Status.Paused,
			existing.Cluster.Status.PauseUntil,
		); err != nil {
			return err
		}
	}
	if existing != nil && skipIfPresent {
		return nil
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
		if err := ensureClusterNotPaused(
			cd.Cluster.Status.Paused,
			cd.Cluster.Status.PauseUntil,
		); err != nil {
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
		if err := ensureClusterNotPaused(
			cd.Cluster.Status.Paused,
			cd.Cluster.Status.PauseUntil,
		); err != nil {
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
		if err := ensureClusterNotPaused(
			cd.Cluster.Status.Paused,
			cd.Cluster.Status.PauseUntil,
		); err != nil {
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
