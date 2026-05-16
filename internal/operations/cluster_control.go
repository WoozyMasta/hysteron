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

// Package operations provides shared cluster mutation operations reused by
// management interfaces (CLI and web admin API).
package operations

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/store"
)

const clusterMutateRetries = 3

// PauseCluster enables cluster pause mode with optional reason and TTL.
func PauseCluster(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	reason string,
	ttl time.Duration,
) error {
	if ttl < 0 {
		return ErrPauseTTLNegative
	}
	return mutateClusterData(ctx, cfg, false, func(cd *cluster.ClusterData) error {
		cd.Cluster.Status.Paused = true
		cd.Cluster.Status.PauseReason = strings.TrimSpace(reason)
		if ttl > 0 {
			until := time.Now().UTC().Add(ttl)
			cd.Cluster.Status.PauseUntil = &until
		} else {
			cd.Cluster.Status.PauseUntil = nil
		}
		return nil
	})
}

// ResumeCluster disables cluster pause mode.
func ResumeCluster(ctx context.Context, cfg *stconfig.CommonConfig) error {
	return mutateClusterData(ctx, cfg, false, func(cd *cluster.ClusterData) error {
		cd.Cluster.Status.Paused = false
		cd.Cluster.Status.PauseReason = ""
		cd.Cluster.Status.PauseUntil = nil
		return nil
	})
}

// RequestManualSwitch stores operator-requested switch intent.
func RequestManualSwitch(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
	mode cluster.ManualSwitchMode,
) error {
	targetKeeperUID = strings.TrimSpace(targetKeeperUID)
	if targetKeeperUID == "" {
		return ErrTargetKeeperUIDRequired
	}

	return mutateClusterData(ctx, cfg, true, func(cd *cluster.ClusterData) error {
		if _, ok := cd.Keepers[targetKeeperUID]; !ok {
			return ErrTargetKeeperNotFound
		}
		cd.Cluster.Status.ManualSwitch = &cluster.ManualSwitchRequest{
			TargetKeeperUID: targetKeeperUID,
			Mode:            mode,
		}
		return nil
	})
}

// ReinitializeReplica marks target standby for resync.
func ReinitializeReplica(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
) error {
	targetKeeperUID = strings.TrimSpace(targetKeeperUID)
	if targetKeeperUID == "" {
		return ErrKeeperUIDRequired
	}

	return mutateClusterData(ctx, cfg, true, func(cd *cluster.ClusterData) error {
		targetKeeper := cd.Keepers[targetKeeperUID]
		if targetKeeper == nil {
			return ErrKeeperNotFound
		}
		targetDB := cd.FindDB(targetKeeper)
		if targetDB == nil {
			return ErrTargetKeeperHasNoDB
		}
		if targetDB.UID == cd.Cluster.Status.Master {
			return ErrCannotReinitializeCurrentDB
		}

		targetDB.Spec.InitMode = cluster.DBInitModeResync
		targetDB.Spec.Role = common.RoleStandby
		targetDB.Spec.FollowConfig = &cluster.FollowConfig{
			Type:  cluster.FollowTypeInternal,
			DBUID: cd.Cluster.Status.Master,
		}
		cd.Cluster.Status.ManualSwitch = nil
		return nil
	})
}

func mutateClusterData(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	rejectWhenPaused bool,
	mutate func(cd *cluster.ClusterData) error,
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

	for range clusterMutateRetries {
		cd, pair, err := s.GetClusterData(ctx)
		if err != nil {
			return err
		}
		if cd == nil || pair == nil {
			return ErrNoClusterData
		}
		if cd.Cluster == nil || cd.Cluster.Spec == nil {
			return ErrNoClusterSpec
		}
		if rejectWhenPaused && cd.Cluster.Status.PauseActive(time.Now().UTC()) {
			return ErrClusterPaused
		}

		next := cd.DeepCopy()
		if err := mutate(next); err != nil {
			return err
		}

		if _, err := s.AtomicPutClusterData(ctx, next, pair); err != nil {
			if errors.Is(err, store.ErrKeyModified) {
				continue
			}
			return err
		}
		return nil
	}
	return ErrClusterDataUpdateRetriesExceed
}
