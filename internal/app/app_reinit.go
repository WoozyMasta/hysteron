// Copyright 20[0-9][0-9](?:-20[0-9][0-9])? (?:Sorint\.lab|WoozyMasta)(?:\nCopyright 2026 WoozyMasta)?
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

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/store"
)

// ReinitializeReplica marks a standby database on the target keeper for resync.
func ReinitializeReplica(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
) error {
	if targetKeeperUID == "" {
		return ErrKeeperUIDRequired
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
		if _, ok := cd.Keepers[targetKeeperUID]; !ok {
			return ErrKeeperNotFound
		}

		targetDB := cd.FindDB(cd.Keepers[targetKeeperUID])
		if targetDB == nil {
			return ErrReplicaReinitTargetNotFound
		}
		if targetDB.UID == cd.Cluster.Status.Master {
			return ErrReplicaReinitTargetIsMaster
		}

		newCD := cd.DeepCopy()
		newTargetDB := newCD.DBs[targetDB.UID]
		newTargetDB.Spec.InitMode = cluster.DBInitModeResync
		newTargetDB.Spec.Role = common.RoleStandby
		newTargetDB.Spec.FollowConfig = &cluster.FollowConfig{
			Type:  cluster.FollowTypeInternal,
			DBUID: newCD.Cluster.Status.Master,
		}
		// Avoid conflicting operator intents while the replica is being reinitialized.
		newCD.Cluster.Status.ManualSwitch = nil

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
