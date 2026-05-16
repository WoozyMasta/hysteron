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
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/operations"
)

func ensureClusterNotPaused(cdPaused bool, cdPauseUntil *time.Time) error {
	if !(cluster.ClusterStatus{
		Paused:     cdPaused,
		PauseUntil: cdPauseUntil,
	}).PauseActive(time.Now().UTC()) {
		return nil
	}
	return ErrClusterPaused
}

// PauseCluster enables cluster pause mode with optional reason and TTL.
func PauseCluster(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	reason string,
	ttl time.Duration,
) error {
	return operations.PauseCluster(ctx, cfg, reason, ttl)
}

// ResumeCluster disables cluster pause mode.
func ResumeCluster(ctx context.Context, cfg *stconfig.CommonConfig) error {
	return operations.ResumeCluster(ctx, cfg)
}

// RequestManualSwitchover requests a master switch to target keeper.
func RequestManualSwitchover(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
) error {
	return operations.RequestManualSwitch(
		ctx,
		cfg,
		targetKeeperUID,
		cluster.ManualSwitchModeSwitchover,
	)
}

// RequestManualFailover requests a forced master switch to target keeper.
func RequestManualFailover(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
) error {
	return operations.RequestManualSwitch(
		ctx,
		cfg,
		targetKeeperUID,
		cluster.ManualSwitchModeFailover,
	)
}

// ReinitializeReplica marks a standby database on the target keeper for resync.
func ReinitializeReplica(
	ctx context.Context,
	cfg *stconfig.CommonConfig,
	targetKeeperUID string,
) error {
	return operations.ReinitializeReplica(ctx, cfg, targetKeeperUID)
}
