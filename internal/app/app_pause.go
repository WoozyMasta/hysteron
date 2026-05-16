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
	"strings"
	"time"

	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/store"
)

func isPauseActive(now time.Time, paused bool, until *time.Time) bool {
	if !paused {
		return false
	}
	if until == nil {
		return true
	}
	return now.Before(*until)
}

func ensureClusterNotPaused(cdPaused bool, cdPauseUntil *time.Time) error {
	if !isPauseActive(time.Now().UTC(), cdPaused, cdPauseUntil) {
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
	if ttl < 0 {
		return errors.New("pause ttl must be >= 0")
	}
	reason = strings.TrimSpace(reason)
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		newCD := cd.DeepCopy()
		newCD.Cluster.Status.Paused = true
		newCD.Cluster.Status.PauseReason = reason
		if ttl > 0 {
			until := time.Now().UTC().Add(ttl)
			newCD.Cluster.Status.PauseUntil = &until
		} else {
			newCD.Cluster.Status.PauseUntil = nil
		}

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

// ResumeCluster disables cluster pause mode.
func ResumeCluster(ctx context.Context, cfg *stconfig.CommonConfig) error {
	for range clusterDataMutateRetries {
		s, cd, pair, err := validatedClusterDataWithStore(ctx, cfg)
		if err != nil {
			return err
		}

		newCD := cd.DeepCopy()
		newCD.Cluster.Status.Paused = false
		newCD.Cluster.Status.PauseReason = ""
		newCD.Cluster.Status.PauseUntil = nil

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
