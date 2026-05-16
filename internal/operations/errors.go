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

import "errors"

var (
	// ErrClusterPaused reports blocked mutation while pause mode is active.
	ErrClusterPaused = errors.New("cluster is paused; resume first")
	// ErrPauseTTLNegative reports invalid negative pause TTL.
	ErrPauseTTLNegative = errors.New("pause ttl must be >= 0")
	// ErrTargetKeeperUIDRequired reports missing target keeper uid.
	ErrTargetKeeperUIDRequired = errors.New("target keeper uid required")
	// ErrKeeperUIDRequired reports missing keeper uid.
	ErrKeeperUIDRequired = errors.New("keeper uid required")
	// ErrTargetKeeperNotFound reports unknown target keeper uid.
	ErrTargetKeeperNotFound = errors.New("target keeper not found")
	// ErrKeeperNotFound reports unknown keeper uid.
	ErrKeeperNotFound = errors.New("keeper doesn't exist")
	// ErrTargetKeeperHasNoDB reports target keeper without assigned DB.
	ErrTargetKeeperHasNoDB = errors.New("target keeper has no assigned database")
	// ErrCannotReinitializeCurrentDB reports reinit request against current master DB.
	ErrCannotReinitializeCurrentDB = errors.New("cannot reinitialize current master database")
	// ErrNoClusterData reports missing cluster data in store.
	ErrNoClusterData = errors.New("no cluster data available")
	// ErrNoClusterSpec reports missing cluster spec in cluster data.
	ErrNoClusterSpec = errors.New("no cluster spec available")
	// ErrClusterDataUpdateRetriesExceed reports CAS retries exhausted.
	ErrClusterDataUpdateRetriesExceed = errors.New("failed to update cluster data after 3 retries")
)
