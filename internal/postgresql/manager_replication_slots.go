// Copyright 2015 Sorint.lab
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

package postgresql

import (
	"context"
)

// GetSyncStandbys returns synchronous standby names currently reported by PostgreSQL.
func (p *Manager) GetSyncStandbys() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getSyncStandbys(ctx, p.localConnParams)
}

// GetReplicationSlots returns replication slot names currently present.
func (p *Manager) GetReplicationSlots() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getReplicationSlots(ctx, p.localConnParams)
}

// GetPhysicalReplicationSlots returns non-temporary physical replication slots.
func (p *Manager) GetPhysicalReplicationSlots() ([]PhysicalReplicationSlot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getPhysicalReplicationSlots(ctx, p.localConnParams)
}

// CreateReplicationSlot creates a physical replication slot.
func (p *Manager) CreateReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return createReplicationSlot(ctx, p.localConnParams, name)
}

// DropReplicationSlot removes a replication slot.
func (p *Manager) DropReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return dropReplicationSlot(ctx, p.localConnParams, name)
}

// GetLogicalReplicationSlots returns non-temporary logical replication slots.
func (p *Manager) GetLogicalReplicationSlots() ([]LogicalReplicationSlot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return getLogicalReplicationSlots(ctx, p.localConnParams)
}

// CreateLogicalReplicationSlot creates a logical replication slot.
func (p *Manager) CreateLogicalReplicationSlot(
	name,
	database,
	plugin string,
	failover bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return createLogicalReplicationSlot(ctx, p.localConnParams, name, database, plugin, failover)
}

// DropLogicalReplicationSlot removes a logical replication slot.
func (p *Manager) DropLogicalReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return dropLogicalReplicationSlot(ctx, p.localConnParams, name)
}

// AdvanceLogicalReplicationSlot advances a logical slot to target LSN.
func (p *Manager) AdvanceLogicalReplicationSlot(name, database string, targetLSN uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return advanceLogicalReplicationSlot(ctx, p.localConnParams, name, database, targetLSN)
}

// IsWALReplayPaused reports whether WAL replay is currently paused.
func (p *Manager) IsWALReplayPaused() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return isWALReplayPaused(ctx, p.localConnParams)
}

// ResumeWALReplay resumes WAL replay when recovery is paused.
func (p *Manager) ResumeWALReplay() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeoutValue())
	defer cancel()
	return resumeWALReplay(ctx, p.localConnParams)
}
