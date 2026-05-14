// Copyright 2016 Sorint.lab
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

package cluster

// Replication-slot and superuser-replication access contract types.

// ManagedLogicalReplicationSlot defines one managed logical slot desired in
// cluster spec.
type ManagedLogicalReplicationSlot struct {
	// Name is the logical replication slot name.
	Name string `json:"name,omitempty"`
	// Database is the database where the logical slot is created.
	Database string `json:"database,omitempty"`
	// Plugin is the logical decoding output plugin.
	Plugin string `json:"plugin,omitempty"`
}

// ReplicationSlotType identifies replication slot type for ignore matchers.
type ReplicationSlotType string

const (
	// ReplicationSlotTypePhysical matches physical replication slots.
	ReplicationSlotTypePhysical ReplicationSlotType = "physical"
	// ReplicationSlotTypeLogical matches logical replication slots.
	ReplicationSlotTypeLogical ReplicationSlotType = "logical"
)

// ReplicationSlotMatcher defines subset matching for slot ignore policies.
type ReplicationSlotMatcher struct {
	// Name is an optional slot name selector.
	Name string `json:"name,omitempty"`
	// Type optionally constrains slot type (`physical` or `logical`).
	Type ReplicationSlotType `json:"type,omitempty"`
	// Database optionally constrains logical slot database.
	Database string `json:"database,omitempty"`
	// Plugin optionally constrains logical slot plugin.
	Plugin string `json:"plugin,omitempty"`
}

// SUReplAccessMode identifies default superuser replication access scope.
type SUReplAccessMode string

const (
	// SUReplAccessAll allows access from every host.
	SUReplAccessAll SUReplAccessMode = "all"
	// SUReplAccessStrict allows access from standby server IPs only.
	SUReplAccessStrict SUReplAccessMode = "strict"
)

// SUReplAccessModeP returns a pointer to s.
func SUReplAccessModeP(s SUReplAccessMode) *SUReplAccessMode {
	return new(s)
}
