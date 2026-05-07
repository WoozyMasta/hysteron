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

package readonly

import "github.com/woozymasta/hysteron/internal/cluster"

// ReplicaPriority selects preferred read-only endpoint class.
type ReplicaPriority string

const (
	// ReplicaPrioritySync prefers synchronous standbys first.
	ReplicaPrioritySync ReplicaPriority = "sync"
	// ReplicaPriorityAsync prefers asynchronous standbys first.
	ReplicaPriorityAsync ReplicaPriority = "async"
	// ReplicaPriorityAny uses both synchronous and asynchronous standbys.
	ReplicaPriorityAny ReplicaPriority = "any"
)

// SelectPriority selects read-only candidates by configured priority.
func SelectPriority[T any](
	priority ReplicaPriority,
	syncStandbys []T,
	asyncStandbys []T,
) []T {
	switch priority {
	case ReplicaPriorityAny:
		return append(append([]T{}, syncStandbys...), asyncStandbys...)
	case ReplicaPriorityAsync:
		if len(asyncStandbys) > 0 {
			return asyncStandbys
		}
		return syncStandbys
	case ReplicaPrioritySync:
		fallthrough
	default:
		if len(syncStandbys) > 0 {
			return syncStandbys
		}
		return asyncStandbys
	}
}

// XLogLag returns lag from primary to standby in bytes.
func XLogLag(primaryXLogPos, standbyXLogPos uint64) uint64 {
	if primaryXLogPos > standbyXLogPos {
		return primaryXLogPos - standbyXLogPos
	}
	return 0
}

// DBStatusEligible reports whether DB status can be used for read-only routing.
func DBStatusEligible(db *cluster.DB) bool {
	if db == nil {
		return false
	}
	return db.Status.Healthy &&
		db.Status.CurrentGeneration == db.Generation &&
		db.Status.ListenAddress != "" &&
		db.Status.Port != ""
}
