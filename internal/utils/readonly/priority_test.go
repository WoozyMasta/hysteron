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

import (
	"reflect"
	"testing"

	"github.com/woozymasta/hysteron/internal/cluster"
)

func TestSelectPriority(t *testing.T) {
	syncStandbys := []string{"s1", "s2"}
	asyncStandbys := []string{"a1"}

	tests := []struct {
		name     string
		priority ReplicaPriority
		want     []string
	}{
		{
			name:     "sync default",
			priority: ReplicaPrioritySync,
			want:     []string{"s1", "s2"},
		},
		{
			name:     "async preferred",
			priority: ReplicaPriorityAsync,
			want:     []string{"a1"},
		},
		{
			name:     "any combined",
			priority: ReplicaPriorityAny,
			want:     []string{"s1", "s2", "a1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectPriority(tt.priority, syncStandbys, asyncStandbys)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestXLogLag(t *testing.T) {
	if got := XLogLag(100, 70); got != 30 {
		t.Fatalf("XLogLag(100,70)=%d, want 30", got)
	}
	if got := XLogLag(70, 100); got != 0 {
		t.Fatalf("XLogLag(70,100)=%d, want 0", got)
	}
}

func TestDBStatusEligible(t *testing.T) {
	tests := []struct {
		name string
		db   *cluster.DB
		want bool
	}{
		{
			name: "nil",
			db:   nil,
			want: false,
		},
		{
			name: "healthy current with endpoint",
			db: &cluster.DB{
				Generation: 2,
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 2,
					ListenAddress:     "10.0.0.1",
					Port:              "5432",
				},
			},
			want: true,
		},
		{
			name: "unhealthy",
			db: &cluster.DB{
				Generation: 2,
				Status: cluster.DBStatus{
					Healthy:           false,
					CurrentGeneration: 2,
					ListenAddress:     "10.0.0.1",
					Port:              "5432",
				},
			},
			want: false,
		},
		{
			name: "generation mismatch",
			db: &cluster.DB{
				Generation: 2,
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 3,
					ListenAddress:     "10.0.0.1",
					Port:              "5432",
				},
			},
			want: false,
		},
		{
			name: "missing address",
			db: &cluster.DB{
				Generation: 2,
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 2,
					ListenAddress:     "",
					Port:              "5432",
				},
			},
			want: false,
		},
		{
			name: "missing port",
			db: &cluster.DB{
				Generation: 2,
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 2,
					ListenAddress:     "10.0.0.1",
					Port:              "",
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DBStatusEligible(tt.db)
			if got != tt.want {
				t.Fatalf("DBStatusEligible()=%v, want %v", got, tt.want)
			}
		})
	}
}
