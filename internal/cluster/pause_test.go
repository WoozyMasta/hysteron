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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"testing"
	"time"
)

func TestClusterStatusPauseActive(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)

	tests := []struct {
		name   string
		status ClusterStatus
		want   bool
	}{
		{
			name: "not paused",
			status: ClusterStatus{
				Paused: false,
			},
			want: false,
		},
		{
			name: "paused without ttl",
			status: ClusterStatus{
				Paused: true,
			},
			want: true,
		},
		{
			name: "paused with future ttl",
			status: ClusterStatus{
				Paused:     true,
				PauseUntil: &future,
			},
			want: true,
		},
		{
			name: "paused with expired ttl",
			status: ClusterStatus{
				Paused:     true,
				PauseUntil: &past,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.PauseActive(now)
			if got != tt.want {
				t.Fatalf("PauseActive()=%v, want %v", got, tt.want)
			}
		})
	}
}
