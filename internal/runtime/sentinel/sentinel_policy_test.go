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

package sentinel

import (
	"reflect"
	"testing"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/utils/timer"
)

func TestComputeOrphanMemberSlots(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	prevTS := time.Unix(900, 0).UTC()

	t.Run("tracks removed follower slots", func(t *testing.T) {
		got := computeOrphanMemberSlots(
			[]string{"db1", "db2"},
			[]string{"db1"},
			nil,
			false,
			now,
		)
		want := map[string]time.Time{
			common.HysteronName("db2"): now,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected orphan map, got: %#v, want: %#v", got, want)
		}
	})

	t.Run("keeps previous timestamp and clears on rejoin", func(t *testing.T) {
		got := computeOrphanMemberSlots(
			[]string{"db1"},
			[]string{"db1", "db2"},
			map[string]time.Time{
				common.HysteronName("db2"): prevTS,
			},
			false,
			now,
		)
		if got != nil {
			t.Fatalf("expected orphan map to be cleared, got: %#v", got)
		}
	})

	t.Run("resets tracking on master change", func(t *testing.T) {
		got := computeOrphanMemberSlots(
			[]string{"db1", "db2"},
			[]string{"db1"},
			map[string]time.Time{
				common.HysteronName("db2"): prevTS,
			},
			true,
			now,
		)
		if got != nil {
			t.Fatalf("expected orphan map reset on master change, got: %#v", got)
		}
	})
}

func TestShouldDelayLeaderRace(t *testing.T) {
	failedMaster := &cluster.DB{UID: "master1"}
	candidate := &cluster.DB{UID: "standby1"}
	window := 5 * time.Second

	t.Run("starts backoff when wal is increasing", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{},
			dbIncreasingXLogPosObservedAt: map[string]int64{
				candidate.UID: timer.Now(),
			},
			leaderRaceBackoffTimers: map[string]int64{},
		}
		if !s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("expected leader race delay on first observation")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; !ok {
			t.Fatalf("expected backoff timer to be created")
		}
	})

	t.Run("stops delaying after window elapses", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{},
			dbIncreasingXLogPosObservedAt: map[string]int64{
				candidate.UID: timer.Now(),
			},
			leaderRaceBackoffTimers: map[string]int64{
				failedMaster.UID: timer.Now() - int64(window) - int64(time.Second),
			},
		}
		if s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("expected leader race delay to expire")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; ok {
			t.Fatalf("expected expired backoff timer to be cleared")
		}
	})

	t.Run("does not delay when wal is stalled", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{
				candidate.UID: cluster.DefaultDBNotIncreasingXLogPosTimes + 1,
			},
			dbIncreasingXLogPosObservedAt: map[string]int64{
				candidate.UID: timer.Now(),
			},
			leaderRaceBackoffTimers: map[string]int64{
				failedMaster.UID: timer.Now(),
			},
		}
		if s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("did not expect leader race delay when wal is stalled")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; ok {
			t.Fatalf("expected stale backoff timer to be cleared")
		}
	})
}
