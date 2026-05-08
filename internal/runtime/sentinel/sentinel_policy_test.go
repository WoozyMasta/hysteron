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

	"github.com/woozymasta/hysteron/internal/common"
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
