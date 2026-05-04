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

package cmd

import (
	"strconv"
	"testing"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	pg "github.com/sorintlab/stolon/internal/postgresql"
)

func BenchmarkKeeperCreateRecoveryOptions(b *testing.B) {
	p := &PostgresKeeper{}
	standbySettings := &cluster.StandbySettings{
		PrimaryConninfo:       "host=127.0.0.1 port=5432 user=repl sslmode=prefer",
		PrimarySlotName:       "stolon_db1",
		RecoveryMinApplyDelay: "10s",
	}
	archiveRecoverySettings := &cluster.ArchiveRecoverySettings{
		RestoreCommand: "wal-g wal-fetch %f %p",
	}
	recoveryTargetSettings := &cluster.RecoveryTargetSettings{
		RecoveryTargetTime:     "2026-05-04 12:00:00+03",
		RecoveryTargetTimeline: "latest",
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		options := p.createRecoveryOptions(
			pg.RecoveryModeStandby,
			standbySettings,
			archiveRecoverySettings,
			recoveryTargetSettings,
		)
		if options.RecoveryParameters["primary_conninfo"] == "" {
			b.Fatal("empty primary_conninfo")
		}
	}
}

func BenchmarkKeeperIsDifferentTimelineBranch(b *testing.B) {
	p := &PostgresKeeper{}
	followedDB := &cluster.DB{
		UID: "db-0",
		Status: cluster.DBStatus{
			TimelineID: 3,
			XLogPos:    0x5000000,
			TimelinesHistory: cluster.PostgresTimelinesHistory{
				{TimelineID: 1, SwitchPoint: 0x2000000, Reason: "before 2"},
				{TimelineID: 2, SwitchPoint: 0x4000000, Reason: "before 3"},
			},
		},
	}
	pgState := &cluster.PostgresState{
		UID:        "db-1",
		TimelineID: 3,
		XLogPos:    0x5000000,
		TimelinesHistory: cluster.PostgresTimelinesHistory{
			{TimelineID: 1, SwitchPoint: 0x2000000, Reason: "before 2"},
			{TimelineID: 2, SwitchPoint: 0x4000000, Reason: "before 3"},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if p.isDifferentTimelineBranch(followedDB, pgState) {
			b.Fatal("unexpected different branch")
		}
	}
}

func BenchmarkKeeperUpdateReplSlotsNoChanges(b *testing.B) {
	p := &PostgresKeeper{}
	uid := "db-0"
	followersUIDs := make([]string, 16)
	additionalReplSlots := make([]string, 4)
	curReplSlots := []string{"manual_slot"}
	for i := range followersUIDs {
		followersUIDs[i] = "db-" + strconv.Itoa(i+1)
		curReplSlots = append(curReplSlots, common.StolonName(followersUIDs[i]))
	}
	for i := range additionalReplSlots {
		additionalReplSlots[i] = "extra-" + strconv.Itoa(i)
		curReplSlots = append(curReplSlots, common.StolonName(additionalReplSlots[i]))
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := p.updateReplSlots(curReplSlots, uid, followersUIDs, additionalReplSlots); err != nil {
			b.Fatal(err)
		}
	}
}
