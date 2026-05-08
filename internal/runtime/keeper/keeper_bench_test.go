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

package keeper

import (
	"strconv"
	"testing"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	pg "github.com/woozymasta/hysteron/internal/postgresql"
)

func BenchmarkKeeperCreatePGParameters(b *testing.B) {
	p := &PostgresKeeper{
		pgListenAddress: "127.0.0.1",
		pgPort:          "5432",
		dbLocalState: &DBLocalState{
			InitPGParameters: common.Parameters{
				"max_connections": "100",
				"shared_buffers":  "128MB",
			},
		},
		pgBinaryVersion: func() (int, int, error) { return 13, 0, nil },
	}
	db := benchmarkKeeperDB()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parameters := p.createPGParameters(db)
		if parameters["wal_keep_size"] == "" {
			b.Fatal("empty wal_keep_size")
		}
	}
}

func BenchmarkKeeperCreateRecoveryOptions(b *testing.B) {
	p := &PostgresKeeper{}
	standbySettings := &cluster.StandbySettings{
		PrimaryConninfo:       "host=127.0.0.1 port=5432 user=repl sslmode=prefer",
		PrimarySlotName:       "hysteron_db1",
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

func benchmarkKeeperDB() *cluster.DB {
	return &cluster.DB{
		UID:        "db-0",
		Generation: cluster.InitialGeneration,
		Spec: &cluster.DBSpec{
			KeeperUID:            "keeper-0",
			InitMode:             cluster.DBInitModeNone,
			Role:                 common.RoleMaster,
			IncludeConfig:        true,
			MaxStandbys:          8,
			AdditionalWalSenders: 2,
			UsePgrewind:          true,
			PGParameters: cluster.PGParameters{
				"shared_buffers": "256MB",
				"work_mem":       "16MB",
			},
			SynchronousReplication:      true,
			SynchronousStandbys:         []string{"db-1", "db-2"},
			ExternalSynchronousStandbys: []string{"external_sync"},
			Followers:                   []string{"db-1", "db-2"},
			AdditionalReplicationSlots:  []string{"extra"},
		},
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
		curReplSlots = append(curReplSlots, common.HysteronName(followersUIDs[i]))
	}
	for i := range additionalReplSlots {
		additionalReplSlots[i] = "extra-" + strconv.Itoa(i)
		curReplSlots = append(curReplSlots, common.HysteronName(additionalReplSlots[i]))
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := p.updateReplSlots(
			curReplSlots,
			uid,
			followersUIDs,
			additionalReplSlots,
			nil,
			0,
			nil,
			nil,
			nil,
		); err != nil {
			b.Fatal(err)
		}
	}
}
