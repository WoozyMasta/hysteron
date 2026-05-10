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
	"reflect"
	"testing"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	pg "github.com/woozymasta/hysteron/internal/postgresql"
)

func TestEvaluatePgrewindDecision(t *testing.T) {
	t.Run("disabled when database is not initialized", func(t *testing.T) {
		d := evaluatePgrewindDecision(false, "sys1", "sys1", true, 0, "")
		if d.try {
			t.Fatal("expected pg_rewind to be disabled")
		}
		if d.reason != pgrewindReasonNotInitialized {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
	})

	t.Run("disabled when system IDs differ", func(t *testing.T) {
		d := evaluatePgrewindDecision(true, "sys1", "sys2", true, 0, "")
		if d.try {
			t.Fatal("expected pg_rewind to be disabled")
		}
		if d.reason != pgrewindReasonSystemIDDiff {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
	})

	t.Run("disabled when no master is available", func(t *testing.T) {
		d := evaluatePgrewindDecision(true, "sys1", "sys1", false, 0, "")
		if d.try {
			t.Fatal("expected pg_rewind to be disabled")
		}
		if d.reason != pgrewindReasonNoMaster {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
	})

	t.Run("wal check error keeps pg_rewind enabled", func(t *testing.T) {
		d := evaluatePgrewindDecision(true, "sys1", "sys1", true, 0, "bad-wal")
		if !d.try {
			t.Fatal("expected pg_rewind to stay enabled when wal check fails")
		}
		if d.reason != pgrewindReasonWalCheckErr {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
		if d.walCheckErr == nil {
			t.Fatal("expected wal check error")
		}
	})

	t.Run("missing required wal disables pg_rewind", func(t *testing.T) {
		d := evaluatePgrewindDecision(
			true,
			"sys1",
			"sys1",
			true,
			0,
			"00000001000000000000000A",
		)
		if d.try {
			t.Fatal("expected pg_rewind to be disabled")
		}
		if d.reason != pgrewindReasonWalMissing {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
		if d.requiredWal == "" {
			t.Fatal("expected required wal to be populated")
		}
		if d.olderWal == "" {
			t.Fatal("expected older wal to be populated")
		}
	})

	t.Run("valid preconditions enable pg_rewind", func(t *testing.T) {
		d := evaluatePgrewindDecision(true, "sys1", "sys1", true, 0, "")
		if !d.try {
			t.Fatal("expected pg_rewind to be enabled")
		}
		if d.reason != pgrewindReasonAllowed {
			t.Fatalf("unexpected reason: %q", d.reason)
		}
		if d.walCheckErr != nil {
			t.Fatalf("unexpected wal check error: %v", d.walCheckErr)
		}
	})
}

func TestManagedReplicationSlotsRespectsIgnoreList(t *testing.T) {
	ignoredFollowerSlot := common.HysteronName("db2")
	ignoredAdditionalSlot := common.HysteronName("extra")
	managedFollowerSlot := common.HysteronName("db3")

	internalSlots, ignoredSlots := managedReplicationSlots(
		"db1",
		[]string{"db1", "db2", "db3"},
		[]string{"extra"},
		[]string{ignoredFollowerSlot, ignoredAdditionalSlot},
	)

	if _, ok := ignoredSlots[ignoredFollowerSlot]; !ok {
		t.Fatalf("expected ignored follower slot %q in ignored set", ignoredFollowerSlot)
	}
	if _, ok := ignoredSlots[ignoredAdditionalSlot]; !ok {
		t.Fatalf("expected ignored additional slot %q in ignored set", ignoredAdditionalSlot)
	}
	if _, ok := internalSlots[ignoredFollowerSlot]; ok {
		t.Fatalf("did not expect ignored follower slot %q in managed set", ignoredFollowerSlot)
	}
	if _, ok := internalSlots[ignoredAdditionalSlot]; ok {
		t.Fatalf("did not expect ignored additional slot %q in managed set", ignoredAdditionalSlot)
	}
	if _, ok := internalSlots[managedFollowerSlot]; !ok {
		t.Fatalf("expected managed follower slot %q in managed set", managedFollowerSlot)
	}
}

func TestStaleSlotsWithXmin(t *testing.T) {
	slots := []pg.PhysicalReplicationSlot{
		{Name: "hysteron_db2", Active: false, HasXmin: true},
		{Name: "hysteron_db3", Active: true, HasXmin: true},
		{Name: "hysteron_db4", Active: false, HasXmin: false},
		{Name: "hysteron_extra", Active: false, HasXmin: true},
		{Name: "manualslot", Active: false, HasXmin: true},
	}

	managed := map[string]struct{}{
		"hysteron_db2": {},
	}
	ignored := map[string]struct{}{
		"hysteron_extra": {},
	}

	got := staleSlotsWithXmin(slots, managed, ignored)
	want := []string{}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected stale slots, got: %v, want: %v", got, want)
	}

	got = staleSlotsWithXmin(slots, map[string]struct{}{}, ignored)
	want = []string{"hysteron_db2"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected stale slots, got: %v, want: %v", got, want)
	}
}

func TestShouldDropUnmanagedHysteronSlot(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	slot := common.HysteronName("db2")

	t.Run("ttl disabled keeps legacy drop behavior", func(t *testing.T) {
		drop, reason := shouldDropUnmanagedHysteronSlot(slot, 0, nil, nil, nil, now)
		if !drop || reason != "ttl_disabled" {
			t.Fatalf("unexpected decision: drop=%v reason=%s", drop, reason)
		}
	})

	t.Run("known member slot waits for orphan tracking", func(t *testing.T) {
		drop, reason := shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			map[string]time.Time{},
			nil,
			map[string]struct{}{"db2": {}},
			now,
		)
		if drop || reason != "awaiting_orphan_tracking" {
			t.Fatalf("unexpected decision: drop=%v reason=%s", drop, reason)
		}
	})

	t.Run("untracked unknown slot drops even with ttl", func(t *testing.T) {
		drop, reason := shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			map[string]time.Time{},
			nil,
			map[string]struct{}{},
			now,
		)
		if !drop || reason != "not_tracked_orphan" {
			t.Fatalf("unexpected decision: drop=%v reason=%s", drop, reason)
		}
	})

	t.Run("untracked non-member slot drops immediately even with ttl", func(t *testing.T) {
		drop, reason := shouldDropUnmanagedHysteronSlot(
			common.HysteronName("extra"),
			10*time.Minute,
			map[string]time.Time{},
			nil,
			map[string]struct{}{"db2": {}},
			now,
		)
		if !drop || reason != "not_tracked_orphan" {
			t.Fatalf("unexpected decision: drop=%v reason=%s", drop, reason)
		}
	})

	t.Run("tracked orphan respects ttl and state", func(t *testing.T) {
		orphan := map[string]time.Time{slot: now.Add(-5 * time.Minute)}
		state := map[string]pg.PhysicalReplicationSlot{
			slot: {Name: slot, Active: false, HasXmin: false},
		}

		drop, reason := shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			orphan,
			state,
			map[string]struct{}{"db2": {}},
			now,
		)
		if drop || reason != "ttl_not_elapsed" {
			t.Fatalf("unexpected decision before ttl: drop=%v reason=%s", drop, reason)
		}

		orphan[slot] = now.Add(-15 * time.Minute)
		drop, reason = shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			orphan,
			state,
			map[string]struct{}{"db2": {}},
			now,
		)
		if !drop || reason != "ttl_elapsed" {
			t.Fatalf("unexpected decision after ttl: drop=%v reason=%s", drop, reason)
		}

		state[slot] = pg.PhysicalReplicationSlot{Name: slot, Active: true, HasXmin: false}
		drop, reason = shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			orphan,
			state,
			map[string]struct{}{"db2": {}},
			now,
		)
		if drop || reason != "slot_active" {
			t.Fatalf("unexpected active-slot decision: drop=%v reason=%s", drop, reason)
		}

		state[slot] = pg.PhysicalReplicationSlot{Name: slot, Active: false, HasXmin: true}
		drop, reason = shouldDropUnmanagedHysteronSlot(
			slot,
			10*time.Minute,
			orphan,
			state,
			map[string]struct{}{"db2": {}},
			now,
		)
		if drop || reason != "slot_has_xmin" {
			t.Fatalf("unexpected xmin decision: drop=%v reason=%s", drop, reason)
		}
	})
}

func TestEvaluateManagedLogicalSlotsDecision(t *testing.T) {
	t.Run("creates missing managed slots", func(t *testing.T) {
		decision := evaluateManagedLogicalSlotsDecision(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
			},
			nil,
		)
		if len(decision.create) != 1 || decision.create[0].Name != "hysteron_slot1" {
			t.Fatalf("unexpected create decision: %+v", decision.create)
		}
	})

	t.Run("flags mismatched managed slot", func(t *testing.T) {
		decision := evaluateManagedLogicalSlotsDecision(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
			},
			[]pg.LogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "wal2json"},
			},
		)
		if !reflect.DeepEqual(decision.mismatch, []string{"hysteron_slot1"}) {
			t.Fatalf("unexpected mismatch decision: %+v", decision.mismatch)
		}
	})

	t.Run("drops only inactive unmanaged hysteron slots", func(t *testing.T) {
		decision := evaluateManagedLogicalSlotsDecision(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
			},
			[]pg.LogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput", Active: true},
				{Name: "hysteron_old", Database: "postgres", Plugin: "pgoutput", Active: false},
				{Name: "hysteron_active", Database: "postgres", Plugin: "pgoutput", Active: true},
				{Name: "external_slot", Database: "postgres", Plugin: "pgoutput", Active: false},
			},
		)

		if !reflect.DeepEqual(decision.drop, []string{"hysteron_old"}) {
			t.Fatalf("unexpected drop decision: %+v", decision.drop)
		}
		if !reflect.DeepEqual(decision.active, []string{"hysteron_active"}) {
			t.Fatalf("unexpected active decision: %+v", decision.active)
		}
	})
}

func TestShouldReconcileManagedLogicalSlots(t *testing.T) {
	t.Run("disabled when not configured", func(t *testing.T) {
		ok, reason := shouldReconcileManagedLogicalSlots(nil, cluster.PGParameters{})
		if ok || reason != "not_configured" {
			t.Fatalf("unexpected result: ok=%v reason=%s", ok, reason)
		}
	})

	t.Run("disabled when wal_level is not logical", func(t *testing.T) {
		ok, reason := shouldReconcileManagedLogicalSlots(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
			},
			cluster.PGParameters{"wal_level": "replica"},
		)
		if ok || reason != "wal_level_not_logical" {
			t.Fatalf("unexpected result: ok=%v reason=%s", ok, reason)
		}
	})

	t.Run("enabled when configured and wal_level logical", func(t *testing.T) {
		ok, reason := shouldReconcileManagedLogicalSlots(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
			},
			cluster.PGParameters{"wal_level": "logical"},
		)
		if !ok || reason != "enabled" {
			t.Fatalf("unexpected result: ok=%v reason=%s", ok, reason)
		}
	})
}

func TestEvaluateManagedLogicalSlotReadiness(t *testing.T) {
	t.Run("reports missing and mismatch slots", func(t *testing.T) {
		readiness := evaluateManagedLogicalSlotReadiness(
			[]cluster.ManagedLogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "pgoutput"},
				{Name: "hysteron_slot2", Database: "postgres", Plugin: "pgoutput"},
			},
			[]pg.LogicalReplicationSlot{
				{Name: "hysteron_slot1", Database: "postgres", Plugin: "wal2json"},
			},
		)
		if !reflect.DeepEqual(readiness.missing, []string{"hysteron_slot2"}) {
			t.Fatalf("unexpected missing readiness: %+v", readiness.missing)
		}
		if !reflect.DeepEqual(readiness.mismatch, []string{"hysteron_slot1"}) {
			t.Fatalf("unexpected mismatch readiness: %+v", readiness.mismatch)
		}
	})
}

func TestManagedLogicalSlotReadinessSignature(t *testing.T) {
	t.Run("returns stable sorted signature", func(t *testing.T) {
		sig := managedLogicalSlotReadinessSignature(managedLogicalSlotReadiness{
			missing:  []string{"b", "a"},
			mismatch: []string{"d", "c"},
		})
		want := "mismatch:c|mismatch:d|missing:a|missing:b"
		if sig != want {
			t.Fatalf("unexpected signature: got %q want %q", sig, want)
		}
	})

	t.Run("returns empty for ready state", func(t *testing.T) {
		sig := managedLogicalSlotReadinessSignature(managedLogicalSlotReadiness{})
		if sig != "" {
			t.Fatalf("expected empty signature, got %q", sig)
		}
	})
}

func TestShouldUseNativeLogicalSlotFailover(t *testing.T) {
	t.Run("disabled gate", func(t *testing.T) {
		if shouldUseNativeLogicalSlotFailover(false, 18) {
			t.Fatalf("expected false when gate is disabled")
		}
	})

	t.Run("enabled but pg16", func(t *testing.T) {
		if shouldUseNativeLogicalSlotFailover(true, 16) {
			t.Fatalf("expected false for pg16")
		}
	})

	t.Run("enabled on pg17", func(t *testing.T) {
		if !shouldUseNativeLogicalSlotFailover(true, 17) {
			t.Fatalf("expected true for pg17")
		}
	})
}

func TestEnforceHotStandbyFeedbackForLogicalSlotFailover(t *testing.T) {
	t.Run("disabled gate keeps existing value", func(t *testing.T) {
		params := common.Parameters{"hot_standby_feedback": "off"}
		enforceHotStandbyFeedbackForLogicalSlotFailover(params, false)
		if params["hot_standby_feedback"] != "off" {
			t.Fatalf("unexpected value: %q", params["hot_standby_feedback"])
		}
	})

	t.Run("enabled gate forces on", func(t *testing.T) {
		params := common.Parameters{"hot_standby_feedback": "off"}
		enforceHotStandbyFeedbackForLogicalSlotFailover(params, true)
		if params["hot_standby_feedback"] != "on" {
			t.Fatalf("unexpected value: %q", params["hot_standby_feedback"])
		}
	})
}

func TestShouldEmitLogicalSlotGateNotice(t *testing.T) {
	tests := []struct {
		name           string
		enabled        bool
		alreadyEmitted bool
		want           bool
	}{
		{name: "emit on first enable", enabled: true, alreadyEmitted: false, want: true},
		{name: "do not emit repeatedly", enabled: true, alreadyEmitted: true, want: false},
		{name: "do not emit when disabled", enabled: false, alreadyEmitted: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldEmitLogicalSlotGateNotice(tt.enabled, tt.alreadyEmitted)
			if got != tt.want {
				t.Fatalf("unexpected result: got %v want %v", got, tt.want)
			}
		})
	}
}
