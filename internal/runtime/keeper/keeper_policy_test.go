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
	"testing"

	"github.com/woozymasta/hysteron/internal/common"
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
