// Copyright 2015 Sorint.lab
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

package postgresql

import "testing"

func TestManagerIsStartedMissingDataDir(t *testing.T) {
	manager := NewManager(
		"missing-bin",
		t.TempDir(),
		"",
		nil,
		nil,
		"trust",
		"superuser",
		"",
		"trust",
		"replication",
		"",
		0,
	)

	started, err := manager.IsStarted()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if started {
		t.Fatal("expected missing data dir to be stopped")
	}
}
