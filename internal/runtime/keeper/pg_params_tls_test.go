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

package keeper

import (
	"testing"

	"github.com/woozymasta/hysteron/internal/cluster"
)

func TestGetReplConnParams_UsesReplicationTLSMode(t *testing.T) {
	p := &PostgresKeeper{
		pgReplUsername:   "repl",
		pgReplAuthMethod: "md5",
		pgReplPassword:   "secret",
	}
	db := &cluster.DB{
		UID: "db1",
		Spec: &cluster.DBSpec{
			ReplicationTLSMode: cluster.ReplicationTLSModeVerifyFull,
		},
	}
	followedDB := &cluster.DB{
		Status: cluster.DBStatus{
			ListenAddress: "10.0.0.2",
			Port:          "5432",
		},
	}

	params := p.getReplConnParams(db, followedDB)
	if got := params.Get("sslmode"); got != "verify-full" {
		t.Fatalf("sslmode=%q, want %q", got, "verify-full")
	}
}

func TestGetReplConnParams_DefaultsToPreferForEmptyMode(t *testing.T) {
	p := &PostgresKeeper{
		pgReplUsername:   "repl",
		pgReplAuthMethod: "md5",
		pgReplPassword:   "secret",
	}
	db := &cluster.DB{
		UID:  "db1",
		Spec: &cluster.DBSpec{},
	}
	followedDB := &cluster.DB{
		Status: cluster.DBStatus{
			ListenAddress: "10.0.0.2",
			Port:          "5432",
		},
	}

	params := p.getReplConnParams(db, followedDB)
	if got := params.Get("sslmode"); got != "prefer" {
		t.Fatalf("sslmode=%q, want %q", got, "prefer")
	}
}
