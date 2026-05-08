// Copyright 2018 Sorint.lab
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

package cluster

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestValidateReplicationSlots(t *testing.T) {
	tests := []struct {
		in  string
		err error
	}{
		{
			in: "goodslotname_434432",
		},
		{
			in:  "badslotname-34223",
			err: errors.New(`wrong replication slot name: "badslotname-34223"`),
		},
		{
			in:  "badslotname\n",
			err: errors.New(`wrong replication slot name: "badslotname\n"`),
		},
		{
			in:  " badslotname",
			err: errors.New(`wrong replication slot name: " badslotname"`),
		},
		{
			in:  "badslotname ",
			err: errors.New(`wrong replication slot name: "badslotname "`),
		},
		{
			in:  "hysteron_c874a3cb",
			err: errors.New(`replication slot name is reserved: "hysteron_c874a3cb"`),
		},
	}

	for i, tt := range tests {
		err := validateReplicationSlot(tt.in)

		if tt.err != nil {
			if err == nil {
				t.Errorf("#%d: got no error, wanted error: %v", i, tt.err)
			} else if tt.err.Error() != err.Error() {
				t.Errorf("#%d: got error: %v, wanted error: %v", i, err, tt.err)
			}
		} else {
			if err != nil {
				t.Errorf("#%d: unexpected error: %v", i, err)
			}
		}
	}
}

func TestClusterData_FindDB(t *testing.T) {
	db := DB{
		UID:  "dbUUID",
		Spec: &DBSpec{KeeperUID: "sameKeeperUUID"},
	}
	tests := []struct {
		name        string
		clusterData ClusterData
		keeper      *Keeper
		expectedDB  *DB
	}{
		{
			name:        "should return nil if the clusterData is empty",
			clusterData: ClusterData{},
			keeper:      &Keeper{},
			expectedDB:  nil,
		},
		{
			name: "should return nil if DB is not found for given keeper",
			clusterData: ClusterData{
				DBs: map[string]*DB{
					"dbUUID": &db,
				},
			},
			keeper:     &Keeper{UID: "differentUUID"},
			expectedDB: nil,
		},
		{
			name: "should return the DB if DB is found for given keeper",
			clusterData: ClusterData{
				DBs: map[string]*DB{
					"dbUUID": &db,
				},
			},
			keeper:     &Keeper{UID: "sameKeeperUUID"},
			expectedDB: &db,
		},
	}

	for _, tt := range tests {
		actual := tt.clusterData.FindDB(tt.keeper)
		if actual != tt.expectedDB {
			t.Errorf("Expected %v, but got %v", tt.expectedDB, actual)
		}
	}
}

func TestClusterSpecWithDefaultsDoesNotMutateOriginal(t *testing.T) {
	spec := &ClusterSpec{}

	defaulted := spec.WithDefaults()

	if spec.SleepInterval != nil {
		t.Fatalf("WithDefaults mutated original SleepInterval: %v", spec.SleepInterval)
	}
	if defaulted.SleepInterval == nil || defaulted.SleepInterval.Duration != DefaultSleepInterval {
		t.Fatalf("unexpected default SleepInterval: %v", defaulted.SleepInterval)
	}
	if defaulted.RequestTimeout == nil || defaulted.RequestTimeout.Duration != DefaultRequestTimeout {
		t.Fatalf("unexpected default RequestTimeout: %v", defaulted.RequestTimeout)
	}
	if defaulted.InitMode != nil {
		t.Fatalf("InitMode should not be defaulted: %v", *defaulted.InitMode)
	}
}

func TestClusterSpecValidate(t *testing.T) {
	newMode := ClusterInitModeNew
	existingMode := ClusterInitModeExisting
	pitrMode := ClusterInitModePITR
	standbyRole := ClusterRoleStandby
	strictAccess := SUReplAccessStrict
	unknownAccess := SUReplAccessMode("unknown")
	negativeDuration := &Duration{Duration: -time.Second}
	oneSecond := &Duration{Duration: time.Second}
	twoSeconds := &Duration{Duration: 2 * time.Second}
	zero := uint16(0)
	one := uint16(1)
	two := uint16(2)

	tests := []struct {
		spec    *ClusterSpec
		name    string
		wantErr string
	}{
		{
			name: "new cluster spec is valid",
			spec: &ClusterSpec{InitMode: &newMode},
		},
		{
			name:    "init mode is required",
			spec:    &ClusterSpec{},
			wantErr: "initMode undefined",
		},
		{
			name:    "negative duration is rejected",
			spec:    &ClusterSpec{InitMode: &newMode, SleepInterval: negativeDuration},
			wantErr: "sleepInterval must be positive",
		},
		{
			name: "proxy check interval must be lower than proxy timeout",
			spec: &ClusterSpec{
				InitMode:           &newMode,
				ProxyCheckInterval: twoSeconds,
				ProxyTimeout:       oneSecond,
			},
			wantErr: "proxyCheckInterval should be less than proxyTimeout",
		},
		{
			name: "ha timing must fit fail interval budget",
			spec: &ClusterSpec{
				InitMode:       &newMode,
				SleepInterval:  &Duration{Duration: 5 * time.Second},
				RequestTimeout: &Duration{Duration: 10 * time.Second},
				FailInterval:   &Duration{Duration: 20 * time.Second},
			},
			wantErr: "invalid HA timing: sleepInterval + 2*requestTimeout must be less than or equal to failInterval",
		},
		{
			name: "ha timing check applies to partial overrides with defaults",
			spec: &ClusterSpec{
				InitMode:      &newMode,
				SleepInterval: &Duration{Duration: 11 * time.Second},
			},
			wantErr: "invalid HA timing: sleepInterval + 2*requestTimeout must be less than or equal to failInterval",
		},
		{
			name: "pitr rejects conflicting recovery target selectors",
			spec: &ClusterSpec{
				InitMode: &pitrMode,
				PITRConfig: &PITRConfig{
					DataRestoreCommand: "restore",
					RecoveryTargetSettings: &RecoveryTargetSettings{
						RecoveryTargetName: "rp1",
						RecoveryTargetXid:  "123",
					},
				},
			},
			wantErr: "only one recovery target selector can be set among recoveryTarget, recoveryTargetLsn, recoveryTargetName, recoveryTargetTime, recoveryTargetXid",
		},
		{
			name: "pitr rejects unsupported recoveryTarget value",
			spec: &ClusterSpec{
				InitMode: &pitrMode,
				PITRConfig: &PITRConfig{
					DataRestoreCommand: "restore",
					RecoveryTargetSettings: &RecoveryTargetSettings{
						RecoveryTarget: "latest",
					},
				},
			},
			wantErr: `recoveryTarget must be "immediate" when defined, got "latest"`,
		},
		{
			name:    "max standbys must be positive",
			spec:    &ClusterSpec{InitMode: &newMode, MaxStandbys: &zero},
			wantErr: "maxStandbys must be at least 1",
		},
		{
			name: "max synchronous standbys must cover min synchronous standbys",
			spec: &ClusterSpec{
				InitMode:               &newMode,
				MinSynchronousStandbys: &two,
				MaxSynchronousStandbys: &one,
			},
			wantErr: "maxSynchronousStandbys must be greater or equal to minSynchronousStandbys",
		},
		{
			name:    "reserved replication slot is rejected",
			spec:    &ClusterSpec{InitMode: &newMode, AdditionalMasterReplicationSlots: []string{"hysteron_reserved"}},
			wantErr: `replication slot name is reserved: "hysteron_reserved"`,
		},
		{
			name:    "invalid ignored replication slot name is rejected",
			spec:    &ClusterSpec{InitMode: &newMode, IgnoreMasterReplicationSlots: []string{"bad-slot-name"}},
			wantErr: `wrong replication slot name: "bad-slot-name"`,
		},
		{
			name: "negative member replication slot ttl is rejected",
			spec: &ClusterSpec{
				InitMode:                 &newMode,
				MemberReplicationSlotTTL: negativeDuration,
			},
			wantErr: "memberReplicationSlotTTL must be positive",
		},
		{
			name: "managed logical slot validates required fields",
			spec: &ClusterSpec{
				InitMode: &newMode,
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "goodslot", Plugin: "pgoutput"},
				},
			},
			wantErr: `managedLogicalReplicationSlots database undefined for slot "goodslot"`,
		},
		{
			name: "managed logical slot rejects duplicate names",
			spec: &ClusterSpec{
				InitMode: &newMode,
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "dup_slot", Database: "postgres", Plugin: "pgoutput"},
					{Name: "dup_slot", Database: "postgres", Plugin: "wal2json"},
				},
			},
			wantErr: `duplicated managedLogicalReplicationSlots name: "dup_slot"`,
		},
		{
			name: "managed logical slot validates slot name",
			spec: &ClusterSpec{
				InitMode: &newMode,
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "bad-slot", Database: "postgres", Plugin: "pgoutput"},
				},
			},
			wantErr: `wrong replication slot name: "bad-slot"`,
		},
		{
			name: "managed logical slot accepts valid config",
			spec: &ClusterSpec{
				InitMode: &newMode,
				PGParameters: PGParameters{
					"wal_level": "logical",
				},
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "slot_ok", Database: "postgres", Plugin: "pgoutput"},
				},
			},
		},
		{
			name: "managed logical slot requires wal_level logical",
			spec: &ClusterSpec{
				InitMode: &newMode,
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "slot_ok", Database: "postgres", Plugin: "pgoutput"},
				},
			},
			wantErr: `managedLogicalReplicationSlots requires pgParameters.wal_level to be set to "logical"`,
		},
		{
			name: "managed logical slot rejects non logical wal_level",
			spec: &ClusterSpec{
				InitMode: &newMode,
				PGParameters: PGParameters{
					"wal_level": "replica",
				},
				ManagedLogicalReplicationSlots: []ManagedLogicalReplicationSlot{
					{Name: "slot_ok", Database: "postgres", Plugin: "pgoutput"},
				},
			},
			wantErr: `managedLogicalReplicationSlots requires pgParameters.wal_level to be set to "logical"`,
		},
		{
			name:    "pg hba entries cannot contain newline characters",
			spec:    &ClusterSpec{InitMode: &newMode, PGHBA: []string{"host all all 127.0.0.1/32 trust\n"}},
			wantErr: "pgHBA entries cannot contain newline characters",
		},
		{
			name:    "new init cannot request standby role",
			spec:    &ClusterSpec{InitMode: &newMode, Role: &standbyRole},
			wantErr: `invalid cluster role standby when initMode is "new"`,
		},
		{
			name:    "existing init requires existing config",
			spec:    &ClusterSpec{InitMode: &existingMode},
			wantErr: `existingConfig undefined. Required when initMode is "existing"`,
		},
		{
			name:    "existing init requires keeper uid",
			spec:    &ClusterSpec{InitMode: &existingMode, ExistingConfig: &ExistingConfig{}},
			wantErr: "existingConfig.keeperUID undefined",
		},
		{
			name:    "pitr init requires pitr config",
			spec:    &ClusterSpec{InitMode: &pitrMode},
			wantErr: `pitrConfig undefined. Required when initMode is "pitr"`,
		},
		{
			name:    "pitr init requires restore command",
			spec:    &ClusterSpec{InitMode: &pitrMode, PITRConfig: &PITRConfig{}},
			wantErr: "pitrConfig.DataRestoreCommand undefined",
		},
		{
			name:    "strict su repl access is valid",
			spec:    &ClusterSpec{InitMode: &newMode, DefaultSUReplAccessMode: &strictAccess},
			wantErr: "",
		},
		{
			name:    "unknown su repl access is rejected",
			spec:    &ClusterSpec{InitMode: &newMode, DefaultSUReplAccessMode: &unknownAccess},
			wantErr: `unknown defaultSUReplAccessMode: "unknown"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("got error %q, wanted %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestClusterUpdateSpecRejectsImmutableChanges(t *testing.T) {
	newMode := ClusterInitModeNew
	existingMode := ClusterInitModeExisting
	masterRole := ClusterRoleMaster
	standbyRole := ClusterRoleStandby

	tests := []struct {
		cluster *Cluster
		newSpec *ClusterSpec
		name    string
		wantErr string
	}{
		{
			name:    "rejects init mode change",
			cluster: &Cluster{Spec: &ClusterSpec{InitMode: &newMode}},
			newSpec: &ClusterSpec{
				InitMode:       &existingMode,
				ExistingConfig: &ExistingConfig{KeeperUID: "keeper1"},
			},
			wantErr: "cannot change cluster init mode",
		},
		{
			name: "rejects master to standby role change",
			cluster: &Cluster{
				Spec: &ClusterSpec{
					InitMode:       &existingMode,
					Role:           &masterRole,
					ExistingConfig: &ExistingConfig{KeeperUID: "keeper1"},
				},
			},
			newSpec: &ClusterSpec{
				InitMode:       &existingMode,
				Role:           &standbyRole,
				ExistingConfig: &ExistingConfig{KeeperUID: "keeper1"},
				StandbyConfig:  &StandbyConfig{},
			},
			wantErr: "cannot update a cluster from master role to standby role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cluster.UpdateSpec(tt.newSpec)
			if err == nil {
				t.Fatalf("expected error %q", tt.wantErr)
			}
			if err.Error() != "invalid cluster spec: "+tt.wantErr && err.Error() != tt.wantErr {
				t.Fatalf("got error %q, wanted %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestDurationJSON(t *testing.T) {
	d := Duration{Duration: 90 * time.Second}

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	if string(data) != `"1m30s"` {
		t.Fatalf("got json %s, wanted %q", data, `"1m30s"`)
	}

	var decoded Duration
	if err := json.Unmarshal([]byte(`"250ms"`), &decoded); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if decoded.Duration != 250*time.Millisecond {
		t.Fatalf("got duration %s, wanted 250ms", decoded.Duration)
	}

	if err := json.Unmarshal([]byte(`"not-a-duration"`), &decoded); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestClusterDataDeepCopyIsIndependent(t *testing.T) {
	mode := ClusterInitModeNew
	cd := NewClusterData(NewCluster("cluster1", &ClusterSpec{InitMode: &mode}))
	cd.Keepers["keeper1"] = &Keeper{UID: "keeper1"}
	cd.DBs["db1"] = &DB{
		UID:  "db1",
		Spec: &DBSpec{KeeperUID: "keeper1"},
		Status: DBStatus{
			PGParameters: PGParameters{"max_connections": "100"},
		},
	}

	copied := cd.DeepCopy()
	copied.Cluster.UID = "cluster2"
	copied.Keepers["keeper1"].UID = "keeper2"
	copied.DBs["db1"].Status.PGParameters["max_connections"] = "200"

	if cd.Cluster.UID != "cluster1" {
		t.Fatalf("original cluster uid was mutated: %q", cd.Cluster.UID)
	}
	if cd.Keepers["keeper1"].UID != "keeper1" {
		t.Fatalf("original keeper uid was mutated: %q", cd.Keepers["keeper1"].UID)
	}
	if cd.DBs["db1"].Status.PGParameters["max_connections"] != "100" {
		t.Fatalf("original pg parameters were mutated: %#v", cd.DBs["db1"].Status.PGParameters)
	}
}
