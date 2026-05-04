// Copyright 2026 Sorint.lab
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
	"fmt"
	"testing"

	"github.com/sorintlab/stolon/internal/common"
)

var benchmarkClusterData = newBenchmarkClusterData(8)

func BenchmarkClusterDataMarshal(b *testing.B) {
	cd := benchmarkClusterData

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(cd)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) == 0 {
			b.Fatal("empty cluster data")
		}
	}
}

func BenchmarkClusterDataUnmarshal(b *testing.B) {
	data, err := json.Marshal(benchmarkClusterData)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var cd ClusterData
		if err := json.Unmarshal(data, &cd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterDataDeepCopy(b *testing.B) {
	cd := benchmarkClusterData

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		copied := cd.DeepCopy()
		if copied.Cluster.UID == "" {
			b.Fatal("empty cluster uid")
		}
	}
}

func BenchmarkClusterDataFindDB(b *testing.B) {
	cd := benchmarkClusterData
	keeper := cd.Keepers["keeper7"]

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db := cd.FindDB(keeper)
		if db == nil {
			b.Fatal("db not found")
		}
	}
}

func BenchmarkClusterSpecValidate(b *testing.B) {
	spec := benchmarkClusterData.Cluster.Spec

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := spec.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkClusterData(size int) *ClusterData {
	initMode := ClusterInitModeNew
	spec := &ClusterSpec{
		InitMode:                         &initMode,
		AdditionalMasterReplicationSlots: []string{"slot_a", "slot_b"},
		PGHBA: []string{
			"host all all 127.0.0.1/32 md5",
			"host replication repluser 10.0.0.0/8 md5",
		},
		PGParameters: PGParameters{
			"max_connections":            "200",
			"max_replication_slots":      "20",
			"max_wal_senders":            "20",
			"wal_keep_size":              "128MB",
			"shared_buffers":             "256MB",
			"log_min_duration_statement": "500",
		},
	}
	cd := NewClusterData(NewCluster("cluster1", spec))

	for i := 0; i < size; i++ {
		keeperUID := fmt.Sprintf("keeper%d", i)
		dbUID := fmt.Sprintf("db%d", i)
		cd.Keepers[keeperUID] = &Keeper{
			UID: keeperUID,
			Status: KeeperStatus{
				Healthy: true,
			},
		}
		cd.DBs[dbUID] = &DB{
			UID: dbUID,
			Spec: &DBSpec{
				KeeperUID: keeperUID,
				Role:      dbRole(i),
			},
			Status: DBStatus{
				Healthy:       true,
				ListenAddress: fmt.Sprintf("10.0.0.%d", i+1),
				Port:          "5432",
				SystemID:      "system1",
				TimelineID:    uint64(i + 1),
				XLogPos:       uint64(1024 * (i + 1)),
				PGParameters: PGParameters{
					"max_connections":       "200",
					"max_replication_slots": "20",
				},
			},
		}
		if i > 0 {
			cd.DBs[dbUID].Spec.FollowConfig = &FollowConfig{
				Type:  FollowTypeInternal,
				DBUID: "db0",
			}
		}
	}

	cd.Cluster.Status.Master = "db0"
	cd.Proxy.Spec.MasterDBUID = "db0"
	return cd
}

func dbRole(i int) common.Role {
	if i == 0 {
		return common.RoleMaster
	}
	return common.RoleStandby
}
