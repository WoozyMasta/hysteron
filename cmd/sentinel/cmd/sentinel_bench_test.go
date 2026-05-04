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
	"fmt"
	"testing"
	"time"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
)

func BenchmarkSentinelDBStatus(b *testing.B) {
	s := benchmarkSentinel()
	cd := benchmarkSentinelClusterData(8, 0)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.dbStatus(cd, "db-3")
	}
}

func BenchmarkSentinelValidStandbysByStatus(b *testing.B) {
	s := benchmarkSentinel()
	cd := benchmarkSentinelClusterData(8, 0)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.validStandbysByStatus(cd)
	}
}

func BenchmarkSentinelFindBestStandbys(b *testing.B) {
	s := benchmarkSentinel()
	cd := benchmarkSentinelClusterData(8, 0)
	masterDB := cd.DBs[cd.Cluster.Status.Master]

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.findBestStandbys(cd, masterDB)
	}
}

func BenchmarkSentinelFindBestNewMasters(b *testing.B) {
	s := benchmarkSentinel()
	cd := benchmarkSentinelClusterData(8, 0)
	masterDB := cd.DBs[cd.Cluster.Status.Master]

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.findBestNewMasters(cd, masterDB)
	}
}

func BenchmarkSentinelUpdateClusterNormal(b *testing.B) {
	s := benchmarkSentinel()
	cd := benchmarkSentinelClusterData(8, 2)
	pis := cluster.ProxiesInfo{
		"proxy-0": {
			UID:          "proxy-0",
			Generation:   1,
			ProxyTimeout: time.Second,
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.updateCluster(cd, pis); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSentinel() *Sentinel {
	nextUID := 0
	return &Sentinel{
		uid:                    "sentinel-0",
		UIDFn:                  func() string { nextUID++; return fmt.Sprintf("generated-db-%d", nextUID) },
		RandFn:                 func(int) int { return 0 },
		keeperErrorTimers:      map[string]int64{},
		dbErrorTimers:          map[string]int64{},
		dbNotIncreasingXLogPos: map[string]int64{},
		dbConvergenceInfos:     map[string]*DBConvergenceInfo{},
		keeperInfoHistories:    KeeperInfoHistories{},
		proxyInfoHistories:     ProxyInfoHistories{},
	}
}

func benchmarkSentinelClusterData(standbys, freeKeepers int) *cluster.ClusterData {
	now := time.Now()
	cd := cluster.NewClusterData(&cluster.Cluster{
		UID:        "cluster-0",
		Generation: cluster.InitialGeneration,
		Spec: &cluster.ClusterSpec{
			InitMode:               cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
			MaxStandbys:            cluster.Uint16P(uint16(standbys + freeKeepers)),
			MaxStandbysPerSender:   cluster.Uint16P(uint16(standbys)),
			MaxStandbyLag:          cluster.Uint32P(256 * 1024 * 1024),
			SynchronousReplication: cluster.BoolP(false),
			Role:                   cluster.ClusterRoleP(cluster.ClusterRoleMaster),
		},
		Status: cluster.ClusterStatus{
			Phase:  cluster.ClusterPhaseNormal,
			Master: "db-0",
		},
	})
	cd.ChangeTime = now
	cd.Proxy = &cluster.Proxy{
		UID:        "proxy",
		Generation: 1,
		Spec: cluster.ProxySpec{
			MasterDBUID:    "db-0",
			EnabledProxies: []string{"proxy-0"},
		},
	}

	totalKeepers := standbys + freeKeepers + 1
	for i := 0; i < totalKeepers; i++ {
		keeperUID := fmt.Sprintf("keeper-%d", i)
		cd.Keepers[keeperUID] = &cluster.Keeper{
			UID:        keeperUID,
			Generation: cluster.InitialGeneration,
			Spec:       &cluster.KeeperSpec{},
			Status: cluster.KeeperStatus{
				Healthy:         true,
				LastHealthyTime: now,
				BootUUID:        fmt.Sprintf("boot-%d", i),
				PostgresBinaryVersion: cluster.PostgresBinaryVersion{
					Maj: 12,
				},
			},
		}
	}

	master := benchmarkSentinelDB("db-0", "keeper-0", common.RoleMaster, nil, 0x4000000)
	cd.DBs[master.UID] = master

	for i := 1; i <= standbys; i++ {
		dbUID := fmt.Sprintf("db-%d", i)
		keeperUID := fmt.Sprintf("keeper-%d", i)
		followConfig := &cluster.FollowConfig{
			Type:  cluster.FollowTypeInternal,
			DBUID: master.UID,
		}
		cd.DBs[dbUID] = benchmarkSentinelDB(
			dbUID,
			keeperUID,
			common.RoleStandby,
			followConfig,
			master.Status.XLogPos-uint64(i*4096),
		)
		master.Spec.Followers = append(master.Spec.Followers, dbUID)
	}

	return cd
}

func benchmarkSentinelDB(uid, keeperUID string, role common.Role, followConfig *cluster.FollowConfig, xlogPos uint64) *cluster.DB {
	return &cluster.DB{
		UID:        uid,
		Generation: cluster.InitialGeneration,
		Spec: &cluster.DBSpec{
			KeeperUID:              keeperUID,
			InitMode:               cluster.DBInitModeNone,
			Role:                   role,
			FollowConfig:           followConfig,
			Followers:              []string{},
			AdditionalWalSenders:   cluster.DefaultAdditionalWalSenders,
			MaxStandbys:            cluster.DefaultMaxStandbys,
			SynchronousReplication: false,
			UsePgrewind:            cluster.DefaultUsePgrewind,
		},
		Status: cluster.DBStatus{
			Healthy:           true,
			CurrentGeneration: cluster.InitialGeneration,
			SystemID:          "system-0",
			TimelineID:        1,
			XLogPos:           xlogPos,
		},
	}
}
