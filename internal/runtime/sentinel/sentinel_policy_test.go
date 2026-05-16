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

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
)

func TestChooseBestNewMaster_RespectsEffectiveClusterSyncMode(t *testing.T) {
	s := &Sentinel{
		dbConvergenceInfos:      map[string]*DBConvergenceInfo{},
		forceFailedKeeperUIDs:   map[string]struct{}{},
		leaderRaceBackoffTimers: map[string]time.Time{},
	}

	cd := &cluster.ClusterData{
		Cluster: &cluster.Cluster{
			UID: "cluster1",
			Spec: &cluster.ClusterSpec{
				SynchronousReplication: cluster.BoolP(false),
				MaxStandbyLag:          cluster.Uint32P(cluster.DefaultMaxStandbyLag),
			},
			Status: cluster.ClusterStatus{
				Master: "db1",
			},
		},
		Keepers: cluster.Keepers{
			"keeper1": {
				UID: "keeper1",
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
			"keeper2": {
				UID: "keeper2",
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
		},
		DBs: cluster.DBs{
			"db1": {
				UID: "db1",
				Spec: &cluster.DBSpec{
					KeeperUID:              "keeper1",
					Role:                   common.RoleMaster,
					SynchronousReplication: true,
					SynchronousStandbys:    []string{"db3"},
				},
				Status: cluster.DBStatus{
					Healthy:             false,
					CurrentGeneration:   1,
					TimelineID:          1,
					XLogPos:             1000,
					SynchronousStandbys: []string{"db3"},
				},
			},
			"db2": {
				UID:        "db2",
				Generation: 1,
				Spec: &cluster.DBSpec{
					KeeperUID: "keeper2",
					Role:      common.RoleStandby,
					FollowConfig: &cluster.FollowConfig{
						Type:  cluster.FollowTypeInternal,
						DBUID: "db1",
					},
				},
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 1,
					TimelineID:        1,
					XLogPos:           1000,
				},
			},
		},
	}

	newMaster, ok := s.chooseBestNewMaster(cd, cd.DBs["db1"], 0)
	if !ok {
		t.Fatalf("expected a best new master candidate")
	}
	if newMaster == nil {
		t.Fatalf("expected non-nil new master")
	}
	if newMaster.UID != "db2" {
		t.Fatalf("unexpected new master uid, got: %q, want: %q", newMaster.UID, "db2")
	}
}

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

func TestShouldDelayLeaderRace(t *testing.T) {
	failedMaster := &cluster.DB{UID: "master1"}
	candidate := &cluster.DB{UID: "standby1"}
	window := 5 * time.Second

	t.Run("starts backoff when wal is increasing", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{},
			dbIncreasingXLogPosObservedAt: map[string]time.Time{
				candidate.UID: time.Now(),
			},
			leaderRaceBackoffTimers: map[string]time.Time{},
		}
		if !s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("expected leader race delay on first observation")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; !ok {
			t.Fatalf("expected backoff timer to be created")
		}
	})

	t.Run("stops delaying after window elapses", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{},
			dbIncreasingXLogPosObservedAt: map[string]time.Time{
				candidate.UID: time.Now(),
			},
			leaderRaceBackoffTimers: map[string]time.Time{
				failedMaster.UID: time.Now().Add(-window - time.Second),
			},
		}
		if s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("expected leader race delay to expire")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; ok {
			t.Fatalf("expected expired backoff timer to be cleared")
		}
	})

	t.Run("does not delay when wal is stalled", func(t *testing.T) {
		s := &Sentinel{
			dbNotIncreasingXLogPos: map[string]int64{
				candidate.UID: cluster.DefaultDBNotIncreasingXLogPosTimes + 1,
			},
			dbIncreasingXLogPosObservedAt: map[string]time.Time{
				candidate.UID: time.Now(),
			},
			leaderRaceBackoffTimers: map[string]time.Time{
				failedMaster.UID: time.Now(),
			},
		}
		if s.shouldDelayLeaderRace(failedMaster, []*cluster.DB{candidate}, window) {
			t.Fatalf("did not expect leader race delay when wal is stalled")
		}
		if _, ok := s.leaderRaceBackoffTimers[failedMaster.UID]; ok {
			t.Fatalf("expected stale backoff timer to be cleared")
		}
	})
}

func TestUpdateCluster_SkipsMutationsWhenPaused(t *testing.T) {
	s := &Sentinel{
		dbConvergenceInfos: map[string]*DBConvergenceInfo{},
		UIDFn: func() string {
			return "newdb"
		},
	}

	cd := &cluster.ClusterData{
		Cluster: &cluster.Cluster{
			UID: "cluster1",
			Spec: &cluster.ClusterSpec{
				InitMode: cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
			},
			Status: cluster.ClusterStatus{
				Phase:             cluster.ClusterPhaseNormal,
				Master:            "db1",
				CurrentGeneration: 1,
				Paused:            true,
			},
		},
		Keepers: cluster.Keepers{
			"keeper1": {
				UID: "keeper1",
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
		},
		DBs: cluster.DBs{
			"db1": {
				UID:        "db1",
				Generation: 1,
				Spec: &cluster.DBSpec{
					KeeperUID: "keeper1",
					Role:      common.RoleMaster,
					Followers: []string{},
				},
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 1,
				},
			},
		},
		Proxy: &cluster.Proxy{},
	}

	got, err := s.updateCluster(cd, cluster.ProxiesInfo{})
	if err != nil {
		t.Fatalf("updateCluster() error: %v", err)
	}
	if got == nil {
		t.Fatalf("updateCluster() returned nil cluster data")
	}
	if !reflect.DeepEqual(got, cd) {
		t.Fatalf("updateCluster() mutated paused cluster data")
	}
}

func TestUpdateCluster_AppliesManualSwitchoverRequest(t *testing.T) {
	s := &Sentinel{
		dbConvergenceInfos: map[string]*DBConvergenceInfo{},
		UIDFn: func() string {
			return "newdb"
		},
	}

	cd := &cluster.ClusterData{
		Cluster: &cluster.Cluster{
			UID: "cluster1",
			Spec: &cluster.ClusterSpec{
				InitMode:               cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
				Role:                   cluster.ClusterRoleP(cluster.ClusterRoleMaster),
				SynchronousReplication: cluster.BoolP(false),
				MaxStandbyLag:          cluster.Uint32P(cluster.DefaultMaxStandbyLag),
			},
			Status: cluster.ClusterStatus{
				Phase:             cluster.ClusterPhaseNormal,
				Master:            "db1",
				CurrentGeneration: 1,
				ManualSwitch: &cluster.ManualSwitchRequest{
					TargetKeeperUID: "keeper2",
					Mode:            cluster.ManualSwitchModeSwitchover,
				},
			},
		},
		Keepers: cluster.Keepers{
			"keeper1": {
				UID: "keeper1",
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
			"keeper2": {
				UID: "keeper2",
				Status: cluster.KeeperStatus{
					Healthy: true,
				},
			},
		},
		DBs: cluster.DBs{
			"db1": {
				UID:        "db1",
				Generation: 1,
				Spec: &cluster.DBSpec{
					KeeperUID: "keeper1",
					Role:      common.RoleMaster,
					Followers: []string{"db2"},
				},
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 1,
					TimelineID:        1,
					XLogPos:           100,
				},
			},
			"db2": {
				UID:        "db2",
				Generation: 1,
				Spec: &cluster.DBSpec{
					KeeperUID: "keeper2",
					Role:      common.RoleStandby,
					FollowConfig: &cluster.FollowConfig{
						Type:  cluster.FollowTypeInternal,
						DBUID: "db1",
					},
					Followers: []string{},
				},
				Status: cluster.DBStatus{
					Healthy:           true,
					CurrentGeneration: 1,
					TimelineID:        1,
					XLogPos:           100,
				},
			},
		},
		Proxy: &cluster.Proxy{},
	}

	got, err := s.updateCluster(cd, cluster.ProxiesInfo{})
	if err != nil {
		t.Fatalf("updateCluster() error: %v", err)
	}
	if got == nil {
		t.Fatalf("updateCluster() returned nil cluster data")
	}
	if got.Cluster.Status.Master != "db2" {
		t.Fatalf("expected new master db2, got %q", got.Cluster.Status.Master)
	}
	if got.Cluster.Status.ManualSwitch != nil {
		t.Fatalf("expected manual switch request to be cleared")
	}
}
