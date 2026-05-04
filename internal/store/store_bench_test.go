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

package store

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
)

func BenchmarkKVBackedPutClusterData(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	cd := benchmarkStoreClusterData(8)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.PutClusterData(context.Background(), cd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedAtomicPutClusterData(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	cd := benchmarkStoreClusterData(8)
	previous := &KVPair{Key: filepath.Join("/cluster", clusterDataFile), LastIndex: 1}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.AtomicPutClusterData(context.Background(), cd, previous); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedGetClusterData(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	cd := benchmarkStoreClusterData(8)
	if err := s.PutClusterData(context.Background(), cd); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := s.GetClusterData(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedSetKeeperInfo(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	ki := benchmarkStoreKeeperInfo("keeper-0")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.SetKeeperInfo(context.Background(), ki.UID, ki, time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedGetKeepersInfo(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	for i := 0; i < 8; i++ {
		ki := benchmarkStoreKeeperInfo("keeper-" + strconv.Itoa(i))
		if err := s.SetKeeperInfo(context.Background(), ki.UID, ki, time.Second); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.GetKeepersInfo(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedSetProxyInfo(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	pi := &cluster.ProxyInfo{
		InfoUID:      "info-0",
		UID:          "proxy-0",
		Generation:   cluster.InitialGeneration,
		ProxyTimeout: cluster.DefaultProxyTimeout,
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.SetProxyInfo(context.Background(), pi, time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVBackedGetProxiesInfo(b *testing.B) {
	s := newBenchmarkKVBackedStore()
	for i := 0; i < 4; i++ {
		index := strconv.Itoa(i)
		pi := &cluster.ProxyInfo{
			InfoUID:      "info-" + index,
			UID:          "proxy-" + index,
			Generation:   cluster.InitialGeneration,
			ProxyTimeout: cluster.DefaultProxyTimeout,
		}
		if err := s.SetProxyInfo(context.Background(), pi, time.Second); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.GetProxiesInfo(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkKVBackedStore() *KVBackedStore {
	return NewKVBackedStore(&benchmarkKVStore{values: map[string]*KVPair{}}, "/cluster")
}

func benchmarkStoreClusterData(size int) *cluster.ClusterData {
	cd := cluster.NewClusterData(&cluster.Cluster{
		UID:        "cluster-0",
		Generation: cluster.InitialGeneration,
		Spec: &cluster.ClusterSpec{
			InitMode: cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
			Role:     cluster.ClusterRoleP(cluster.ClusterRoleMaster),
		},
		Status: cluster.ClusterStatus{
			Phase:  cluster.ClusterPhaseNormal,
			Master: "db-0",
		},
	})
	cd.Proxy = &cluster.Proxy{
		UID:        "proxy",
		Generation: cluster.InitialGeneration,
		Spec: cluster.ProxySpec{
			MasterDBUID:    "db-0",
			EnabledProxies: []string{"proxy-0", "proxy-1"},
		},
	}
	for i := 0; i < size; i++ {
		index := strconv.Itoa(i)
		keeperUID := "keeper-" + index
		dbUID := "db-" + index
		cd.Keepers[keeperUID] = &cluster.Keeper{
			UID:        keeperUID,
			Generation: cluster.InitialGeneration,
			Spec:       &cluster.KeeperSpec{},
			Status: cluster.KeeperStatus{
				Healthy:         true,
				LastHealthyTime: time.Now(),
				BootUUID:        "boot-" + index,
			},
		}
		role := common.RoleStandby
		var followConfig *cluster.FollowConfig
		if i == 0 {
			role = common.RoleMaster
		} else {
			followConfig = &cluster.FollowConfig{Type: cluster.FollowTypeInternal, DBUID: "db-0"}
			cd.DBs["db-0"].Spec.Followers = append(cd.DBs["db-0"].Spec.Followers, dbUID)
		}
		cd.DBs[dbUID] = &cluster.DB{
			UID:        dbUID,
			Generation: cluster.InitialGeneration,
			Spec: &cluster.DBSpec{
				KeeperUID:    keeperUID,
				InitMode:     cluster.DBInitModeNone,
				Role:         role,
				FollowConfig: followConfig,
				Followers:    []string{},
			},
			Status: cluster.DBStatus{
				Healthy:           true,
				CurrentGeneration: cluster.InitialGeneration,
				ListenAddress:     "127.0.0.1",
				Port:              "5432",
				SystemID:          "system-0",
				TimelineID:        1,
				XLogPos:           0x4000000 - uint64(i*4096),
			},
		}
	}
	return cd
}

func benchmarkStoreKeeperInfo(uid string) *cluster.KeeperInfo {
	return &cluster.KeeperInfo{
		InfoUID:    "info-" + uid,
		UID:        uid,
		ClusterUID: "cluster-0",
		BootUUID:   "boot-" + uid,
		PostgresState: &cluster.PostgresState{
			UID:           "db-" + uid,
			Generation:    cluster.InitialGeneration,
			ListenAddress: "127.0.0.1",
			Port:          "5432",
			Healthy:       true,
			SystemID:      "system-0",
			TimelineID:    1,
			XLogPos:       0x4000000,
			PGParameters:  common.Parameters{"max_connections": "100"},
			OlderWalFile:  "000000010000000000000001",
			TimelinesHistory: cluster.PostgresTimelinesHistory{
				{TimelineID: 1, SwitchPoint: 0x3000000, Reason: "benchmark"},
			},
		},
	}
}

type benchmarkKVStore struct {
	values    map[string]*KVPair
	lastIndex uint64
}

func (s *benchmarkKVStore) Put(_ context.Context, key string, value []byte, _ *WriteOptions) error {
	s.lastIndex++
	s.values[key] = &KVPair{Key: key, Value: cloneBytes(value), LastIndex: s.lastIndex}
	return nil
}

func (s *benchmarkKVStore) Get(_ context.Context, key string) (*KVPair, error) {
	pair, ok := s.values[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return &KVPair{Key: pair.Key, Value: cloneBytes(pair.Value), LastIndex: pair.LastIndex}, nil
}

func (s *benchmarkKVStore) List(_ context.Context, directory string) ([]*KVPair, error) {
	var pairs []*KVPair
	for key, pair := range s.values {
		if !strings.HasPrefix(key, directory) {
			continue
		}
		pairs = append(pairs, &KVPair{Key: pair.Key, Value: cloneBytes(pair.Value), LastIndex: pair.LastIndex})
	}
	if len(pairs) == 0 {
		return nil, ErrKeyNotFound
	}
	return pairs, nil
}

func (s *benchmarkKVStore) AtomicPut(_ context.Context, key string, value []byte, _ *KVPair, _ *WriteOptions) (*KVPair, error) {
	s.lastIndex++
	pair := &KVPair{Key: key, Value: cloneBytes(value), LastIndex: s.lastIndex}
	s.values[key] = pair
	return pair, nil
}

func (s *benchmarkKVStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func (s *benchmarkKVStore) Close() error {
	return nil
}

func cloneBytes(in []byte) []byte {
	return append([]byte(nil), in...)
}
