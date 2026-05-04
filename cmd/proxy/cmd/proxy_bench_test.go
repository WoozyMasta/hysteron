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
	"context"
	"testing"
	"time"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	slog "github.com/sorintlab/stolon/internal/log"
	"github.com/sorintlab/stolon/internal/store"

	"go.uber.org/zap"
)

func BenchmarkProxyCheckEnabledMaster(b *testing.B) {
	c := benchmarkProxyChecker(benchmarkProxyClusterData(true))
	defer c.stopTCPProxy()
	if err := c.Check(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Check(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProxyCheckDisabledProxy(b *testing.B) {
	c := benchmarkProxyChecker(benchmarkProxyClusterData(false))
	defer c.stopTCPProxy()
	if err := c.Check(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Check(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProxySetProxyInfo(b *testing.B) {
	c := benchmarkProxyChecker(benchmarkProxyClusterData(true))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.SetProxyInfo(cluster.InitialGeneration, cluster.DefaultProxyTimeout); err != nil {
			b.Fatal(err)
		}
	}
}

func init() {
	slog.SetLevel(zap.ErrorLevel)
}

func benchmarkProxyChecker(cd *cluster.ClusterData) *ClusterChecker {
	cfg.listenAddress = "127.0.0.1"
	cfg.port = "0"
	cfg.keepAliveIdle = 0
	cfg.keepAliveCount = 0
	cfg.keepAliveInterval = 0

	return &ClusterChecker{
		uid:                "proxy-0",
		listenAddress:      cfg.listenAddress,
		port:               cfg.port,
		stopListening:      true,
		e:                  &benchmarkProxyStore{cd: cd},
		endTCPProxyCh:      make(chan error, 1),
		proxyCheckInterval: cluster.DefaultProxyCheckInterval,
		proxyTimeout:       cluster.DefaultProxyTimeout,
	}
}

func benchmarkProxyClusterData(enabled bool) *cluster.ClusterData {
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
			MasterDBUID: "db-0",
		},
	}
	if enabled {
		cd.Proxy.Spec.EnabledProxies = []string{"proxy-0"}
	}
	cd.DBs["db-0"] = &cluster.DB{
		UID:        "db-0",
		Generation: cluster.InitialGeneration,
		Spec: &cluster.DBSpec{
			KeeperUID: "keeper-0",
			InitMode:  cluster.DBInitModeNone,
			Role:      common.RoleMaster,
			Followers: []string{},
		},
		Status: cluster.DBStatus{
			Healthy:           true,
			CurrentGeneration: cluster.InitialGeneration,
			ListenAddress:     "127.0.0.1",
			Port:              "5432",
			SystemID:          "system-0",
			TimelineID:        1,
			XLogPos:           0x4000000,
		},
	}
	return cd
}

type benchmarkProxyStore struct {
	cd        *cluster.ClusterData
	proxyInfo *cluster.ProxyInfo
}

func (s *benchmarkProxyStore) AtomicPutClusterData(context.Context, *cluster.ClusterData, *store.KVPair) (*store.KVPair, error) {
	return nil, nil
}

func (s *benchmarkProxyStore) PutClusterData(context.Context, *cluster.ClusterData) error {
	return nil
}

func (s *benchmarkProxyStore) GetClusterData(context.Context) (*cluster.ClusterData, *store.KVPair, error) {
	return s.cd, nil, nil
}

func (s *benchmarkProxyStore) SetKeeperInfo(context.Context, string, *cluster.KeeperInfo, time.Duration) error {
	return nil
}

func (s *benchmarkProxyStore) GetKeepersInfo(context.Context) (cluster.KeepersInfo, error) {
	return nil, nil
}

func (s *benchmarkProxyStore) SetSentinelInfo(context.Context, *cluster.SentinelInfo, time.Duration) error {
	return nil
}

func (s *benchmarkProxyStore) GetSentinelsInfo(context.Context) (cluster.SentinelsInfo, error) {
	return nil, nil
}

func (s *benchmarkProxyStore) SetProxyInfo(_ context.Context, pi *cluster.ProxyInfo, _ time.Duration) error {
	s.proxyInfo = pi
	return nil
}

func (s *benchmarkProxyStore) GetProxiesInfo(context.Context) (cluster.ProxiesInfo, error) {
	return nil, nil
}
