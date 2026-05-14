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

package proxy

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/utils/readonly"
)

func TestValidateProxyListeners(t *testing.T) {
	tests := []struct {
		name         string
		cfg          proxyConfig
		wantWritable bool
		wantReadOnly bool
		wantErr      bool
	}{
		{
			name: "writable only",
			cfg: proxyConfig{
				Writable: writableOptions{
					ListenAddress: "127.0.0.1",
					Port:          "5432",
				},
			},
			wantWritable: true,
		},
		{
			name: "read only only",
			cfg: proxyConfig{
				Writable: writableOptions{
					ListenAddress:   "127.0.0.1",
					Port:            "5432",
					DisableListener: true,
				},
				ReadOnly: readOnlyOptions{
					Port: "5433",
				},
			},
			wantReadOnly: true,
		},
		{
			name: "both listeners",
			cfg: proxyConfig{
				Writable: writableOptions{
					ListenAddress: "127.0.0.1",
					Port:          "5432",
				},
				ReadOnly: readOnlyOptions{
					Port: "5433",
				},
			},
			wantWritable: true,
			wantReadOnly: true,
		},
		{
			name: "same listener address",
			cfg: proxyConfig{
				Writable: writableOptions{
					ListenAddress: "127.0.0.1",
					Port:          "5432",
				},
				ReadOnly: readOnlyOptions{
					Port: "5432",
				},
			},
			wantErr: true,
		},
		{
			name: "no listener",
			cfg: proxyConfig{
				Writable: writableOptions{
					DisableListener: true,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWritable, gotReadOnly, err := validateProxyListeners(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotWritable != tt.wantWritable || gotReadOnly != tt.wantReadOnly {
				t.Fatalf(
					"got writable=%t readOnly=%t, want writable=%t readOnly=%t",
					gotWritable,
					gotReadOnly,
					tt.wantWritable,
					tt.wantReadOnly,
				)
			}
		})
	}
}

func TestReadOnlyDestinationsReplicaPriority(t *testing.T) {
	tests := []struct {
		name     string
		priority readonly.ReplicaPriority
		want     []string
	}{
		{
			name:     "sync first",
			priority: readonly.ReplicaPrioritySync,
			want:     []string{"127.0.0.11:5432"},
		},
		{
			name:     "async first",
			priority: readonly.ReplicaPriorityAsync,
			want:     []string{"127.0.0.12:5432"},
		},
		{
			name:     "any",
			priority: readonly.ReplicaPriorityAny,
			want:     []string{"127.0.0.11:5432", "127.0.0.12:5432"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := testReadOnlyClusterData()
			c := testReadOnlyClusterChecker(readOnlyOptions{
				MaxLag:          0,
				ReplicaPriority: tt.priority,
			})
			got := tcpAddrStrings(c.readOnlyDestinations(cd, cd.DBs["db-primary"]))
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadOnlyDestinationsLagAndFallback(t *testing.T) {
	cd := testReadOnlyClusterData()
	cd.DBs["db-sync"].Status.XLogPos--
	cd.DBs["db-async"].Status.XLogPos--

	c := testReadOnlyClusterChecker(readOnlyOptions{
		MaxLag:          0,
		ReplicaPriority: readonly.ReplicaPrioritySync,
	})
	got := tcpAddrStrings(c.readOnlyDestinations(cd, cd.DBs["db-primary"]))
	want := []string{"127.0.0.10:5432"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want fallback %v", got, want)
	}

	c.readOnlyOptions.NoFallback = true
	got = tcpAddrStrings(c.readOnlyDestinations(cd, cd.DBs["db-primary"]))
	if len(got) != 0 {
		t.Fatalf("got %v, want no read-only destinations", got)
	}
}

func TestReadOnlyDestinationsIncludePrimary(t *testing.T) {
	cd := testReadOnlyClusterData()
	c := testReadOnlyClusterChecker(readOnlyOptions{
		MaxLag:          0,
		IncludePrimary:  true,
		ReplicaPriority: readonly.ReplicaPrioritySync,
	})
	got := tcpAddrStrings(c.readOnlyDestinations(cd, cd.DBs["db-primary"]))
	want := []string{"127.0.0.11:5432", "127.0.0.10:5432"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReadOnlyOnlyCheckDoesNotRegisterWritableProxy(t *testing.T) {
	cd := testReadOnlyClusterData()
	store := &benchmarkProxyStore{cd: cd}
	c := &ClusterChecker{
		uid:                "proxy-0",
		readOnly:           testReadOnlyProxyListener(),
		readOnlyOptions:    readOnlyOptions{ReplicaPriority: readonly.ReplicaPrioritySync},
		stopListening:      true,
		e:                  store,
		proxyCheckInterval: cluster.DefaultProxyCheckInterval,
		proxyTimeout:       cluster.DefaultProxyTimeout,
	}
	defer c.readOnly.stop()

	if err := c.Check(); err != nil {
		t.Fatalf("Check() failed: %v", err)
	}
	if store.proxyInfo != nil {
		t.Fatalf("read-only-only proxy registered writable proxy info: %+v", store.proxyInfo)
	}
}

func testReadOnlyClusterChecker(opts readOnlyOptions) *ClusterChecker {
	return &ClusterChecker{
		readOnly: &proxyListener{
			mode: proxyModeReadOnly,
		},
		readOnlyOptions: opts,
	}
}

func testReadOnlyProxyListener() *proxyListener {
	return &proxyListener{
		mode:          proxyModeReadOnly,
		listenAddress: "127.0.0.1",
		port:          "0",
		endTCPProxyCh: make(chan error, 1),
	}
}

func testReadOnlyClusterData() *cluster.ClusterData {
	cd := cluster.NewClusterData(&cluster.Cluster{
		UID:        "cluster-0",
		Generation: cluster.InitialGeneration,
		Spec: &cluster.ClusterSpec{
			InitMode: cluster.ClusterInitModeP(cluster.ClusterInitModeNew),
			Role:     cluster.ClusterRoleP(cluster.ClusterRoleMaster),
		},
		Status: cluster.ClusterStatus{
			Phase:  cluster.ClusterPhaseNormal,
			Master: "db-primary",
		},
	})
	cd.Proxy = &cluster.Proxy{
		UID:        "proxy",
		Generation: cluster.InitialGeneration,
		Spec: cluster.ProxySpec{
			MasterDBUID:    "db-primary",
			EnabledProxies: []string{"proxy-0"},
		},
	}
	cd.DBs["db-primary"] = testReadOnlyDB(
		"db-primary",
		common.RoleMaster,
		"127.0.0.10",
		0x4000000,
		[]string{"db-sync"},
	)
	cd.DBs["db-sync"] = testReadOnlyDB(
		"db-sync",
		common.RoleStandby,
		"127.0.0.11",
		0x4000000,
		nil,
	)
	cd.DBs["db-async"] = testReadOnlyDB(
		"db-async",
		common.RoleStandby,
		"127.0.0.12",
		0x4000000,
		nil,
	)
	return cd
}

func testReadOnlyDB(uid string, role common.Role, listenAddress string, xlogPos uint64, synchronousStandbys []string) *cluster.DB {
	return &cluster.DB{
		UID:        uid,
		Generation: cluster.InitialGeneration,
		ChangeTime: time.Time{},
		Spec: &cluster.DBSpec{
			KeeperUID: uid + "-keeper",
			InitMode:  cluster.DBInitModeNone,
			Role:      role,
			Followers: []string{},
		},
		Status: cluster.DBStatus{
			Healthy:                true,
			CurrentGeneration:      cluster.InitialGeneration,
			ListenAddress:          listenAddress,
			Port:                   "5432",
			SystemID:               "system-0",
			TimelineID:             1,
			XLogPos:                xlogPos,
			SynchronousStandbys:    synchronousStandbys,
			CurSynchronousStandbys: synchronousStandbys,
		},
	}
}

func tcpAddrStrings(addrs []*net.TCPAddr) []string {
	strings := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		strings = append(strings, addr.String())
	}
	return strings
}
