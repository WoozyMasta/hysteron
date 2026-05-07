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

package commands

import (
	"slices"
	"testing"
)

func TestCommandGroups(t *testing.T) {
	cfg = newRootCommand()
	parser := NewParser()

	tests := []struct {
		name  string
		group string
	}{
		{name: "keeper", group: "Runtime Commands"},
		{name: "sentinel", group: "Runtime Commands"},
		{name: "proxy", group: "Runtime Commands"},
		{name: "cluster", group: "Management Commands"},
		{name: "failover", group: "Management Commands"},
	}

	for _, tt := range tests {
		command := parser.Find(tt.name)
		if command == nil {
			t.Fatalf("command %q not found", tt.name)
		}
		if command.CommandGroup != tt.group {
			t.Fatalf(
				"command %q group mismatch: got %q, want %q",
				tt.name,
				command.CommandGroup,
				tt.group,
			)
		}
	}
}

func TestRootAndRuntimeOptionScoping(t *testing.T) {
	cfg = newRootCommand()
	parser := NewParser()

	if parser.FindOptionByLongName("log-level") == nil {
		t.Fatal("expected root to expose global --log-level option")
	}
	if parser.FindOptionByLongName("store-backend") != nil {
		t.Fatal("root must not expose management --store-backend option")
	}
	if parser.FindOptionByLongName("k8s-config") != nil {
		t.Fatal("root must not expose backend-specific --k8s-config option")
	}
	if parser.FindOptionByLongName("etcd-endpoints") != nil {
		t.Fatal("root must not expose backend-specific --etcd-endpoints option")
	}

	proxy := parser.Find("proxy")
	if proxy == nil {
		t.Fatal("proxy command not found")
	}
	if proxy.FindOptionByLongName("cluster-name") == nil {
		t.Fatal("proxy must expose --cluster-name option")
	}
	if proxy.FindOptionByLongName("metrics-listen-address") == nil {
		t.Fatal("proxy must expose --metrics-listen-address option")
	}
	if proxy.FindOptionByLongName("k8s-config") != nil {
		t.Fatal("proxy parent must not expose --k8s-config option")
	}
	if proxy.FindOptionByLongName("etcd-endpoints") != nil {
		t.Fatal("proxy parent must not expose --etcd-endpoints option")
	}
}

func TestBackendOptionScopingAndAliases(t *testing.T) {
	cfg = newRootCommand()
	parser := NewParser()

	proxy := parser.Find("proxy")
	if proxy == nil {
		t.Fatal("proxy command not found")
	}

	etcd := proxy.Find("etcd")
	if etcd == nil {
		t.Fatal("proxy etcd backend command not found")
	}
	if !slices.Contains(etcd.Aliases, "etcdv3") {
		t.Fatal("proxy etcd backend must include etcdv3 alias")
	}
	if etcd.FindOptionByLongName("etcd-endpoints") == nil {
		t.Fatal("proxy etcd backend must expose --etcd-endpoints option")
	}
	if etcd.FindOptionByLongName("k8s-config") != nil {
		t.Fatal("proxy etcd backend must not expose --k8s-config option")
	}

	k8s := proxy.Find("kubernetes")
	if k8s == nil {
		t.Fatal("proxy kubernetes backend command not found")
	}
	if !slices.Contains(k8s.Aliases, "k8s") {
		t.Fatal("proxy kubernetes backend must include k8s alias")
	}
	if k8s.FindOptionByLongName("k8s-config") == nil {
		t.Fatal("proxy kubernetes backend must expose --k8s-config option")
	}
	if k8s.FindOptionByLongName("etcd-endpoints") != nil {
		t.Fatal("proxy kubernetes backend must not expose --etcd-endpoints option")
	}
}

func TestManagementStoreBackendChoices(t *testing.T) {
	cfg = newRootCommand()
	parser := NewParser()

	cluster := parser.Find("cluster")
	if cluster == nil {
		t.Fatal("cluster command not found")
	}
	storeBackend := cluster.FindOptionByLongName("store-backend")
	if storeBackend == nil {
		t.Fatal("cluster must expose --store-backend option")
	}
	if !slices.Contains(storeBackend.Choices, "etcd") {
		t.Fatal("cluster --store-backend must include etcd alias choice")
	}
	if !slices.Contains(storeBackend.Choices, "etcdv3") {
		t.Fatal("cluster --store-backend must include etcdv3 canonical choice")
	}
	if !slices.Contains(storeBackend.Choices, "k8s") {
		t.Fatal("cluster --store-backend must include k8s alias choice")
	}
	if !slices.Contains(storeBackend.Choices, "kubernetes") {
		t.Fatal("cluster --store-backend must include kubernetes canonical choice")
	}
}
