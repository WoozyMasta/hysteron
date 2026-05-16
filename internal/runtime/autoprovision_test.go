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

package runtime

import "testing"

func TestAutoProvisionKeeper_UsesStatefulSetOrdinalAndPodIP(t *testing.T) {
	opts := &KeeperOptions{}
	env := map[string]string{
		"POD_NAME": "hysteron-keeper-2",
		"POD_IP":   "10.42.0.17",
	}

	autoProvisionKeeper(opts, mapLookup(env), func() (string, error) {
		return "ignored-host", nil
	})

	if opts.UID != "keeper2" {
		t.Fatalf("uid=%q, want %q", opts.UID, "keeper2")
	}
	if opts.PG.ListenAddress != "10.42.0.17" {
		t.Fatalf("listen=%q, want %q", opts.PG.ListenAddress, "10.42.0.17")
	}
}

func TestAutoProvisionKeeper_DoesNotOverrideExplicitValues(t *testing.T) {
	opts := &KeeperOptions{
		UID: "custom_uid",
		PG: KeeperPostgresOptions{
			ListenAddress: "192.168.1.20",
		},
	}
	env := map[string]string{
		"POD_NAME": "hysteron-keeper-9",
		"POD_IP":   "10.42.0.19",
	}

	autoProvisionKeeper(opts, mapLookup(env), func() (string, error) {
		return "ignored-host", nil
	})

	if opts.UID != "custom_uid" {
		t.Fatalf("uid=%q, want unchanged explicit uid", opts.UID)
	}
	if opts.PG.ListenAddress != "192.168.1.20" {
		t.Fatalf("listen=%q, want unchanged explicit listen address", opts.PG.ListenAddress)
	}
}

func TestAutoProvisionKeeper_FallsBackToHostnameWhenNoPodName(t *testing.T) {
	opts := &KeeperOptions{}
	env := map[string]string{
		"HOSTNAME": "worker-a-01.example",
	}

	autoProvisionKeeper(opts, mapLookup(env), func() (string, error) {
		return "ignored-host", nil
	})

	if opts.UID != "keeper_worker_a_01_example" {
		t.Fatalf("uid=%q, want %q", opts.UID, "keeper_worker_a_01_example")
	}
}

func TestAutoProvisionKeeper_HostnameFuncFallback(t *testing.T) {
	opts := &KeeperOptions{}
	env := map[string]string{}

	autoProvisionKeeper(opts, mapLookup(env), func() (string, error) {
		return "node-2", nil
	})

	if opts.UID != "keeper_node_2" {
		t.Fatalf("uid=%q, want %q", opts.UID, "keeper_node_2")
	}
}

func TestKeeperUIDFromPodName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "keeper-0", want: "keeper0"},
		{in: "db-12", want: "keeper12"},
		{in: "plain", want: ""},
		{in: "bad-ordinal-x", want: ""},
	}
	for _, tt := range tests {
		got := keeperUIDFromPodName(tt.in)
		if got != tt.want {
			t.Fatalf("keeperUIDFromPodName(%q)=%q want %q", tt.in, got, tt.want)
		}
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}
