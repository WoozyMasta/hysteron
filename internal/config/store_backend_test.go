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

package config

import "testing"

func TestNormalizeStoreBackend(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "etcd alias", in: "etcd", want: "etcdv3"},
		{name: "etcdv3 canonical", in: "etcdv3", want: "etcdv3"},
		{name: "k8s alias", in: "k8s", want: "kubernetes"},
		{name: "kubernetes canonical", in: "kubernetes", want: "kubernetes"},
		{name: "unknown preserved", in: "unknown", want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeStoreBackend(tt.in)
			if got != tt.want {
				t.Fatalf("unexpected normalized backend: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestCheckCommonConfigWithEtcdAlias(t *testing.T) {
	cfg := &CommonConfig{
		Store: StoreOptions{
			Backend: "etcd",
		},
	}
	if err := CheckCommonConfig(cfg); err != nil {
		t.Fatalf("etcd alias must pass common config check: %v", err)
	}
}
