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
	"reflect"
	"testing"
)

func TestCommonConfigClusterNamesList(t *testing.T) {
	cfg := &CommonConfig{
		ClusterNames: []string{"one,two", " three ", "", "four"},
	}

	got := cfg.ClusterNamesList()
	want := []string{"one", "two", "three", "four"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ClusterNamesList() = %v, want %v", got, want)
	}
}

func TestCheckClusterNameRequiresExactlyOneCluster(t *testing.T) {
	tests := []struct {
		name string
		cfg  CommonConfig
		err  bool
	}{
		{
			name: "one cluster",
			cfg:  CommonConfig{ClusterNames: []string{"one"}},
		},
		{
			name: "no clusters",
			cfg:  CommonConfig{},
			err:  true,
		},
		{
			name: "multiple clusters",
			cfg:  CommonConfig{ClusterNames: []string{"one", "two"}},
			err:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckClusterName(&tt.cfg)
			if tt.err && err == nil {
				t.Fatal("expected error")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCheckClusterNamesRejectsDuplicates(t *testing.T) {
	cfg := &CommonConfig{ClusterNames: []string{"one", "two", "one"}}
	if _, err := CheckClusterNames(cfg); err == nil {
		t.Fatal("expected duplicate cluster name error")
	}
}
