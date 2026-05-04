// Copyright 2018 Sorint.lab
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

package common_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sorintlab/stolon/internal/common"
	"github.com/sorintlab/stolon/internal/util"
)

func TestDiffReturnsChangedParams(t *testing.T) {
	var curParams common.Parameters = map[string]string{
		"max_connections": "100",
		"shared_buffers":  "10MB",
		"huge":            "off",
	}

	var newParams common.Parameters = map[string]string{
		"max_connections": "200",
		"shared_buffers":  "10MB",
		"work_mem":        "4MB",
	}

	expectedDiff := []string{"max_connections", "huge", "work_mem"}

	diff := curParams.Diff(newParams)

	if !util.CompareStringSliceNoOrder(expectedDiff, diff) {
		t.Errorf("Expected diff is %v, but got %v", expectedDiff, diff)
	}
}

func TestWriteFileAtomicWithAbsolutePath(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "postgresql.conf")

	if err := common.WriteFileAtomic(filename, 0600, []byte("port = '5432'\n")); err != nil {
		t.Fatalf("WriteFileAtomic failed: %v", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != "port = '5432'\n" {
		t.Fatalf("unexpected data %q", string(data))
	}
}
