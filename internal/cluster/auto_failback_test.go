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

package cluster

import (
	"strings"
	"testing"
	"time"
)

func TestClusterSpecWithDefaults_AutoFailback(t *testing.T) {
	spec := (&ClusterSpec{}).WithDefaults()
	if spec.UnsafeAutoFailback == nil || *spec.UnsafeAutoFailback {
		t.Fatalf("unsafeAutoFailback default must be false")
	}
	if spec.AutoFailbackMinUptime == nil || spec.AutoFailbackMinUptime.Duration != DefaultAutoFailbackMinUptime {
		t.Fatalf("unexpected autoFailbackMinUptime default: %v", spec.AutoFailbackMinUptime)
	}
	if spec.AutoFailbackCooldown == nil || spec.AutoFailbackCooldown.Duration != DefaultAutoFailbackCooldown {
		t.Fatalf("unexpected autoFailbackCooldown default: %v", spec.AutoFailbackCooldown)
	}
}

func TestClusterSpecValidate_AutoFailbackDurations(t *testing.T) {
	mode := ClusterInitModeNew
	spec := &ClusterSpec{
		InitMode:              &mode,
		AutoFailbackMinUptime: &Duration{Duration: -time.Second},
	}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "autoFailbackMinUptime") {
		t.Fatalf("expected autoFailbackMinUptime validation error, got: %v", err)
	}

	spec = &ClusterSpec{
		InitMode:              &mode,
		AutoFailbackCooldown:  &Duration{Duration: -time.Second},
		AutoFailbackMinUptime: &Duration{Duration: 0},
	}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "autoFailbackCooldown") {
		t.Fatalf("expected autoFailbackCooldown validation error, got: %v", err)
	}
}
