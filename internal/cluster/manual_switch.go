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

package cluster

// ManualSwitchMode identifies operator-requested master switch semantics.
type ManualSwitchMode string

const (
	// ManualSwitchModeSwitchover is a planned role switch to a target keeper.
	ManualSwitchModeSwitchover ManualSwitchMode = "switchover"
	// ManualSwitchModeFailover is a forced role switch to a target keeper.
	ManualSwitchModeFailover ManualSwitchMode = "failover"
)

// ManualSwitchRequest is an operator-requested master switch intent.
type ManualSwitchRequest struct {
	TargetKeeperUID string           `json:"targetKeeperUID,omitempty"`
	Mode            ManualSwitchMode `json:"mode,omitempty"`
}
