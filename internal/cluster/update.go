// Copyright 2016 Sorint.lab
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

import (
	"errors"
	"fmt"
	"time"
)

// UpdateSpec validates and replaces the cluster spec.
func (c *Cluster) UpdateSpec(nextSpec *ClusterSpec) error {
	if err := nextSpec.Validate(); err != nil {
		return fmt.Errorf("invalid cluster spec: %v", err)
	}

	currentDefaults := c.Spec.WithDefaults()
	nextDefaults := nextSpec.WithDefaults()

	if *currentDefaults.InitMode != *nextDefaults.InitMode {
		return errors.New("cannot change cluster init mode")
	}
	if *currentDefaults.Role == ClusterRoleMaster && *nextDefaults.Role == ClusterRoleStandby {
		return errors.New("cannot update a cluster from master role to standby role")
	}

	c.Spec = nextSpec
	return nil
}

// NewCluster creates a new cluster with the initial generation.
func NewCluster(uid string, spec *ClusterSpec) *Cluster {
	return &Cluster{
		UID:        uid,
		Generation: InitialGeneration,
		ChangeTime: time.Now(),
		Spec:       spec,
		Status: ClusterStatus{
			Phase: ClusterPhaseInitializing,
		},
	}
}
