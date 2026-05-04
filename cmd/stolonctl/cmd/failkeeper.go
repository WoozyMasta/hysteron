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

package cmd

import (
	"context"
	"errors"
	"fmt"
)

// FailKeeperCommand forces a keeper into a temporarily failed state.
//
// It's a one-shot operation: the sentinel computes a new clusterdata
// considering the keeper as failed and restores its state to the real
// one. For example, if the force-failed keeper is master, the sentinel
// will try to elect a new master; if no new master can be elected, the
// force-failed keeper, if really healthy, will be re-elected.
type FailKeeperCommand struct{}

// Execute runs `stolonctl failkeeper KEEPER_UID`.
func (c *FailKeeperCommand) Execute(args []string) error {
	return runStolonCtl(func() error { return c.run(args) })
}

func (c *FailKeeperCommand) run(args []string) error {
	if len(args) > 1 {
		return errors.New("too many arguments")
	}
	if len(args) == 0 {
		return errors.New("keeper uid required")
	}
	keeperID := args[0]

	s, err := newStore()
	if err != nil {
		return err
	}

	cd, pair, err := getClusterData(s)
	if err != nil {
		return err
	}
	if cd.Cluster == nil || cd.Cluster.Spec == nil {
		return errors.New("no cluster spec available")
	}

	newCd := cd.DeepCopy()
	keeperInfo := newCd.Keepers[keeperID]
	if keeperInfo == nil {
		return errors.New("keeper doesn't exist")
	}
	keeperInfo.Status.ForceFail = true

	if _, err := s.AtomicPutClusterData(context.TODO(), newCd, pair); err != nil {
		return fmt.Errorf("cannot update cluster data: %v", err)
	}
	return nil
}
