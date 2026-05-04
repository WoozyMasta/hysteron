// Copyright 2017 Sorint.lab
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

// RemoveKeeperCommand removes a keeper from cluster data.
type RemoveKeeperCommand struct{}

// Execute runs `stolonctl removekeeper KEEPER_UID`.
func (c *RemoveKeeperCommand) Execute(args []string) error {
	return runStolonCtl(func() error { return c.run(args) })
}

func (c *RemoveKeeperCommand) run(args []string) error {
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
	keeperDb := newCd.FindDB(keeperInfo)
	if keeperDb != nil && newCd.Cluster.Status.Master == keeperDb.UID {
		return errors.New("keeper assigned db is the current cluster master db")
	}

	delete(newCd.Keepers, keeperID)
	if keeperDb != nil {
		delete(newCd.DBs, keeperDb.UID)
	}
	// NOTE: if the removed db is listed inside another db.Followers it'll
	// be cleaned up by the sentinels.

	if _, err := s.AtomicPutClusterData(context.TODO(), newCd, pair); err != nil {
		return fmt.Errorf("cannot update cluster data: %v", err)
	}
	return nil
}
