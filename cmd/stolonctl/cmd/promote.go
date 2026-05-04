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

package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/store"
)

// PromoteCommand promotes a standby cluster to a primary cluster.
type PromoteCommand struct {
	ForceYes bool `short:"y" long:"yes" description:"don't ask for confirmation"`
}

// Execute runs `stolonctl promote`.
func (c *PromoteCommand) Execute(args []string) error {
	return runStolonCtl(func() error { return c.run(args) })
}

func (c *PromoteCommand) run(args []string) error {
	if len(args) > 0 {
		return errors.New("too many arguments")
	}

	e, err := newStore()
	if err != nil {
		return err
	}

	if !c.ForceYes {
		accepted, err := askConfirmation("Are you sure you want to continue? [yes/no] ")
		if err != nil {
			return err
		}
		if !accepted {
			stdout("exiting")
			return nil
		}
	}

	for range maxRetries {
		cd, pair, err := getClusterData(e)
		if err != nil {
			return err
		}
		if cd.Cluster == nil || cd.Cluster.Spec == nil {
			return errors.New("no cluster spec available")
		}

		ds := cd.Cluster.DefSpec()
		if *ds.Role == cluster.ClusterRoleMaster {
			stderr("cluster spec role already set to master")
			return nil
		}
		cd.Cluster.Spec.Role = cluster.ClusterRoleP(cluster.ClusterRoleMaster)

		if err = cd.Cluster.UpdateSpec(cd.Cluster.Spec); err != nil {
			return fmt.Errorf("cannot update cluster spec: %v", err)
		}

		if _, err := e.AtomicPutClusterData(context.TODO(), cd, pair); err != nil {
			if err == store.ErrKeyModified {
				continue
			}
			return fmt.Errorf("cannot update cluster data: %v", err)
		}
		return nil
	}
	return fmt.Errorf("failed to update cluster data after %d retries", maxRetries)
}
