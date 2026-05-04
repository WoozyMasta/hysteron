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
	"io"
	"os"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	"github.com/sorintlab/stolon/internal/configfile"
)

// InitCommand initializes a new cluster.
type InitCommand struct {
	File     string `short:"f" long:"file" description:"file containing the new cluster spec"`
	ForceYes bool   `short:"y" long:"yes" description:"don't ask for confirmation"`
}

// Execute runs `stolonctl init`.
func (c *InitCommand) Execute(args []string) error {
	return runStolonCtl(func() error {
		return c.run(args)
	})
}

func (c *InitCommand) run(args []string) error {
	if len(args) > 1 {
		return errors.New("too many arguments")
	}

	dataSupplied := false
	var data []byte
	switch len(args) {
	case 1:
		dataSupplied = true
		data = []byte(args[0])
	case 0:
		if c.File != "" {
			dataSupplied = true
			var err error
			if c.File == "-" {
				data, err = io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("cannot read from stdin: %v", err)
				}
			} else {
				data, err = os.ReadFile(c.File)
				if err != nil {
					return fmt.Errorf("cannot read file: %v", err)
				}
			}
		}
	}

	e, err := newStore()
	if err != nil {
		return err
	}

	cd, _, err := e.GetClusterData(context.TODO())
	if err != nil {
		return fmt.Errorf("cannot get cluster data: %v", err)
	}
	if cd != nil {
		stdout("WARNING: The current cluster data will be removed")
	}
	stdout("WARNING: The databases managed by the keepers will be overwritten depending on the provided cluster spec.")

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

	var cs *cluster.ClusterSpec
	if dataSupplied {
		cs, err = configfile.ClusterSpec(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal cluster spec: %v", err)
		}
	} else {
		// Define a new cluster spec with initMode "new"
		cs = &cluster.ClusterSpec{}
		cs.InitMode = cluster.ClusterInitModeP(cluster.ClusterInitModeNew)
	}

	if err := cs.Validate(); err != nil {
		return fmt.Errorf("invalid cluster spec: %v", err)
	}

	c2 := cluster.NewCluster(common.UID(), cs)
	cd = cluster.NewClusterData(c2)

	if err := e.PutClusterData(context.TODO(), cd); err != nil {
		return fmt.Errorf("cannot update cluster data: %v", err)
	}
	return nil
}
