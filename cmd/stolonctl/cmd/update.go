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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/configfile"
	"github.com/sorintlab/stolon/internal/store"

	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// UpdateCommand replaces or patches the current cluster spec.
type UpdateCommand struct {
	File  string `short:"f" long:"file" description:"file containing a complete cluster specification or a patch to apply to the current cluster specification"`
	Patch bool   `short:"p" long:"patch" description:"patch the current cluster specification instead of replacing it"`
}

// Execute runs `stolonctl update`.
func (c *UpdateCommand) Execute(args []string) error {
	return runStolonCtl(func() error { return c.run(args) })
}

func (c *UpdateCommand) run(args []string) error {
	if len(args) > 1 {
		return errors.New("too many arguments")
	}
	if c.File == "" && len(args) < 1 {
		return errors.New("no cluster spec provided as argument and no file provided (--file/-f option)")
	}
	if c.File != "" && len(args) == 1 {
		return errors.New("only one of cluster spec provided as argument or file must be provided (--file/-f option)")
	}

	var data []byte
	if len(args) == 1 {
		data = []byte(args[0])
	} else {
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

	e, err := newStore()
	if err != nil {
		return err
	}

	for range maxRetries {
		cd, pair, err := getClusterData(e)
		if err != nil {
			return err
		}
		if cd.Cluster == nil || cd.Cluster.Spec == nil {
			return errors.New("no cluster spec available")
		}

		var newcs *cluster.ClusterSpec
		if c.Patch {
			newcs, err = patchClusterSpec(cd.Cluster.Spec, data)
			if err != nil {
				return fmt.Errorf("failed to patch cluster spec: %v", err)
			}
		} else {
			newcs, err = configfile.ClusterSpec(data)
			if err != nil {
				return fmt.Errorf("failed to unmarshal cluster spec: %v", err)
			}
		}
		if err = cd.Cluster.UpdateSpec(newcs); err != nil {
			return fmt.Errorf("cannot update cluster spec: %v", err)
		}

		// retry if cd has been modified between reading and writing
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

func patchClusterSpec(cs *cluster.ClusterSpec, p []byte) (*cluster.ClusterSpec, error) {
	csj, err := json.Marshal(cs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster spec: %v", err)
	}
	newcsj, err := strategicpatch.StrategicMergePatch(csj, p, &cluster.ClusterSpec{})
	if err != nil {
		return nil, fmt.Errorf("failed to merge patch cluster spec: %v", err)
	}
	var newcs *cluster.ClusterSpec
	if err := json.Unmarshal(newcsj, &newcs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal patched cluster spec: %v", err)
	}
	return newcs, nil
}
