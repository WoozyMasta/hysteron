// Copyright 2015 Sorint.lab
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
	"encoding/json"
	"errors"
	"fmt"
)

// SpecCommand prints the current cluster specification.
type SpecCommand struct {
	Defaults bool `long:"defaults" description:"also show default values"`
}

// Execute runs `stolonctl spec`.
//
// The cluster spec already uses `omitempty` on every optional field, so
// we can serialize cluster.ClusterSpec directly:
//   - without --defaults: only user-set fields appear (nil pointers are
//     omitted, defaults are not materialized);
//   - with --defaults: cluster.DefSpec() returns a fully populated copy
//     where defaults are non-nil and therefore visible.
func (c *SpecCommand) Execute(_ []string) error {
	return runStolonCtl(func() error { return c.run() })
}

func (c *SpecCommand) run() error {
	e, err := newStore()
	if err != nil {
		return err
	}
	cd, _, err := getClusterData(e)
	if err != nil {
		return err
	}
	if cd.Cluster == nil || cd.Cluster.Spec == nil {
		return errors.New("no cluster spec available")
	}
	spec := cd.Cluster.Spec
	if c.Defaults {
		spec = cd.Cluster.DefSpec()
	}
	specj, err := json.MarshalIndent(spec, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal spec: %v", err)
	}
	stdout("%s", specj)
	return nil
}
