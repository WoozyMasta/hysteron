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

	"github.com/sorintlab/stolon/cmd"
)

// LsCommand lists clusters in the configured store.
type LsCommand struct{}

// Execute runs `stolonctl ls`.
func (c *LsCommand) Execute(_ []string) error {
	return runStolonCtlWithoutCluster(c.run)
}

func (c *LsCommand) run() error {
	clusterNames, err := cmd.ListClusters(context.TODO(), &cfg.CommonConfig)
	if err != nil {
		return err
	}
	for _, clusterName := range clusterNames {
		stdout("%s", clusterName)
	}
	return nil
}
