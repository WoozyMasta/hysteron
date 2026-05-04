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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sorintlab/stolon/internal/configfile"
	"github.com/sorintlab/stolon/internal/store"
)

// ClusterDataCommand groups read/write subcommands for the cluster data
// stored in the backend.
type ClusterDataCommand struct {
	Write ClusterDataWriteCommand `command:"write" description:"Write cluster data"`
	Read  ClusterDataReadCommand  `command:"read" description:"Retrieve the current cluster data"`
}

// ClusterDataReadCommand prints the current cluster data as JSON.
type ClusterDataReadCommand struct {
	Pretty bool `long:"pretty" description:"pretty print"`
}

// Execute runs `stolonctl clusterdata read`.
func (c *ClusterDataReadCommand) Execute(_ []string) error {
	return runStolonCtl(func() error { return c.run() })
}

func (c *ClusterDataReadCommand) run() error {
	e, err := newStore()
	if err != nil {
		return err
	}
	cd, _, err := getClusterData(e)
	if err != nil {
		return err
	}
	if cd.Cluster == nil {
		return errors.New("no cluster clusterdata available")
	}
	var data []byte
	if c.Pretty {
		data, err = json.MarshalIndent(cd, "", "\t")
	} else {
		data, err = json.Marshal(cd)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal clusterdata: %v", err)
	}
	stdout("%s", data)
	return nil
}

// ClusterDataWriteCommand uploads a cluster data document to the store.
type ClusterDataWriteCommand struct {
	File     string `short:"f" long:"file" description:"file containing the new cluster data"`
	ForceYes bool   `short:"y" long:"yes" description:"don't ask for confirmation"`
}

// Execute runs `stolonctl clusterdata write`.
func (c *ClusterDataWriteCommand) Execute(_ []string) error {
	return runStolonCtl(func() error { return c.run() })
}

func (c *ClusterDataWriteCommand) run() error {
	var reader io.Reader
	if c.File == "" || c.File == "-" {
		reader = os.Stdin
	} else {
		f, err := os.Open(c.File)
		if err != nil {
			return fmt.Errorf("cannot read file: %v", err)
		}
		defer func() {
			_ = f.Close()
		}()
		reader = f
	}

	s, err := newStore()
	if err != nil {
		return fmt.Errorf("failed to create new store %v", err)
	}
	return c.writeFrom(reader, s)
}

func (c *ClusterDataWriteCommand) writeFrom(reader io.Reader, s store.Store) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("error while reading data: %v", err)
	}
	cd, err := configfile.ClusterData(data)
	if err != nil {
		return fmt.Errorf("invalid cluster data: %v", err)
	}
	if err := c.isSafeToWrite(s); err != nil {
		return err
	}
	if err := s.PutClusterData(context.TODO(), cd); err != nil {
		return fmt.Errorf("failed to write cluster data into new store %v", err)
	}
	stdout("successfully wrote cluster data into the new store")
	return nil
}

func (c *ClusterDataWriteCommand) isSafeToWrite(s store.Store) error {
	cd, _, err := s.GetClusterData(context.TODO())
	if err != nil {
		return err
	}
	if cd != nil {
		if !c.ForceYes {
			return errors.New("WARNING: cluster data already available use --yes to override")
		}
		stdout("WARNING: The current cluster data will be removed")
	}
	return nil
}
