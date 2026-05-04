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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/store"
)

// StatusCommand prints the current cluster status.
type StatusCommand struct {
	Format string `short:"f" long:"format" choices:"text;json" default:"text" description:"output format"`
}

// Execute runs `stolonctl status`.
func (c *StatusCommand) Execute(_ []string) error {
	return runStolonCtl(func() error { return c.run() })
}

// Status is the stolonctl status output model.
type Status struct {
	Cluster   ClusterStatus    `json:"cluster"`
	Sentinels []SentinelStatus `json:"sentinels"`
	Proxies   []ProxyStatus    `json:"proxies"`
	Keepers   []KeeperStatus   `json:"keepers"`
}

// SentinelStatus is the status output for one sentinel.
type SentinelStatus struct {
	UID    string `json:"uid"`
	Leader bool   `json:"leader"`
}

// ProxyStatus is the status output for one proxy.
type ProxyStatus struct {
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

// KeeperStatus is the status output for one keeper.
type KeeperStatus struct {
	UID                 string `json:"uid"`
	ListenAddress       string `json:"listen_address"`
	Healthy             bool   `json:"healthy"`
	PgHealthy           bool   `json:"pg_healthy"`
	PgWantedGeneration  int64  `json:"pg_wanted_generation"`
	PgCurrentGeneration int64  `json:"pg_current_generation"`
}

// ClusterStatus is the status output for the cluster summary.
type ClusterStatus struct {
	MasterKeeperUID string `json:"master_keeper_uid"`
	MasterDBUID     string `json:"master_db_uid"`
	Available       bool   `json:"available"`
}

func (c *StatusCommand) run() error {
	status, generateErr := generateStatus()
	switch c.Format {
	case "json":
		return renderJSON(status, generateErr)
	case "text", "":
		return renderText(status, generateErr)
	default:
		return fmt.Errorf("unrecognised output format %s", c.Format)
	}
}

func renderJSON(status Status, generateErr error) error {
	if generateErr != nil {
		return marshalJSON(generateErr)
	}
	return marshalJSON(status)
}

func marshalJSON(value any) error {
	output, err := json.MarshalIndent(value, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}
	stdout("%s", output)
	return nil
}

func renderText(status Status, generateErr error) error {
	if generateErr != nil {
		return generateErr
	}

	tabOut := new(tabwriter.Writer)
	tabOut.Init(os.Stdout, 0, 8, 1, '\t', 0)

	stdout("=== Active sentinels ===")
	stdout("")
	if len(status.Sentinels) == 0 {
		stdout("No active sentinels")
	} else {
		writeOutput(tabOut, "ID\tLEADER\n")
		for _, s := range status.Sentinels {
			writeOutput(tabOut, "%s\t%t\n", s.UID, s.Leader)
			if err := tabOut.Flush(); err != nil {
				return fmt.Errorf("flush status output: %v", err)
			}
		}
	}

	stdout("")
	stdout("=== Active proxies ===")
	stdout("")
	if len(status.Proxies) == 0 {
		stdout("No active proxies")
	} else {
		writeOutput(tabOut, "ID\n")
		for _, p := range status.Proxies {
			writeOutput(tabOut, "%s\n", p.UID)
			if err := tabOut.Flush(); err != nil {
				return fmt.Errorf("flush status output: %v", err)
			}
		}
	}

	stdout("")
	stdout("=== Keepers ===")
	stdout("")
	if len(status.Keepers) == 0 {
		stdout("No keepers available")
		stdout("")
	} else {
		writeOutput(tabOut, "UID\tHEALTHY\tPG LISTENADDRESS\tPG HEALTHY\tPG WANTEDGENERATION\tPG CURRENTGENERATION\n")
		for _, k := range status.Keepers {
			writeOutput(tabOut, "%s\t%t\t%s\t%t\t%d\t%d\t\n", k.UID, k.Healthy, k.ListenAddress, k.PgHealthy, k.PgWantedGeneration, k.PgCurrentGeneration)
			if err := tabOut.Flush(); err != nil {
				return fmt.Errorf("flush status output: %v", err)
			}
		}
	}

	if status.Cluster.MasterKeeperUID == "" {
		stdout("No cluster available")
	} else {
		stdout("")
		stdout("=== Cluster Info ===")
		stdout("")
		stdout("Master Keeper: %s", status.Cluster.MasterKeeperUID)
	}

	// This tree data isn't currently available in the Status struct
	e, err := newStore()
	if err != nil {
		return err
	}
	cd, _, err := getClusterData(e)
	if err != nil {
		return err
	}
	if status.Cluster.MasterDBUID != "" {
		stdout("")
		stdout("===== Keepers/DB tree =====")
		stdout("")
		printTree(status.Cluster.MasterDBUID, cd, 0, "", true)
	}
	stdout("")
	return nil
}

func printTree(dbuid string, cd *cluster.ClusterData, level int, prefix string, tail bool) {
	if _, ok := cd.DBs[dbuid]; !ok {
		return
	}
	out := prefix
	if level > 0 {
		if tail {
			out += "└─"
		} else {
			out += "├─"
		}
	}
	out += cd.DBs[dbuid].Spec.KeeperUID
	if dbuid == cd.Cluster.Status.Master {
		out += " (master)"
	}
	stdout("%s", out)
	db := cd.DBs[dbuid]
	followers := db.Spec.Followers
	cnt := len(followers)
	for i, f := range followers {
		emptyspace := ""
		if level > 0 {
			emptyspace = "  "
		}
		linespace := "│ "
		if i < cnt-1 {
			if tail {
				printTree(f, cd, level+1, prefix+emptyspace, false)
			} else {
				printTree(f, cd, level+1, prefix+linespace, false)
			}
		} else {
			if tail {
				printTree(f, cd, level+1, prefix+emptyspace, true)
			} else {
				printTree(f, cd, level+1, prefix+linespace, true)
			}
		}
	}
}

func generateStatus() (Status, error) {
	status := Status{}

	e, err := newStore()
	if err != nil {
		return status, err
	}

	election, err := newElection("")
	if err != nil {
		return status, err
	}

	lsid, err := election.Leader()
	if err != nil && err != store.ErrElectionNoLeader {
		return status, err
	}

	sentinelsInfo, err := e.GetSentinelsInfo(context.TODO())
	if err != nil {
		return status, err
	}

	sentinels := make([]SentinelStatus, 0)
	sort.Sort(sentinelsInfo)
	for _, si := range sentinelsInfo {
		leader := lsid != "" && si.UID == lsid
		sentinels = append(sentinels, SentinelStatus{UID: si.UID, Leader: leader})
	}
	status.Sentinels = sentinels

	proxiesInfo, err := e.GetProxiesInfo(context.TODO())
	if err != nil {
		return status, err
	}
	proxiesInfoSlice := proxiesInfo.ToSlice()

	proxies := make([]ProxyStatus, 0)
	sort.Sort(proxiesInfoSlice)
	for _, pi := range proxiesInfoSlice {
		proxies = append(proxies, ProxyStatus{UID: pi.UID, Generation: pi.Generation})
	}
	status.Proxies = proxies

	cd, _, err := getClusterData(e)
	if err != nil {
		return status, err
	}

	keepers := make([]KeeperStatus, 0)
	for _, kuid := range cd.Keepers.SortedKeys() {
		k := cd.Keepers[kuid]
		db := cd.FindDB(k)
		dbListenAddress := "(no db assigned)"
		var (
			pgHealthy           bool
			pgCurrentGeneration int64
			pgWantedGeneration  int64
		)
		if db != nil {
			pgHealthy = db.Status.Healthy
			pgCurrentGeneration = db.Status.CurrentGeneration
			pgWantedGeneration = db.Generation

			dbListenAddress = "(unknown)"
			if db.Status.ListenAddress != "" {
				dbListenAddress = fmt.Sprintf("%s:%s", db.Status.ListenAddress, db.Status.Port)
			}
		}
		keepers = append(keepers, KeeperStatus{
			UID:                 kuid,
			ListenAddress:       dbListenAddress,
			Healthy:             k.Status.Healthy,
			PgHealthy:           pgHealthy,
			PgWantedGeneration:  pgWantedGeneration,
			PgCurrentGeneration: pgCurrentGeneration,
		})
	}
	status.Keepers = keepers

	clusterStatus := ClusterStatus{}
	if cd.Cluster == nil || cd.DBs == nil {
		clusterStatus.Available = false
	} else {
		master := cd.Cluster.Status.Master
		clusterStatus.Available = true
		if master != "" {
			clusterStatus.MasterDBUID = cd.DBs[master].UID
			clusterStatus.MasterKeeperUID = cd.Keepers[cd.DBs[master].Spec.KeeperUID].UID
		}
	}
	status.Cluster = clusterStatus

	return status, nil
}
