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

package output

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/list"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/woozymasta/hysteron/internal/app"
)

// WriteStatus renders cluster status with the selected format.
func WriteStatus(w io.Writer, format Format, status app.Status) error {
	switch format {
	case FormatJSON:
		return WriteValue(w, format, status)
	case FormatYAML:
		return WriteValue(w, format, status)
	case FormatPlain:
		return writeStatusTable(w, status)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

func writeStatusTable(w io.Writer, status app.Status) error {
	clusterTable := newStatusTable(w, "Cluster")
	clusterTable.AppendHeader(table.Row{"Field", "Value"})
	clusterTable.AppendRows([]table.Row{
		{"Available", status.Cluster.Available},
		{"Phase", valueOrDash(status.Cluster.Phase)},
		{"Paused", status.Cluster.Paused},
		{"Pause Reason", valueOrDash(status.Cluster.PauseReason)},
		{"Pause Until", valueOrDash(status.Cluster.PauseUntil)},
		{"Generation", status.Cluster.Generation},
		{"Format Version", status.Cluster.FormatVersion},
		{"Master Keeper", valueOrDash(status.Cluster.MasterKeeperUID)},
		{"Master DB", valueOrDash(status.Cluster.MasterDBUID)},
		{"Keepers", fmt.Sprintf("%d/%d", status.Cluster.KeepersHealthy, status.Cluster.KeepersTotal)},
		{"DBs", fmt.Sprintf("%d/%d", status.Cluster.DBsHealthy, status.Cluster.DBsTotal)},
		{"Proxies Seen", status.Cluster.ProxiesSeen},
	})
	clusterTable.AppendFooter(table.Row{"Rows", 12})
	clusterTable.Render()

	if err := writeLine(w); err != nil {
		return err
	}
	sentinelTable := newStatusTable(w, "Sentinels")
	sentinelTable.AppendHeader(table.Row{"UID", "Leader"})
	for _, sentinel := range status.Sentinels {
		sentinelTable.AppendRow(table.Row{sentinel.UID, sentinel.Leader})
	}
	sentinelTable.AppendFooter(table.Row{"Rows", len(status.Sentinels)})
	sentinelTable.Render()

	if err := writeLine(w); err != nil {
		return err
	}
	proxyTable := newStatusTable(w, "Proxies")
	proxyTable.AppendHeader(table.Row{"UID", "Mode", "Listeners", "Generation"})
	for _, proxy := range status.Proxies {
		proxyTable.AppendRow(table.Row{
			proxy.UID,
			valueOrDash(proxy.Mode),
			valueOrDash(proxy.Listeners),
			proxy.Generation,
		})
	}
	proxyTable.AppendFooter(table.Row{"Rows", "", "", len(status.Proxies)})
	proxyTable.Render()

	if err := writeLine(w); err != nil {
		return err
	}
	keeperTable := newStatusTable(w, "Keepers")
	keeperTable.AppendHeader(table.Row{
		"UID",
		"DB UID",
		"Role",
		"PG Version",
		"Master Priority",
		"Healthy",
		"Can Be Master",
		"Can Be Sync Replica",
		"PG Listen Address",
		"PG Healthy",
		"PG Wanted Generation",
		"PG Current Generation",
	})
	for _, keeper := range status.Keepers {
		keeperTable.AppendRow(table.Row{
			keeper.UID,
			valueOrDash(keeper.DBUID),
			valueOrDash(keeper.Role),
			valueOrDash(keeper.PGVersion),
			keeper.MasterPriority,
			keeper.Healthy,
			keeper.CanBeMaster,
			keeper.CanBeSyncReplica,
			keeper.ListenAddress,
			keeper.PgHealthy,
			keeper.PgWantedGeneration,
			keeper.PgCurrentGeneration,
		})
	}
	keeperTable.AppendFooter(table.Row{
		"Rows", "", "", "", "", "", "", "", "", "", "", len(status.Keepers),
	})
	keeperTable.Render()

	if len(status.KeeperTree) > 0 {
		if err := writeLine(w); err != nil {
			return err
		}
		treeLines := renderKeeperTree(status.KeeperTree)
		treeTable := newStatusTable(w, "Keeper Tree")
		treeTable.AppendHeader(table.Row{"Line"})
		for _, line := range treeLines {
			treeTable.AppendRow(table.Row{line})
		}
		treeTable.AppendFooter(table.Row{"Rows: " + strconv.Itoa(len(treeLines))})
		treeTable.Render()
	}
	return nil
}

func renderKeeperTree(nodes []app.KeeperTreeNode) []string {
	if len(nodes) == 0 {
		return nil
	}

	w := list.NewWriter()
	w.SetStyle(list.StyleConnectedLight)
	currentLevel := 0

	for _, node := range nodes {
		for currentLevel < node.Level {
			w.Indent()
			currentLevel++
		}
		for currentLevel > node.Level {
			w.UnIndent()
			currentLevel--
		}
		w.AppendItem(node.Label)
	}
	w.UnIndentAll()

	lines := strings.Split(w.Render(), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func newStatusTable(w io.Writer, title string) table.Writer {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetTitle(title)
	t.SetStyle(table.StyleRounded)
	return t
}

func writeLine(w io.Writer) error {
	_, err := fmt.Fprintln(w)
	return err
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
