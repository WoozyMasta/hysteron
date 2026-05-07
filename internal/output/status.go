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
	if err := writeSectionTitle(w, "Cluster"); err != nil {
		return err
	}
	clusterTable := table.NewWriter()
	clusterTable.SetOutputMirror(w)
	clusterTable.AppendHeader(table.Row{"Field", "Value"})
	clusterTable.AppendRows([]table.Row{
		{"Available", status.Cluster.Available},
		{"Master Keeper", valueOrDash(status.Cluster.MasterKeeperUID)},
		{"Master DB", valueOrDash(status.Cluster.MasterDBUID)},
	})
	clusterTable.Render()

	if err := writeLine(w, ""); err != nil {
		return err
	}
	if err := writeSectionTitle(w, "Sentinels"); err != nil {
		return err
	}
	if len(status.Sentinels) == 0 {
		if err := writeLine(w, "No active sentinels"); err != nil {
			return err
		}
	} else {
		t := table.NewWriter()
		t.SetOutputMirror(w)
		t.AppendHeader(table.Row{"UID", "Leader"})
		for _, sentinel := range status.Sentinels {
			t.AppendRow(table.Row{sentinel.UID, sentinel.Leader})
		}
		t.Render()
	}

	if err := writeLine(w, ""); err != nil {
		return err
	}
	if err := writeSectionTitle(w, "Proxies"); err != nil {
		return err
	}
	if len(status.Proxies) == 0 {
		if err := writeLine(w, "No active proxies"); err != nil {
			return err
		}
	} else {
		t := table.NewWriter()
		t.SetOutputMirror(w)
		t.AppendHeader(table.Row{"UID", "Generation"})
		for _, proxy := range status.Proxies {
			t.AppendRow(table.Row{proxy.UID, proxy.Generation})
		}
		t.Render()
	}

	if err := writeLine(w, ""); err != nil {
		return err
	}
	if err := writeSectionTitle(w, "Keepers"); err != nil {
		return err
	}
	if len(status.Keepers) == 0 {
		if err := writeLine(w, "No keepers available"); err != nil {
			return err
		}
	} else {
		t := table.NewWriter()
		t.SetOutputMirror(w)
		t.AppendHeader(table.Row{
			"UID",
			"Healthy",
			"PG Listen Address",
			"PG Healthy",
			"PG Wanted Generation",
			"PG Current Generation",
		})
		for _, keeper := range status.Keepers {
			t.AppendRow(table.Row{
				keeper.UID,
				keeper.Healthy,
				keeper.ListenAddress,
				keeper.PgHealthy,
				keeper.PgWantedGeneration,
				keeper.PgCurrentGeneration,
			})
		}
		t.Render()
	}

	if len(status.KeeperTree) > 0 {
		if err := writeLine(w, ""); err != nil {
			return err
		}
		if err := writeSectionTitle(w, "Keeper Tree"); err != nil {
			return err
		}
		for _, line := range status.KeeperTree {
			if err := writeLine(w, line); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSectionTitle(w io.Writer, title string) error {
	_, err := fmt.Fprintf(w, "== %s ==\n", title)
	return err
}

func writeLine(w io.Writer, value string) error {
	_, err := fmt.Fprintln(w, value)
	return err
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
