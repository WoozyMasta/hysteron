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
	"encoding/json"
	"fmt"
	"io"

	"github.com/jedib0t/go-pretty/v6/table"
	"go.yaml.in/yaml/v3"
)

// WriteClusterList renders cluster names with the selected format.
func WriteClusterList(w io.Writer, format Format, clusterNames []string) error {
	switch format {
	case FormatJSON:
		return WriteValue(w, format, clusterNames)
	case FormatYAML:
		return WriteValue(w, format, clusterNames)
	case FormatPlain:
		return writeClusterTable(w, clusterNames)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

// WriteValue renders a generic value with a machine-readable format.
func WriteValue(w io.Writer, format Format, value any) error {
	switch format {
	case FormatJSON:
		return writeJSON(w, value)
	case FormatYAML, FormatPlain:
		return writeYAML(w, value)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedFormat, format)
	}
}

func writeClusterTable(w io.Writer, clusterNames []string) error {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.AppendHeader(table.Row{"Cluster"})
	for _, name := range clusterNames {
		t.AppendRow(table.Row{name})
	}
	t.Render()
	return nil
}

func writeJSON(w io.Writer, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

func writeYAML(w io.Writer, value any) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}
