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

// Format identifies a supported output mode.
type Format string

const (
	// FormatPlain renders a human-readable output.
	FormatPlain Format = "plain"
	// FormatJSON renders JSON output.
	FormatJSON Format = "json"
	// FormatYAML renders YAML output.
	FormatYAML Format = "yaml"
)

// FormatOptions defines shared output flags for management commands.
type FormatOptions struct {
	Format string `short:"f" long:"format" default:"plain" choices:"plain;yaml;json" description:"output format"`
}

// Selected returns normalized output format.
func (o FormatOptions) Selected() Format {
	if o.Format == "" {
		return FormatPlain
	}
	return Format(o.Format)
}
