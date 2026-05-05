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

package log

// FlagGroup holds logging options for github.com/woozymasta/flags. Nest it in
// each CLI root option struct:
//
//	Log log.FlagGroup `group:"Logging" namespace:"log" env-namespace:"LOG"`
//
// Long flags become --log-* and env vars LOG_*.
type FlagGroup struct {
	Level       string `long:"level" env:"LEVEL" choices:"trace;debug;info;warn;error" default:"info" description:"log verbosity (trace is verbose; use for short-lived diagnostics)"`
	Format      string `long:"format" env:"FORMAT" choices:"text;json" default:"text" description:"log output format (text or JSON)"`
	Output      string `long:"output" env:"OUTPUT" default:"stderr" description:"log destination: stdout, stderr, or a file path"`
	FileMode    string `long:"file-mode" env:"FILE_MODE" choices:"append;truncate" default:"append" description:"when output is a file: append or truncate existing content"`
	TimeFormat  string `long:"time-format" env:"TIME_FORMAT" default:"2006-01-02T15:04:05.000Z07:00" description:"timestamp: Go layout or rfc3339|rfc3339nano|unix|unixms|unixmicro|unixnano"`
	ColorPolicy string `long:"color-policy" env:"COLOR_POLICY" choices:"auto;always;never" default:"auto" description:"console colors: auto honors NO_COLOR, FORCE_COLOR, and TTY detection"`
}

// ToOptions maps parsed CLI/env fields into Configure options.
func (f FlagGroup) ToOptions() Options {
	return Options{
		Level:      f.Level,
		Format:     f.Format,
		Output:     f.Output,
		FileMode:   f.FileMode,
		TimeFormat: f.TimeFormat,
		Color:      f.ColorPolicy,
	}
}
