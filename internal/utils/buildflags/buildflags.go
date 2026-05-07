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

// Package buildflags exposes build metadata shared by all Stolon binaries.
//
// Values are populated via -ldflags="-X ..." at link time (see Makefile)
// and consumed by the CLI parser to render `--version` and the built-in
// `version` subcommand. When the binary is built without these flags
// (e.g. plain `go run`) the defaults below remain visible.
package buildflags

import "time"

// These variables are intentionally exported and writable: they are the
// targets of `-ldflags -X` injections from the build pipeline.
var (
	// Version is the human-readable release tag (e.g. v0.18.0).
	Version = "dev"
	// Commit is the VCS revision (full SHA) the binary was built from.
	Commit = ""
	// Date is the build timestamp in RFC3339 format (UTC).
	Date = ""
	// URL is the upstream repository URL.
	URL = ""
)

// BuildTime parses Date into a time.Time. Returns the zero value if Date
// is empty or unparsable; callers should treat that as "unknown".
func BuildTime() time.Time {
	if Date == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, Date)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}
