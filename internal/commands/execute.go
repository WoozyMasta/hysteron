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

package commands

import (
	"errors"
	"fmt"
	"os"

	"github.com/woozymasta/flags"
)

// Execute parses and runs the unified command tree.
func Execute() {
	os.Exit(ExecuteArgs(os.Args[1:]))
}

// ExecuteArgs parses and runs the unified command tree with explicit args.
// It returns process exit code following parser/help semantics.
func ExecuteArgs(args []string) int {
	parser := NewParser()
	if _, err := parser.ParseArgs(args); err != nil {
		return parseErrorExitCode(err)
	}
	if parser.Active == nil {
		fmt.Fprintf(os.Stderr, "%v\n", ErrNoActiveCommand)
		parser.WriteHelp(os.Stderr)
		return 1
	}
	return 0
}

func parseErrorExitCode(err error) int {
	var flagsErr *flags.Error
	if errors.As(err, &flagsErr) &&
		(flagsErr.Type == flags.ErrHelp || flagsErr.Type == flags.ErrVersion) {
		return 0
	}
	return 1
}
