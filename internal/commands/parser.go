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
	"fmt"

	"github.com/sorintlab/stolon/internal/utils/buildflags"
	"github.com/woozymasta/flags"
)

var cfg = newRootCommand()

// NewParser returns the unified stolon parser.
func NewParser() *flags.Parser {
	parser := flags.NewNamedParser(
		"stolon",
		flags.Default|
			flags.HelpCommand|
			flags.VersionCommand|
			flags.CompletionCommand|
			flags.DocsCommand|
			flags.VersionFlag|
			flags.PassDoubleDash|
			flags.DetectShellFlagStyle|
			flags.DetectShellEnvStyle|
			flags.ShowCommandAliases|
			flags.ShowRepeatableInHelp|
			flags.PrintHelpOnInputErrors|
			flags.CommandChain,
	)
	parser.SetEnvPrefix("STOLON")
	parser.NamespaceDelimiter = "-"
	parser.SetVersion(buildflags.Version)
	parser.SetVersionCommit(buildflags.Commit)
	parser.SetVersionTime(buildflags.BuildTime())
	parser.SetVersionURL(buildflags.URL)
	parser.SetVersionFields(flags.VersionFieldsAll)

	mustConfigureParser(parser.SetMaxLongNameLength(64))
	mustConfigureParser(parser.SetBuiltinCommandHidden("docs", true))
	_, err := parser.AddGroup("Application Options", "", &cfg)
	mustConfigureParser(err)

	return parser
}

func mustConfigureParser(err error) {
	if err != nil {
		panic(fmt.Errorf("configure unified parser: %w", err))
	}
}

func newRootCommand() rootCommand {
	return rootCommand{
		Keeper: runtimeCommand{
			Component: "keeper",
		},
		Sentinel: runtimeCommand{
			Component: "sentinel",
		},
		Proxy: runtimeCommand{
			Component: "proxy",
		},
	}
}
