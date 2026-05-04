// Copyright 2018 Sorint.lab
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

// Command gen_commands_doc generates markdown docs for CLI commands.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	keepercmd "github.com/sorintlab/stolon/cmd/keeper/cmd"
	proxycmd "github.com/sorintlab/stolon/cmd/proxy/cmd"
	sentinelcmd "github.com/sorintlab/stolon/cmd/sentinel/cmd"
	stolonctlcmd "github.com/sorintlab/stolon/cmd/stolonctl/cmd"
	"github.com/woozymasta/flags"
)

func main() {
	// use os.Args instead of "flags" because "flags" will mess up the man pages!
	var outDir string
	if len(os.Args) == 2 {
		outDir = os.Args[1]
	} else {
		fmt.Fprintf(os.Stderr, "usage: %s [output directory]", os.Args[0])
		os.Exit(1)
	}

	if err := writeDoc(outDir, "stolon-keeper.md", keepercmd.NewParser()); err != nil {
		log.Fatal(err)
	}
	if err := writeDoc(outDir, "stolon-sentinel.md", sentinelcmd.NewParser()); err != nil {
		log.Fatal(err)
	}
	if err := writeDoc(outDir, "stolon-proxy.md", proxycmd.NewParser()); err != nil {
		log.Fatal(err)
	}
	if err := writeDoc(outDir, "stolonctl.md", stolonctlcmd.NewParser()); err != nil {
		log.Fatal(err)
	}
}

func writeDoc(outDir, name string, parser *flags.Parser) error {
	if err := parser.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0750); err != nil { //nolint:gosec // The generator intentionally accepts an output directory argument.
		return err
	}
	file, err := os.Create(filepath.Join(outDir, name)) //nolint:gosec // The generator writes fixed filenames under the caller-provided output directory.
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	return parser.WriteDoc(file, flags.DocFormatMarkdown, flags.WithBuiltinTemplate(flags.DocTemplateMarkdownList))
}
