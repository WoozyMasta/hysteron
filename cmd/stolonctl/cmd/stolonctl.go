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

// Package cmd implements stolonctl commands.
package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sorintlab/stolon/cmd"
	"github.com/sorintlab/stolon/internal/cluster"
	slog "github.com/sorintlab/stolon/internal/log"
	"github.com/sorintlab/stolon/internal/store"
	"github.com/woozymasta/flags"
)

const maxRetries = 3

// stolonCtl is the root option/command tree for stolonctl. Subcommands
// are declared via `command:` tags so the parser walks the struct via
// reflection and we never call AddCommand programmatically.
type stolonCtl struct {
	FailKeeper   FailKeeperCommand   `command:"failkeeper" description:"Force a keeper as temporarily failed"`
	RemoveKeeper RemoveKeeperCommand `command:"removekeeper" description:"Remove a keeper from cluster data"`

	Status StatusCommand `command:"status" description:"Display the current cluster status"`

	Init        InitCommand        `command:"init" description:"Initialize a new cluster"`
	Update      UpdateCommand      `command:"update" description:"Update a cluster specification"`
	ClusterData ClusterDataCommand `command:"clusterdata" description:"Manage current cluster data"`

	cmd.CommonConfig

	Spec    SpecCommand    `command:"spec" description:"Retrieve the current cluster specification"`
	Promote PromoteCommand `command:"promote" description:"Promote a standby cluster to a primary cluster"`
}

// Package-level singleton: subcommand handlers reach for `cfg.CommonConfig`
// to construct the store/election clients without explicit plumbing.
var (
	cfg stolonCtl
	log = slog.WithComponent("stolonctl")
)

// NewParser returns the configured stolonctl parser. Used by the docs
// generator and by `Execute` below.
func NewParser() *flags.Parser {
	return cmd.NewParser("stolonctl", "STOLONCTL", &cfg, 0)
}

// Execute runs the stolonctl command.
func Execute() {
	parser := NewParser()
	if _, err := parser.Parse(); err != nil {
		os.Exit(cmd.ParseErrorExitCode(err))
	}
	if parser.Active == nil {
		parser.WriteHelp(os.Stdout)
	}
}

// runStolonCtl wraps a command body with shared validation. It is the
// only piece of glue between the `flags.Commander` interface and the
// existing handlers.
func runStolonCtl(fn func() error) error {
	closer, err := cmd.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	log = slog.WithComponent("stolonctl")
	defer cmd.CloseLogging(closer, &log)
	if err := cmd.CheckClusterName(&cfg.CommonConfig); err != nil {
		return err
	}
	if err := cmd.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return err
	}
	return fn()
}

func newStore() (store.Store, error) {
	return cmd.NewStore(&cfg.CommonConfig, false)
}

func newElection(uid string) (store.Election, error) {
	return cmd.NewElection(&cfg.CommonConfig, uid)
}

func stderr(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	if _, err := fmt.Fprintln(os.Stderr, strings.TrimSuffix(out, "\n")); err != nil {
		log.Fatal().Err(err).Msg("failed to write to stderr")
	}
}

func stdout(format string, a ...any) {
	out := fmt.Sprintf(format, a...)
	if _, err := fmt.Fprintln(os.Stdout, strings.TrimSuffix(out, "\n")); err != nil {
		die("write stdout: %v", err)
	}
}

func writeOutput(w io.Writer, format string, a ...any) {
	if _, err := fmt.Fprintf(w, format, a...); err != nil {
		die("write output: %v", err)
	}
}

func die(format string, a ...any) {
	stderr(format, a...)
	os.Exit(1)
}

func getClusterData(e store.Store) (*cluster.ClusterData, *store.KVPair, error) {
	cd, pair, err := e.GetClusterData(context.TODO())
	if err != nil {
		return nil, nil, fmt.Errorf("cannot get cluster data: %v", err)
	}
	if cd == nil {
		return nil, nil, fmt.Errorf("nil cluster data: %v", err)
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		return nil, nil, fmt.Errorf("unsupported cluster data format version %d", cd.FormatVersion)
	}
	if err := cd.Cluster.Spec.Validate(); err != nil {
		return nil, nil, fmt.Errorf("clusterdata validation failed: %v", err)
	}
	return cd, pair, nil
}

func askConfirmation(message string) (bool, error) {
	in := bufio.NewReader(os.Stdin)
	for {
		writeOutput(os.Stdout, "%s", message)
		input, err := in.ReadString('\n')
		if err != nil {
			return false, fmt.Errorf("error reading input: %v", err)
		}
		switch input {
		case "yes\n":
			return true, nil
		case "no\n":
			return false, nil
		default:
			stdout("Please enter 'yes' or 'no'")
		}
	}
}
