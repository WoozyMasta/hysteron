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

//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	missing := missingIntegrationPrerequisites()
	if len(missing) > 0 {
		fmt.Fprintln(os.Stderr, "skipping integration suite: missing prerequisites:")
		for _, item := range missing {
			fmt.Fprintf(os.Stderr, "  - %s\n", item)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func missingIntegrationPrerequisites() []string {
	var missing []string

	if strings.TrimSpace(os.Getenv("STOLON_BIN")) == "" {
		missing = append(missing, "STOLON_BIN")
	}
	backend := strings.TrimSpace(os.Getenv("STOLON_TEST_STORE_BACKEND"))
	switch backend {
	case "etcd", "etcdv3":
	default:
		missing = append(
			missing,
			`STOLON_TEST_STORE_BACKEND (expected "etcd" or "etcdv3")`,
		)
	}
	if strings.TrimSpace(os.Getenv("ETCD_BIN")) == "" {
		missing = append(missing, "ETCD_BIN")
	}
	missing = appendMissingCommands(missing, "initdb", "postgres", "pg_ctl")
	return missing
}

func appendMissingCommands(missing []string, commands ...string) []string {
	for _, command := range commands {
		if _, err := exec.LookPath(command); err != nil {
			missing = append(missing, fmt.Sprintf("command %q in PATH", command))
		}
	}
	return missing
}
