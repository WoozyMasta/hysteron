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

package keeper

import (
	"fmt"
	"os"

	"github.com/woozymasta/hysteron/internal/log"

	"github.com/rs/zerolog"
)

// keeperRootLog builds logger for code paths that run before PostgresKeeper is initialized.
// Call only after runtimecommon.InitLogging so the root output/level are configured.
func keeperRootLog(cfg *runConfig) *zerolog.Logger {
	var l zerolog.Logger
	if cfg == nil {
		l = log.WithComponent("keeper")
	} else {
		l = log.WithComponent("keeper").With().
			Str(log.FieldClusterName, cfg.ClusterName()).
			Str(log.FieldKeeperUID, cfg.UID).
			Logger()
	}
	return &l
}

// readPasswordFromFile loads password content from file and warns on permissive file mode.
func readPasswordFromFile(filepath string) (string, error) {
	fi, err := os.Lstat(filepath)
	if err != nil {
		return "", fmt.Errorf("stat password file %q: %w", filepath, err)
	}

	if fi.Mode() > 0600 {
		// Keep warning-only behavior for now: some Kubernetes volume projections
		// use file modes more permissive than 0600.
		keeperRootLog(nil).Warn().
			Str("file", filepath).
			Str("mode", fmt.Sprintf("%#o", fi.Mode())).
			Msg("password file mode is more permissive than 0600; tighten permissions in production")
	}

	pwBytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("read password file %q: %w", filepath, err)
	}
	return string(pwBytes), nil
}
