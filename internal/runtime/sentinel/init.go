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

package sentinel

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/configfile"
)

// loadInitialClusterSpecFromFile loads and validates optional initial cluster spec.
func loadInitialClusterSpecFromFile(
	logger zerolog.Logger,
	initialClusterSpecFile string,
) (*cluster.ClusterSpec, error) {
	if initialClusterSpecFile == "" {
		return nil, nil
	}

	configData, err := os.ReadFile(initialClusterSpecFile)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot read provided initial cluster config file: %v",
			err,
		)
	}

	initialClusterSpec, err := configfile.ClusterSpec(configData)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot parse provided initial cluster config: %v",
			err,
		)
	}
	if initialClusterSpec == nil {
		return nil, errors.New("provided initial cluster spec is empty")
	}

	logger.Debug().
		Fields(cluster.LogSummaryClusterSpec(initialClusterSpec)).
		Msg("initial cluster specification loaded from file")

	if err := initialClusterSpec.Validate(); err != nil {
		return nil, fmt.Errorf("invalid initial cluster: %v", err)
	}

	return initialClusterSpec, nil
}
