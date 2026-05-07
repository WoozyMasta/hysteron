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

// Package configfile decodes user-facing Hysteron configuration files.
//
// The package supports both JSON and YAML inputs and applies bash-style
// `${VAR}` expansion uniformly to every string scalar. Users can opt out
// of expansion per scalar with the `$${VAR}` escape.
//
// Expansion is intentionally left unconstrained: any policy of "skip
// these paths" is brittle (it both removes legitimate parametrization
// opportunities like pgHBA CIDRs or primaryConninfo passwords AND fails
// the moment a new field is added). The escape syntax is the right tool
// for keeping a literal `${...}` in shell or PostgreSQL commands.
package configfile

import (
	"errors"
	"strings"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/jamle"
)

// Unmarshal decodes YAML or JSON using json tags and Hysteron expansion rules.
func Unmarshal(data []byte, v any) error {
	return jamle.Unmarshal(data, v)
}

// ClusterSpec decodes a cluster specification file.
func ClusterSpec(data []byte) (*cluster.ClusterSpec, error) {
	var spec *cluster.ClusterSpec
	if err := Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// ClusterData decodes a cluster data file. An empty payload is reported
// with the legacy "unexpected end of JSON input" wording so existing
// tools can rely on it.
func ClusterData(data []byte) (*cluster.ClusterData, error) {
	if strings.TrimSpace(string(data)) == "" {
		return nil, errors.New("unexpected end of JSON input")
	}
	var cd cluster.ClusterData
	if err := Unmarshal(data, &cd); err != nil {
		return nil, err
	}
	return &cd, nil
}
