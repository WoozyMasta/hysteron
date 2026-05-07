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

// Package common contains shared constants and helpers used across Stolon.
package common

import (
	"reflect"
	"strings"
)

const (
	// StorePrefix is the default base path used for cluster data in key-value stores.
	StorePrefix = "stolon/cluster"

	// SentinelLeaderKey is the key used for sentinel leader election state.
	SentinelLeaderKey = "sentinel-leader"
)

// PgUnixSocketDirectories is the default PostgreSQL Unix socket directory.
const PgUnixSocketDirectories = "/tmp"

// Role identifies a PostgreSQL instance role.
type Role string

const (
	// RoleUndefined means the PostgreSQL role is not known.
	RoleUndefined Role = "undefined"
	// RoleMaster means the PostgreSQL instance is primary.
	RoleMaster Role = "master"
	// RoleStandby means the PostgreSQL instance is standby.
	RoleStandby Role = "standby"
)

// Roles enumerates all possible Role values
var Roles = []Role{
	RoleUndefined,
	RoleMaster,
	RoleStandby,
}

const (
	stolonPrefix = "stolon_"
)

// StolonName returns name with the Stolon-managed object prefix.
func StolonName(name string) string {
	return stolonPrefix + name
}

// NameFromStolonName removes the Stolon-managed object prefix from stolonName.
func NameFromStolonName(stolonName string) string {
	return strings.TrimPrefix(stolonName, stolonPrefix)
}

// IsStolonName reports whether name has the Stolon-managed object prefix.
func IsStolonName(name string) bool {
	return strings.HasPrefix(name, stolonPrefix)
}

// Parameters maps PostgreSQL parameter names to values.
type Parameters map[string]string

// Equals reports whether s and is contain the same parameters.
func (s Parameters) Equals(is Parameters) bool {
	return reflect.DeepEqual(s, is)
}

// Diff returns the list of pgParameters changed(newly added, existing deleted and value changed)
func (s Parameters) Diff(newParams Parameters) []string {
	var changedParams []string
	for k, v := range newParams {
		if val, ok := s[k]; !ok || v != val {
			changedParams = append(changedParams, k)
		}
	}

	for k := range s {
		if _, ok := newParams[k]; !ok {
			changedParams = append(changedParams, k)
		}
	}
	return changedParams
}
