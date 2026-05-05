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

package log

import (
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// Stable JSON field keys for structured logs across Stolon binaries.
const (
	FieldComponent   = "component"
	FieldClusterName = "cluster_name"
	FieldKeeperUID   = "keeper_uid"
	FieldDBUID       = "db_uid"
	FieldProxyUID    = "proxy_uid"
	FieldSentinelUID = "sentinel_uid"
)

// WithComponent returns a child of the process root with FieldComponent set.
func WithComponent(name string) zerolog.Logger {
	return zlog.Logger.With().Str(FieldComponent, name).Logger()
}

// WithCluster returns a child logger tagged with FieldClusterName.
func WithCluster(clusterName string) zerolog.Logger {
	return zlog.Logger.With().Str(FieldClusterName, clusterName).Logger()
}
