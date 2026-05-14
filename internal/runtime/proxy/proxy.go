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

// Package proxy implements the Hysteron proxy runtime.
//
// The component reconciles cluster data from the store and drives one writable
// listener plus an optional read-only listener, including backend destination
// switching and proxy-side runtime metrics.
package proxy

import (
	slog "github.com/woozymasta/hysteron/internal/log"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"

	"github.com/rs/zerolog"
)

// log is the proxy component logger; refreshed after logging is configured.
var log zerolog.Logger

func init() {
	log = slog.WithComponent("proxy")
}

// proxyConfig stores merged proxy runtime configuration loaded from CLI/env.
type proxyConfig struct {
	Writable writableOptions `group:"Writable Proxy"`
	ReadOnly readOnlyOptions `group:"Read-Only Proxy" namespace:"read-only" env-namespace:"READ_ONLY"`
	runtimecommon.CommonConfig

	KeepAlive     tcpKeepAliveOptions `group:"TCP Keep-Alive" namespace:"tcp-keepalive" env-namespace:"TCP_KEEPALIVE"`
	StopListening bool                `long:"stop-listening" env:"STOP_LISTENING" description:"stop listening on store error (default true)"`
}
