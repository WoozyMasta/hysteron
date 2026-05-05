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

// Package log provides shared Stolon logging helpers backed by zerolog.
//
// The process-wide root is the same global described in zerolog's log subpackage:
// import zlog "github.com/rs/zerolog/log" -- see zlog.Logger and zlog.Info() etc.
// Configure replaces that root (default from zlog would be stderr; here we start
// from discard until Configure). API that needs *zerolog.Logger can use L(),
// which returns &zlog.Logger.
package log

import (
	stdlog "log"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// stdLogger forwards to the shared root via zerolog.Logger's io.Writer
// implementation (see zerolog docs: stdlog.SetOutput(logger)).
var stdLogger *stdlog.Logger

func init() {
	zlog.Logger = zerolog.Nop()
	stdLogger = stdlog.New(&zlog.Logger, "", 0)
}

// L returns &zlog.Logger, the shared root. Safe to pass where a *zerolog.Logger
// is required; for new code you can also import zlog "github.com/rs/zerolog/log"
// and use zlog.Logger (value) or zlog.Info() directly after Configure.
func L() *zerolog.Logger {
	return &zlog.Logger
}

// Std returns a standard library logger writing through the shared root's
// zerolog Write method (same semantics as upstream zerolog, not a custom level).
func Std() *stdlog.Logger {
	return stdLogger
}
