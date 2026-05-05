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

package common

import (
	slog "github.com/sorintlab/stolon/internal/log"
)

// MustNot terminates the process with a structured fatal log when an internal
// invariant is violated. Use only where the caller cannot recover — never for
// errors that depend on external input or I/O.
func MustNot(err error, msg string) {
	if err == nil {
		return
	}
	slog.L().Fatal().Err(err).Msg(msg)
}

// MustNotMsg logs msg and exits when cond is true (invariant violated).
func MustNotMsg(cond bool, msg string) {
	if !cond {
		return
	}
	slog.L().Fatal().Msg(msg)
}
