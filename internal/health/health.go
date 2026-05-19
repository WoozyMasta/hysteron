// Copyright 20[0-9][0-9](?:-20[0-9][0-9])? (?:Sorint\.lab|WoozyMasta)(?:\nCopyright 2026 WoozyMasta)?
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

package health

import (
	"context"
	"fmt"
	"net/http"
)

// Checker reports process health states for HTTP health endpoints.
type Checker interface {
	// Live returns nil when process liveness is healthy.
	Live(context.Context) error
	// Ready returns nil when process readiness is healthy.
	Ready(context.Context) error
	// Startup returns nil when startup initialization is complete.
	Startup(context.Context) error
}

// StaticChecker always reports healthy for all states.
type StaticChecker struct{}

// Live reports healthy liveness state.
func (StaticChecker) Live(context.Context) error { return nil }

// Ready reports healthy readiness state.
func (StaticChecker) Ready(context.Context) error { return nil }

// Startup reports healthy startup state.
func (StaticChecker) Startup(context.Context) error { return nil }

// CheckerFuncs builds a Checker from simple functions.
type CheckerFuncs struct {
	LiveFn    func(context.Context) error
	ReadyFn   func(context.Context) error
	StartupFn func(context.Context) error
}

// Live runs LiveFn when provided, otherwise reports healthy.
func (c CheckerFuncs) Live(ctx context.Context) error {
	if c.LiveFn == nil {
		return nil
	}
	return c.LiveFn(ctx)
}

// Ready runs ReadyFn when provided, otherwise reports healthy.
func (c CheckerFuncs) Ready(ctx context.Context) error {
	if c.ReadyFn == nil {
		return nil
	}
	return c.ReadyFn(ctx)
}

// Startup runs StartupFn when provided, otherwise reports healthy.
func (c CheckerFuncs) Startup(ctx context.Context) error {
	if c.StartupFn == nil {
		return nil
	}
	return c.StartupFn(ctx)
}

// RegisterRoutes registers shared health endpoints on the provided mux.
func RegisterRoutes(mux *http.ServeMux, checker Checker) {
	c := checker
	if c == nil {
		c = StaticChecker{}
	}

	mux.Handle("/health", stateHandler(c.Ready))
	mux.Handle("/healthz", stateHandler(c.Ready))
	mux.Handle("/health/live", stateHandler(c.Live))
	mux.Handle("/health/ready", stateHandler(c.Ready))
	mux.Handle("/health/startup", stateHandler(c.Startup))
}

type checkerFunc func(context.Context) error

func stateHandler(check checkerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := check(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "error: %v\n", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}
