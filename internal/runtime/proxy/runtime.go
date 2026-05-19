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

package proxy

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/health"
	slog "github.com/woozymasta/hysteron/internal/log"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/tcpproxy"
	"github.com/woozymasta/hysteron/internal/utils/id"
	"github.com/woozymasta/hysteron/internal/utils/readonly"
	"github.com/woozymasta/hysteron/internal/utils/units"
)

// RunOptions provides typed proxy runtime options for unified CLI.
type RunOptions struct {
	ListenAddress           string // Writable listener bind address.
	Port                    string // Writable listener bind port.
	ReadOnlyListenAddress   string // Read-only listener bind address.
	ReadOnlyPort            string // Read-only listener bind port.
	ReadOnlyReplicaPriority string // Read-only replica priority: sync, async, any.
	ReadOnlyMaxLagBytes     uint64 // Maximum eligible read-only lag in bytes.
	ReadOnlyNoFallback      bool   // Disable fallback to primary for read-only routing.
	ReadOnlyIncludePrimary  bool   // Include primary in regular read-only destination pool.
	DisableWritableListener bool   // Disables writable listener when true.
}

// RunWithOptions executes proxy runtime without re-parsing component flags.
func RunWithOptions(commonConfig stconfig.CommonConfig, opts RunOptions) error {
	cfg = proxyConfig{StopListening: true}
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	cfg.Writable.ListenAddress = opts.ListenAddress
	cfg.Writable.Port = opts.Port
	cfg.Writable.DisableListener = opts.DisableWritableListener
	cfg.ReadOnly.ListenAddress = opts.ReadOnlyListenAddress
	cfg.ReadOnly.Port = opts.ReadOnlyPort
	cfg.ReadOnly.ReplicaPriority = readonly.ReplicaPriority(opts.ReadOnlyReplicaPriority)
	cfg.ReadOnly.MaxLag = units.BytesValue(opts.ReadOnlyMaxLagBytes)
	cfg.ReadOnly.NoFallback = opts.ReadOnlyNoFallback
	cfg.ReadOnly.IncludePrimary = opts.ReadOnlyIncludePrimary
	return runProxy()
}

// runProxy is the internal process entrypoint for the proxy component.
func runProxy() error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	log = slog.WithComponent("proxy")
	tcpproxy.SetLogger(log)
	defer runtimecommon.CloseLogging(closer, &log)

	if err := runtimecommon.CheckClusterName(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid cluster name: %w", err)
	}
	if err := runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}

	runtimecommon.SetMetrics(&cfg.CommonConfig, "proxy")

	uid := id.UID()
	log.Info().Str(slog.FieldProxyUID, uid).Msg("proxy UID assigned")

	clusterChecker, err := NewClusterChecker(uid, cfg)
	if err != nil {
		return fmt.Errorf("failed to create cluster checker: %w", err)
	}
	if cfg.Metrics.ListenAddress != "" || cfg.Health.ListenAddress != "" {
		healthAddr := cfg.Health.ListenAddress
		if healthAddr == "" {
			healthAddr = cfg.Metrics.ListenAddress
		}
		plan := health.BuildListenerPlan(map[health.RouteGroup]string{
			health.RouteGroupMetrics: cfg.Metrics.ListenAddress,
			health.RouteGroupHealth:  healthAddr,
		})
		checker := health.CheckerFuncs{
			LiveFn:    func(context.Context) error { return nil },
			ReadyFn:   clusterChecker.probeReady,
			StartupFn: clusterChecker.probeStartup,
		}
		for addr, groups := range plan {
			listenAddr := addr
			listenGroups := groups
			mux := http.NewServeMux()
			for _, group := range listenGroups {
				switch group {
				case health.RouteGroupMetrics:
					mux.Handle(
						"/metrics",
						health.WrapBasicAuth(
							"hysteron-metrics",
							cfg.Metrics.AuthUsername,
							cfg.Metrics.AuthPassword,
							promhttp.Handler(),
						),
					)
				case health.RouteGroupHealth:
					health.RegisterRoutes(mux, checker)
				}
			}
			go func() {
				server := &http.Server{
					Addr:              listenAddr,
					Handler:           mux,
					ReadHeaderTimeout: 5 * time.Second,
				}
				err := server.ListenAndServe()
				if err != nil {
					log.Fatal().
						Err(err).
						Str("addr", listenAddr).
						Msg("HTTP server failed")
				}
			}()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		sig := <-sigs
		log.Info().Str("signal", sig.String()).Msg("received shutdown signal")
		cancel()
	}()

	if err = clusterChecker.Start(ctx); err != nil {
		return fmt.Errorf("cluster checker stopped: %w", err)
	}
	return nil
}
