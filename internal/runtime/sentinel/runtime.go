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
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	slog "github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/id"
)

// RunOptions provides typed sentinel runtime options for unified CLI.
type RunOptions struct {
	InitialClusterSpecFile string // Optional default initial cluster-spec file path.

	WebListenAddress string   // Web UI listen address ("host:port"), empty disables web server.
	WebBasePath      string   // Base path prefix for web routes (for example "/status").
	WebAuthUsername  string   // Basic auth username for web UI; requires password when set.
	WebAuthPassword  string   // Basic auth password for web UI; requires username when set.
	WebReadTimeout   string   // Web server read timeout duration string (for example "5s").
	WebWriteTimeout  string   // Web server write timeout duration string (for example "10s").
	ClusterSpecFiles []string // Per-cluster overrides formatted as "<cluster>=<spec-file-path>".

	WebUnsafeNoAuth bool // Enables admin actions without auth (unsafe, test/dev only).
}

// RunWithOptions executes sentinel runtime without re-parsing component flags.
func RunWithOptions(commonConfig stconfig.CommonConfig, opts RunOptions) error {
	cfg = config{}
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	cfg.InitialClusterSpecFile = opts.InitialClusterSpecFile
	cfg.ClusterSpecFiles = append([]string(nil), opts.ClusterSpecFiles...)
	cfg.Web.ListenAddress = opts.WebListenAddress
	cfg.Web.BasePath = opts.WebBasePath
	cfg.Web.AuthUsername = opts.WebAuthUsername
	cfg.Web.AuthPassword = opts.WebAuthPassword

	if opts.WebReadTimeout != "" {
		d, err := time.ParseDuration(opts.WebReadTimeout)
		if err != nil {
			return fmt.Errorf("invalid web read timeout: %w", err)
		}
		cfg.Web.ReadTimeout = d
	}

	if opts.WebWriteTimeout != "" {
		d, err := time.ParseDuration(opts.WebWriteTimeout)
		if err != nil {
			return fmt.Errorf("invalid web write timeout: %w", err)
		}
		cfg.Web.WriteTimeout = d
	}

	cfg.Web.UnsafeNoAuth = opts.WebUnsafeNoAuth
	return runSentinel()
}

// sigHandler waits for a termination signal and triggers context cancellation.
func sigHandler(sigs chan os.Signal, cancel context.CancelFunc) {
	s := <-sigs
	log.Debug().
		Str("signal", s.String()).
		Msg("received shutdown signal")
	cancel()
}

// clusterSpecFiles builds per-cluster initial spec file mapping from default
// path and explicit cluster overrides.
func clusterSpecFiles(defaultSpec string, overrides []string, clusterNames []string) (map[string]string, error) {
	clusterSet := map[string]struct{}{}
	for _, name := range clusterNames {
		clusterSet[name] = struct{}{}
	}

	specs := map[string]string{}
	for _, name := range clusterNames {
		if defaultSpec != "" {
			specs[name] = defaultSpec
		}
	}

	for _, override := range overrides {
		name, path, ok := strings.Cut(override, "=")
		name = strings.TrimSpace(name)
		path = strings.TrimSpace(path)
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("invalid cluster spec override %q, expected <cluster-name>=<path>", override)
		}
		if _, ok := clusterSet[name]; !ok {
			return nil, fmt.Errorf("cluster spec override references unknown cluster %q", name)
		}
		if _, ok := specs[name]; ok && specs[name] != defaultSpec {
			return nil, fmt.Errorf("duplicate cluster spec override for cluster %q", name)
		}
		specs[name] = path
	}

	return specs, nil
}

// runSentinelCluster runs one cluster-specific sentinel loop with retry and
// exponential backoff.
func runSentinelCluster(
	ctx context.Context,
	uid string,
	cfg *config,
	clusterName,
	initialSpecFile string,
	webRegistry *sentinelWebRegistry,
) {
	logger := slog.WithComponent("sentinel").With().
		Str(slog.FieldClusterName, clusterName).
		Logger()
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		s, err := NewSentinel(uid, cfg, clusterName, initialSpecFile, nil)
		if err != nil {
			logger.Error().
				Err(err).
				Dur("retry_after", backoff).
				Msg("failed to create sentinel cluster runner")
			if !waitForSentinelRetry(ctx, backoff) {
				return
			}
			backoff = nextSentinelRetryBackoff(backoff, maxBackoff)
			continue
		}

		if webRegistry != nil {
			webRegistry.Set(clusterName, s)
		}

		runtimecommon.SetMetricsForCluster(clusterName, "sentinel")
		if err := runSentinelOnce(ctx, s); err != nil {
			s.log.Error().
				Err(err).
				Dur("retry_after", backoff).
				Msg("sentinel cluster runner stopped unexpectedly")
			if !waitForSentinelRetry(ctx, backoff) {
				return
			}
			backoff = nextSentinelRetryBackoff(backoff, maxBackoff)
			continue
		}

		if webRegistry != nil {
			webRegistry.Delete(clusterName)
		}
		return
	}
}

// waitForSentinelRetry blocks until backoff elapses or context is canceled.
func waitForSentinelRetry(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// nextSentinelRetryBackoff doubles retry delay up to configured cap.
func nextSentinelRetryBackoff(current, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

// runSentinelOnce executes one sentinel instance and converts panics to errors.
func runSentinelOnce(ctx context.Context, s *Sentinel) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in sentinel cluster runner: %v", r)
		}
	}()

	s.Start(ctx)
	if ctx.Err() != nil {
		return nil
	}

	return errors.New("sentinel cluster runner returned without cancellation")
}

// runSentinel configures process-level runtime and starts per-cluster loops.
func runSentinel() error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	log = slog.WithComponent("sentinel")
	postgresql.SetLogger(slog.L())
	defer runtimecommon.CloseLogging(closer, &log)

	clusterNames, err := runtimecommon.CheckClusterNames(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("invalid cluster names: %w", err)
	}
	if err := runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}
	if err := checkSentinelConfig(&cfg); err != nil {
		return fmt.Errorf("invalid sentinel configuration: %w", err)
	}

	specFiles, err := clusterSpecFiles(cfg.InitialClusterSpecFile, cfg.ClusterSpecFiles, clusterNames)
	if err != nil {
		return fmt.Errorf("invalid cluster spec configuration: %w", err)
	}

	uid := id.UID()
	log.Info().Str(slog.FieldSentinelUID, uid).Msg("sentinel UID assigned")

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go sigHandler(sigs, cancel)

	if cfg.Metrics.ListenAddress != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsServer := http.Server{
			Addr:              cfg.Metrics.ListenAddress,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			err := metricsServer.ListenAndServe()
			if err != nil {
				log.Error().Err(err).Msg("metrics http server error")
				cancel()
			}
		}()
	}
	webRegistry := newSentinelWebRegistry(uid)
	if cfg.Web.ListenAddress != "" {
		webServer := newWebServer(&cfg, clusterNames, webRegistry)
		go func() {
			err := webServer.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error().Err(err).Msg("web http server error")
				cancel()
			}
		}()
	}

	var wg sync.WaitGroup
	for _, clusterName := range clusterNames {
		wg.Go(func() {
			runSentinelCluster(
				ctx,
				uid,
				&cfg,
				clusterName,
				specFiles[clusterName],
				webRegistry,
			)
		})
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// checkSentinelConfig validates sentinel-specific configuration constraints.
func checkSentinelConfig(cfg *config) error {
	if (cfg.KubeService.Enabled || cfg.KubeService.ReadOnlyEnabled) &&
		stconfig.NormalizeStoreBackend(cfg.Store.Backend) != "kubernetes" {
		return errors.New("kubernetes service publishing requires --store-backend=kubernetes")
	}

	if cfg.KubeService.Enabled &&
		cfg.KubeService.ReadOnlyEnabled &&
		cfg.KubeService.ServiceName == cfg.KubeService.ReadOnlyServiceName &&
		cfg.KubeService.ServicePort == cfg.KubeService.ReadOnlyServicePort {
		return errors.New("kubernetes writable and read-only services cannot use the same name and port")
	}

	if err := validateWebConfig(cfg); err != nil {
		return err
	}

	return nil
}
