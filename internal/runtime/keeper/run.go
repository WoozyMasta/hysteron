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

package keeper

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/woozymasta/hysteron/internal/config"
	"github.com/woozymasta/hysteron/internal/log"
	"github.com/woozymasta/hysteron/internal/postgresql"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/osuser"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Keeper runtime entrypoints and process wiring.

// RunPostgresReplOptions configures postgres replication user in RunWithOptions.
type RunPostgresReplOptions struct {
	AuthMethod   string
	Username     string
	Password     string
	PasswordFile string
}

// RunPostgresSUOptions configures postgres superuser in RunWithOptions.
type RunPostgresSUOptions struct {
	AuthMethod   string
	Username     string
	Password     string
	PasswordFile string
}

// RunPostgresOptions configures postgres runtime fields in RunWithOptions.
type RunPostgresOptions struct {
	ListenAddress    string
	AdvertiseAddress string
	Port             string
	AdvertisePort    string
	BinPath          string
	WALDir           string
	TablespaceDirs   []string
	Repl             RunPostgresReplOptions
	SU               RunPostgresSUOptions
}

// RunOptions provides typed keeper runtime options for unified CLI.
type RunOptions struct {
	PG      RunPostgresOptions
	UID     string
	DataDir string

	CanBeMaster             bool
	CanBeSynchronousReplica bool
	DisableDataDirLocking   bool
	AllowNewerPG            bool
}

// RunWithOptions executes keeper runtime without re-parsing component flags.
func RunWithOptions(commonConfig config.CommonConfig, opts RunOptions) error {
	cfg = runConfig{
		CanBeMaster:             opts.CanBeMaster,
		CanBeSynchronousReplica: opts.CanBeSynchronousReplica,
	}
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	cfg.UID = opts.UID
	cfg.DataDir = opts.DataDir
	cfg.DisableDataDirLocking = opts.DisableDataDirLocking
	cfg.AllowNewerPG = opts.AllowNewerPG

	cfg.PG.ListenAddress = opts.PG.ListenAddress
	cfg.PG.AdvertiseAddress = opts.PG.AdvertiseAddress
	cfg.PG.Port = opts.PG.Port
	cfg.PG.AdvertisePort = opts.PG.AdvertisePort
	cfg.PG.BinPath = opts.PG.BinPath
	cfg.PG.WALDir = opts.PG.WALDir
	cfg.PG.TablespaceDirs = opts.PG.TablespaceDirs

	cfg.PG.Repl.AuthMethod = opts.PG.Repl.AuthMethod
	cfg.PG.Repl.Username = opts.PG.Repl.Username
	cfg.PG.Repl.Password = opts.PG.Repl.Password
	cfg.PG.Repl.PasswordFile = opts.PG.Repl.PasswordFile

	cfg.PG.SU.AuthMethod = opts.PG.SU.AuthMethod
	cfg.PG.SU.Username = opts.PG.SU.Username
	cfg.PG.SU.Password = opts.PG.SU.Password
	cfg.PG.SU.PasswordFile = opts.PG.SU.PasswordFile

	return runKeeper()
}

// sigHandler blocks for termination signals and triggers keeper cancellation.
func sigHandler(sigs chan os.Signal, cancel context.CancelFunc) {
	s := <-sigs
	keeperRootLog(
		nil,
	).Debug().
		Str("signal", s.String()).
		Msg("shutdown signal received")
	cancel()
}

// runKeeper executes keeper runtime using the package-level cfg values.
func runKeeper() error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	kl := keeperRootLog(&cfg)
	defer runtimecommon.CloseLogging(closer, kl)
	postgresql.SetLogger(log.L())

	var (
		listenAddFlag = "pg-advertise-address"
	)

	if cfg.PG.SU.Username == "" {
		// set the pgSuUsername to the current user
		var user string
		user, err = osuser.GetUser()
		if err != nil {
			return fmt.Errorf("failed to get current user: %w", err)
		}
		cfg.PG.SU.Username = user
	}

	if cfg.DataDir == "" {
		return errors.New("data directory is required")
	}

	if err = runtimecommon.CheckClusterName(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid cluster name: %w", err)
	}
	if err = runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}

	runtimecommon.SetMetrics(&cfg.CommonConfig, "keeper")

	if err = os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory %q: %w", cfg.DataDir, err)
	}

	if cfg.PG.ListenAddress == "" {
		return errors.New("postgresql listen address is required")
	}

	if cfg.PG.AdvertiseAddress == "" {
		listenAddFlag = "pg-listen-address"
		cfg.PG.AdvertiseAddress = cfg.PG.ListenAddress
	}

	if cfg.PG.AdvertisePort == "" {
		cfg.PG.AdvertisePort = cfg.PG.Port
	}

	ip := net.ParseIP(cfg.PG.AdvertiseAddress)
	if ip == nil {
		kl.Warn().
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("PostgreSQL advertise address is not an IP address")
	}

	ipAddr, err := net.ResolveIPAddr("ip", cfg.PG.AdvertiseAddress)
	if err != nil {
		kl.Warn().
			Err(err).
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("failed to resolve PostgreSQL advertise address")
	} else if ipAddr.IP.IsLoopback() {
		kl.Warn().
			Str("listen_flag", listenAddFlag).
			Str("advertise_address", cfg.PG.AdvertiseAddress).
			Msg("PostgreSQL advertise address is a loopback address")
	}

	if cfg.PG.Repl.Username == "" {
		return errors.New("postgresql replication username is required")
	}
	if cfg.PG.Repl.AuthMethod == "trust" {
		kl.Warn().Msg("PostgreSQL replication user uses trust authentication")
		if cfg.PG.Repl.Password != "" ||
			cfg.PG.Repl.PasswordFile != "" {
			return errors.New("postgresql replication password cannot be set when trust authentication is used")
		}
	} else if cfg.PG.Repl.Password == "" && cfg.PG.Repl.PasswordFile == "" {
		return errors.New("postgresql replication password is required")
	}
	if cfg.PG.SU.AuthMethod == "trust" {
		kl.Warn().Msg("PostgreSQL superuser uses trust authentication")
		if cfg.PG.SU.Password != "" || cfg.PG.SU.PasswordFile != "" {
			return errors.New("postgresql superuser password cannot be set when trust authentication is used")
		}
	} else if cfg.PG.SU.Password == "" && cfg.PG.SU.PasswordFile == "" {
		return errors.New("postgresql superuser password is required")
	}

	if cfg.PG.Repl.PasswordFile != "" {
		cfg.PG.Repl.Password, err = readPasswordFromFile(
			cfg.PG.Repl.PasswordFile,
		)
		if err != nil {
			return fmt.Errorf("failed to read PostgreSQL replication password file: %w", err)
		}
	}
	if cfg.PG.SU.PasswordFile != "" {
		cfg.PG.SU.Password, err = readPasswordFromFile(
			cfg.PG.SU.PasswordFile,
		)
		if err != nil {
			return fmt.Errorf("failed to read PostgreSQL superuser password file: %w", err)
		}
	}

	// Trim trailing new lines from passwords
	tp := strings.TrimRight(cfg.PG.SU.Password, "\r\n")
	if cfg.PG.SU.Password != tp {
		kl.Warn().Msg("superuser password contains a trailing newline, removing it")
		if tp == "" {
			return errors.New("superuser password is empty after removing trailing newlines")
		}
		cfg.PG.SU.Password = tp
	}

	tp = strings.TrimRight(cfg.PG.Repl.Password, "\r\n")
	if cfg.PG.Repl.Password != tp {
		kl.Warn().Msg("replication user password contains a trailing newline, removing it")
		if tp == "" {
			return errors.New("replication user password is empty after removing trailing newlines")
		}
		cfg.PG.Repl.Password = tp
	}

	if cfg.PG.SU.Username == cfg.PG.Repl.Username {
		kl.Warn().Msg("provided superuser name and replication user name are the same")
		if cfg.PG.Repl.AuthMethod != cfg.PG.SU.AuthMethod {
			return errors.New("provided superuser name and replication user name are the same but provided authentication methods are different")
		}
		if cfg.PG.SU.Password != cfg.PG.Repl.Password &&
			cfg.PG.SU.AuthMethod != "trust" &&
			cfg.PG.Repl.AuthMethod != "trust" {
			return errors.New("provided superuser name and replication user name are the same but provided passwords are different")
		}
	}

	// Open (and create if needed) the lock file.
	// There is no need to clean up this file since we don't use the file as an
	// actual lock. We get a lock on the file. So the lock get released when
	// our process stops (or log.Fatalfs).
	var lockFile *os.File
	if !cfg.DisableDataDirLocking {
		lockFileName := filepath.Join(cfg.DataDir, "lock")
		lockFile, err = os.OpenFile(
			lockFileName,
			os.O_RDWR|os.O_CREATE,
			0600,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to open data directory lock file %q: %w",
				lockFileName,
				err,
			)
		}

		if err := takeDataDirLock(lockFile); err != nil {
			return fmt.Errorf(
				"cannot take exclusive lock on data dir %q: %w",
				lockFileName,
				err,
			)
		}

		kl.Info().
			Str("data_dir", cfg.DataDir).
			Msg("exclusive lock on data dir taken")
	}

	if cfg.UID != "" {
		if !postgresql.IsValidReplSlotName(cfg.UID) {
			return fmt.Errorf("keeper uid %q is not a valid replication slot name", cfg.UID)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error)
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
			err = metricsServer.ListenAndServe()
			if err != nil {
				kl.Error().
					Err(err).
					Str("addr", cfg.Metrics.ListenAddress).
					Msg("metrics HTTP server failed")
				cancel()
			}
		}()
	}

	p, err := NewPostgresKeeper(&cfg, end)
	if err != nil {
		return fmt.Errorf("failed to create postgres keeper: %w", err)
	}
	go p.Start(ctx)

	if err := <-end; err != nil {
		p.baseLog().Error().Err(err).Msg("keeper run failed")
	}

	if !cfg.DisableDataDirLocking {
		if err := lockFile.Close(); err != nil {
			p.baseLog().
				Error().
				Err(err).
				Msg("failed to close data directory lock file")
		}
	}
	return nil
}
