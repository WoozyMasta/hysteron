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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/log"
	rtcommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/id"

	"github.com/rs/zerolog"
)

// baseLog is the structured logger with stable keeper identity (use for all
// PostgresKeeper events).
func (p *PostgresKeeper) baseLog() *zerolog.Logger {
	uid := ""
	if p.keeperLocalState != nil {
		uid = p.keeperLocalState.UID
	}
	l := log.L().With().
		Str(log.FieldComponent, "keeper").
		Str(log.FieldClusterName, p.cfg.ClusterName()).
		Str(log.FieldKeeperUID, uid).
		Str("boot_uuid", p.bootUUID).
		Logger()
	return &l
}

// NewPostgresKeeper creates a PostgreSQL keeper from command configuration.
func NewPostgresKeeper(
	cfg *runConfig,
	end chan error,
) (*PostgresKeeper, error) {
	e, err := rtcommon.NewStore(&cfg.CommonConfig, true)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}

	// Clean and get absolute datadir path
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute datadir path for %q: %w", cfg.DataDir, err)
	}
	walDir := cfg.PG.WALDir
	if walDir != "" {
		walDir, err = filepath.Abs(walDir)
		if err != nil {
			return nil, fmt.Errorf("resolve absolute waldir path for %q: %w", cfg.PG.WALDir, err)
		}
	}

	p := &PostgresKeeper{
		cfg: cfg,

		bootUUID: id.UUID(),

		dataDir: dataDir,

		pgListenAddress:    cfg.PG.ListenAddress,
		pgAdvertiseAddress: cfg.PG.AdvertiseAddress,
		pgPort:             cfg.PG.Port,
		pgAdvertisePort:    cfg.PG.AdvertisePort,
		pgBinPath:          cfg.PG.BinPath,
		pgWALDir:           walDir,
		pgReplAuthMethod:   cfg.PG.Repl.AuthMethod,
		pgReplUsername:     cfg.PG.Repl.Username,
		pgReplPassword:     cfg.PG.Repl.Password,
		pgSUAuthMethod:     cfg.PG.SU.AuthMethod,
		pgSUUsername:       cfg.PG.SU.Username,
		pgSUPassword:       cfg.PG.SU.Password,

		sleepInterval:  cluster.DefaultSleepInterval,
		requestTimeout: cluster.DefaultRequestTimeout,

		keeperLocalState: &LocalState{},
		dbLocalState:     &DBLocalState{},

		canBeMaster:             &cfg.CanBeMaster,
		canBeSynchronousReplica: &cfg.CanBeSynchronousReplica,

		e:   e,
		end: end,

		logicalSlotStandbyAdvanceRetryAfter: make(map[string]time.Time),
		logicalSlotStandbyAdvanceRetryDelay: 10 * time.Second,
		logicalSlotAdvancePending:           make(map[string]queuedLogicalSlotAdvanceOperation),
		logicalSlotAdvanceNotify:            make(chan struct{}, 1),
		failsafeEnabled:                     cluster.DefaultEnableFailsafeMode,
		failsafeProbeInterval:               cluster.DefaultFailsafeProbeInterval,
		failsafeProbeTimeout:                cluster.DefaultFailsafeProbeTimeout,
		failsafeMaxMissingPeers:             cluster.DefaultFailsafeMaxMissingPeers,
		failsafeTTL:                         cluster.DefaultFailsafeTTL,
		failsafeState:                       failsafeStateDisabled,
	}

	err = p.loadKeeperLocalState()
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load keeper local state: %w", err)
	}
	if p.keeperLocalState.UID != "" && p.cfg.UID != "" &&
		p.keeperLocalState.UID != p.cfg.UID {
		return nil, fmt.Errorf(
			"refusing to start: uid in local state file %q does not match --uid %q",
			p.keeperLocalState.UID,
			cfg.UID,
		)
	}
	if p.keeperLocalState.UID == "" {
		p.keeperLocalState.UID = cfg.UID
		if cfg.UID == "" {
			p.keeperLocalState.UID = id.UID()
			p.baseLog().
				Info().
				Str(log.FieldKeeperUID, p.keeperLocalState.UID).
				Msg("generated a new keeper UID (none was configured on the command line)")
		}
		if err = p.saveKeeperLocalState(); err != nil {
			return nil, fmt.Errorf("could not write keeper local state file: %w", err)
		}
	}

	p.baseLog().
		Info().
		Str(log.FieldKeeperUID, p.keeperLocalState.UID).
		Msg("keeper identity loaded; continuing startup")

	err = p.loadDBLocalState()
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load db local state: %w", err)
	}
	return p, nil
}
