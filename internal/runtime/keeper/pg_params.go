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
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	"github.com/woozymasta/hysteron/internal/postgresql"
	"github.com/woozymasta/hysteron/internal/utils/units"
)

// walLevel returns wal_level value to use.
// If user provided wal_level is "logical" return it, otherwise use "replica".
func (p *PostgresKeeper) walLevel(db *cluster.DB) string {
	var additionalValidWalLevels = []string{
		"logical",
	}
	walLevel := "replica"

	if db.Spec.PGParameters != nil {
		if l, ok := db.Spec.PGParameters["wal_level"]; ok {
			if slices.Contains(additionalValidWalLevels, l) {
				walLevel = l
			}
		}
	}

	return walLevel
}

// walKeepSegments returns wal_keep_segments clamped to keeper minimum.
func (p *PostgresKeeper) walKeepSegments(db *cluster.DB) int {
	walKeepSegments := minWalKeepSegments
	if db.Spec.PGParameters != nil {
		if v, ok := db.Spec.PGParameters["wal_keep_segments"]; ok {
			// ignore wrong wal_keep_segments values
			if configuredWalKeepSegments, err := strconv.Atoi(v); err == nil {
				if configuredWalKeepSegments > walKeepSegments {
					walKeepSegments = configuredWalKeepSegments
				}
			}
		}
	}

	return walKeepSegments
}

// walKeepSize returns wal_keep_size clamped to keeper minimum safe value.
func (p *PostgresKeeper) walKeepSize(db *cluster.DB) string {
	// Assume default PostgreSQL segment size unless explicit segment-size support
	// is introduced in cluster contracts.
	minMiB := uint64(minWalKeepSegments) * uint64(postgresql.WalSegSize/(1024*1024))
	defaultWalKeepSize := strconv.FormatUint(minMiB, 10) + "MB"
	if db.Spec.PGParameters != nil {
		if v, ok := db.Spec.PGParameters["wal_keep_size"]; ok {
			sizeBytes, err := units.ParsePostgreSQLBytes(v)
			if err != nil {
				p.baseLog().Warn().
					Str("wal_keep_size", v).
					Err(err).
					Msg("invalid wal_keep_size, using minimum safe value")
				return defaultWalKeepSize
			}

			minBytes := minMiB * 1024 * 1024
			if sizeBytes < minBytes {
				p.baseLog().Warn().
					Str("wal_keep_size", v).
					Uint64("minimum_bytes", minBytes).
					Msg("wal_keep_size below minimum safe value, clamping")
				return defaultWalKeepSize
			}

			return strings.TrimSpace(v)
		}
	}

	return defaultWalKeepSize
}

// mandatoryPGParameters builds keeper-managed PostgreSQL parameters.
func (p *PostgresKeeper) mandatoryPGParameters(
	db *cluster.DB,
) common.Parameters {
	params := common.Parameters{
		"unix_socket_directories": common.PgUnixSocketDirectories,
		"wal_level":               p.walLevel(db),
		"hot_standby":             "on",
	}

	majorVersion, _, err := p.binaryVersion()
	if err != nil {
		// in case we fail to parse the binary version don't return any wal_keep_segments or wal_keep_size
		p.baseLog().
			Warn().
			Err(err).
			Msg("could not read PostgreSQL binary version from installation")
		return params
	}

	if majorVersion >= 13 {
		params["wal_keep_size"] = p.walKeepSize(db)
	} else {
		params["wal_keep_segments"] = strconv.Itoa(p.walKeepSegments(db))
	}

	return params
}

// getSUConnParams builds superuser connection parameters for a target DB.
func (p *PostgresKeeper) getSUConnParams(
	db, followedDB *cluster.DB,
) postgresql.ConnParams {
	sslMode := "prefer"
	if db != nil && db.Spec != nil && db.Spec.ReplicationTLSMode != "" {
		sslMode = string(db.Spec.ReplicationTLSMode)
	}
	connParams := postgresql.ConnParams{
		"user":             p.pgSUUsername,
		"host":             followedDB.Status.ListenAddress,
		"port":             followedDB.Status.Port,
		"application_name": common.HysteronName(db.UID),
		"dbname":           "postgres",
		// Explicitly set sslmode for Go pg clients.
		"sslmode": sslMode,
	}
	if p.pgSUAuthMethod != "trust" {
		connParams.Set("password", p.pgSUPassword)
	}
	return connParams
}

// getReplConnParams builds replication connection parameters for a target DB.
func (p *PostgresKeeper) getReplConnParams(
	db, followedDB *cluster.DB,
) postgresql.ConnParams {
	sslMode := "prefer"
	if db != nil && db.Spec != nil && db.Spec.ReplicationTLSMode != "" {
		sslMode = string(db.Spec.ReplicationTLSMode)
	}
	connParams := postgresql.ConnParams{
		"user":             p.pgReplUsername,
		"host":             followedDB.Status.ListenAddress,
		"port":             followedDB.Status.Port,
		"application_name": common.HysteronName(db.UID),
		// Explicitly set sslmode for Go pg clients.
		"sslmode": sslMode,
	}
	if p.pgReplAuthMethod != "trust" {
		connParams.Set("password", p.pgReplPassword)
	}
	return connParams
}

// getLocalConnParams returns local superuser connection parameters.
func (p *PostgresKeeper) getLocalConnParams() postgresql.ConnParams {
	connParams := postgresql.ConnParams{
		"user":   p.pgSUUsername,
		"host":   common.PgUnixSocketDirectories,
		"port":   p.pgPort,
		"dbname": "postgres",
		// no sslmode defined since it's not needed and supported over unix sockets
	}
	if p.pgSUAuthMethod != "trust" {
		connParams.Set("password", p.pgSUPassword)
	}
	return connParams
}

// getLocalReplConnParams returns local replication connection parameters.
func (p *PostgresKeeper) getLocalReplConnParams() postgresql.ConnParams {
	connParams := postgresql.ConnParams{
		"user":     p.pgReplUsername,
		"password": p.pgReplPassword,
		"host":     common.PgUnixSocketDirectories,
		"port":     p.pgPort,
		// no sslmode defined since it's not needed and supported over unix sockets
	}
	if p.pgReplAuthMethod != "trust" {
		connParams.Set("password", p.pgReplPassword)
	}
	return connParams
}

// createPGParameters merges mandatory and user parameters for current DB role.
func (p *PostgresKeeper) createPGParameters(
	db *cluster.DB,
) common.Parameters {
	parameters := common.Parameters{}

	// Include init parameters if include config is required
	dbLocalState := p.dbLocalStateCopy()
	if db.Spec.IncludeConfig {
		maps.Copy(parameters, dbLocalState.InitPGParameters)
	}

	// Copy user defined pg parameters
	maps.Copy(parameters, db.Spec.PGParameters)

	// Add/Replace mandatory PGParameters
	maps.Copy(parameters, p.mandatoryPGParameters(db))
	enforceHotStandbyFeedbackForLogicalSlotFailover(
		parameters,
		db.Spec.EnableLogicalSlotFailover,
	)

	parameters["listen_addresses"] = p.pgListenAddress

	parameters["port"] = p.pgPort
	desiredMaxReplSlots := int(db.Spec.MaxStandbys)
	if current, ok := p.currentPGParameterInt("max_replication_slots"); ok && current > desiredMaxReplSlots {
		desiredMaxReplSlots = current
	}
	parameters["max_replication_slots"] = strconv.Itoa(desiredMaxReplSlots)

	// Add some more wal senders, since also the keeper will use them
	desiredMaxWalSenders := int((db.Spec.MaxStandbys * 2) + 2 + db.Spec.AdditionalWalSenders)
	if current, ok := p.currentPGParameterInt("max_wal_senders"); ok && current > desiredMaxWalSenders {
		desiredMaxWalSenders = current
	}
	parameters["max_wal_senders"] = strconv.Itoa(desiredMaxWalSenders)

	// required by pg_rewind (if data checksum is enabled it's ignored)
	if db.Spec.UsePgrewind {
		parameters["wal_log_hints"] = "on"
	}

	// Setup synchronous replication
	if db.Spec.SynchronousReplication &&
		(len(db.Spec.SynchronousStandbys) > 0 || len(db.Spec.ExternalSynchronousStandbys) > 0) {
		synchronousStandbys := make(
			[]string,
			0,
			len(
				db.Spec.SynchronousStandbys,
			)+len(
				db.Spec.ExternalSynchronousStandbys,
			),
		)
		for _, synchronousStandby := range db.Spec.SynchronousStandbys {
			synchronousStandbys = append(
				synchronousStandbys,
				common.HysteronName(synchronousStandby),
			)
		}
		synchronousStandbys = append(
			synchronousStandbys,
			db.Spec.ExternalSynchronousStandbys...)

		// We deliberately don't use postgres FIRST or ANY methods with N
		// different than len(synchronousStandbys) because we need that all the
		// defined standbys are synchronous (so just only one failed standby
		// will block the primary).
		// This is needed for consistency. If we have 3 standbys and we use
		// FIRST 2 (a, b, c), the sentinel, when the master fails, won't be able to know
		// which of the 3 standbys is really synchronous and in sync with the
		// master. And choosing the non synchronous one will cause the loss of
		// the transactions contained in the wal records not transmitted.
		if len(synchronousStandbys) > 1 {
			parameters["synchronous_standby_names"] = fmt.Sprintf(
				"%d (%s)",
				len(synchronousStandbys),
				strings.Join(synchronousStandbys, ","),
			)
		} else {
			parameters["synchronous_standby_names"] = strings.Join(synchronousStandbys, ",")
		}
	} else {
		parameters["synchronous_standby_names"] = ""
	}

	return parameters
}

// createRecoveryOptions builds recovery settings for standby/recovery modes.
func (p *PostgresKeeper) createRecoveryOptions(
	recoveryMode postgresql.RecoveryMode,
	standbySettings *cluster.StandbySettings,
	archiveRecoverySettings *cluster.ArchiveRecoverySettings,
	recoveryTargetSettings *cluster.RecoveryTargetSettings,
) *postgresql.RecoveryOptions {
	parameters := common.Parameters{}

	if standbySettings != nil {
		if standbySettings.PrimaryConninfo != "" {
			parameters["primary_conninfo"] = standbySettings.PrimaryConninfo
		}
		if standbySettings.PrimarySlotName != "" {
			parameters["primary_slot_name"] = standbySettings.PrimarySlotName
		}
		if standbySettings.RecoveryMinApplyDelay != "" {
			parameters["recovery_min_apply_delay"] = standbySettings.RecoveryMinApplyDelay
		}
	}

	if archiveRecoverySettings != nil {
		parameters["restore_command"] = archiveRecoverySettings.RestoreCommand
	}

	parameters["recovery_target_timeline"] = "latest"
	if recoveryTargetSettings == nil {
		return &postgresql.RecoveryOptions{
			RecoveryMode:       recoveryMode,
			RecoveryParameters: parameters,
		}
	}

	if recoveryTargetSettings.RecoveryTargetTimeline != "" {
		parameters["recovery_target_timeline"] = recoveryTargetSettings.RecoveryTargetTimeline
	}
	if recoveryTargetSettings.RecoveryTarget != "" {
		parameters["recovery_target"] = recoveryTargetSettings.RecoveryTarget
	}
	if recoveryTargetSettings.RecoveryTargetLsn != "" {
		parameters["recovery_target_lsn"] = recoveryTargetSettings.RecoveryTargetLsn
	}
	if recoveryTargetSettings.RecoveryTargetName != "" {
		parameters["recovery_target_name"] = recoveryTargetSettings.RecoveryTargetName
	}
	if recoveryTargetSettings.RecoveryTargetTime != "" {
		parameters["recovery_target_time"] = recoveryTargetSettings.RecoveryTargetTime
	}
	if recoveryTargetSettings.RecoveryTargetXid != "" {
		parameters["recovery_target_xid"] = recoveryTargetSettings.RecoveryTargetXid
	}
	if hasRecoveryTargetSelector(recoveryTargetSettings) {
		parameters["recovery_target_action"] = "promote"
	}

	return &postgresql.RecoveryOptions{
		RecoveryMode:       recoveryMode,
		RecoveryParameters: parameters,
	}
}

// hasRecoveryTargetSelector reports whether any recovery target selector is set.
func hasRecoveryTargetSelector(recoveryTargetSettings *cluster.RecoveryTargetSettings) bool {
	if recoveryTargetSettings == nil {
		return false
	}

	for _, value := range []string{
		recoveryTargetSettings.RecoveryTarget,
		recoveryTargetSettings.RecoveryTargetLsn,
		recoveryTargetSettings.RecoveryTargetName,
		recoveryTargetSettings.RecoveryTargetTime,
		recoveryTargetSettings.RecoveryTargetXid,
	} {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}

	return false
}
