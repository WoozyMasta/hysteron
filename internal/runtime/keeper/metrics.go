// Copyright 2019 Sorint.lab
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/woozymasta/hysteron/internal/common"
)

var (
	clusterdataLastValidUpdateSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_clusterdata_last_valid_update_seconds",
			Help: "Last time we received a valid clusterdata from our store as seconds since unix epoch",
		},
	)
	targetRoleGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_target_role",
			Help: "Keeper last requested target role",
		},
		[]string{"role"},
	)
	localRoleGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_local_role",
			Help: "Keeper current local role",
		},
		[]string{"role"},
	)
	needsReloadGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_needs_reload",
			Help: "Set to 1 if Postgres requires reload",
		},
	)
	needsRestartGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_needs_restart",
			Help: "Set to 1 if Postgres requires restart",
		},
	)
	pgRunningGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_running",
			Help: "Set to 1 when local PostgreSQL instance is observed healthy/running by keeper",
		},
	)
	pgInRecoveryGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_in_recovery",
			Help: "Set to 1 when local PostgreSQL instance is in recovery role (standby)",
		},
	)
	pgTimelineGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_timeline",
			Help: "Current local PostgreSQL timeline ID observed by keeper",
		},
	)
	pgServerVersionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_server_version",
			Help: "PostgreSQL binary version encoded as major*10000+minor",
		},
	)
	pgPendingRestartGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_pending_restart",
			Help: "Set to 1 when PostgreSQL reports pending restart-required parameters",
		},
	)
	pgStreamingGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_pg_streaming",
			Help: "Set to 1 when PostgreSQL standby appears configured to stream from upstream",
		},
	)
	lastSyncSuccessSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_last_sync_success_seconds",
			Help: "Last time we successfully synced our keeper",
		},
	)
	reconcileDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hysteron_keeper_reconcile_duration_seconds",
			Help:    "Duration of keeper reconcile phases",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"phase"},
	)
	reconcileErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_reconcile_errors_total",
			Help: "Total number of keeper reconcile errors by phase and reason",
		},
		[]string{"phase", "reason"},
	)
	dcsDegradedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_dcs_degraded",
			Help: "Set to 1 when keeper cannot read cluster data from DCS, 0 when recovered",
		},
	)
	dcsLastSuccessSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_dcs_last_success_seconds",
			Help: "Last time keeper successfully read cluster data from DCS as unix epoch seconds",
		},
	)
	sleepInterval = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_sleep_interval",
			Help: "Seconds to sleep between sync loops",
		},
	)
	shutdownSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_shutdown_seconds",
			Help: "Shutdown time (received termination signal) since unix epoch in seconds",
		},
	)
	logicalSlotStandbyAdvanceAttemptsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_attempts_total",
			Help: "Total logical slot standby advance attempts",
		},
	)
	logicalSlotStandbyAdvanceSuccessTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_success_total",
			Help: "Total successful logical slot standby advance operations",
		},
	)
	logicalSlotStandbyAdvanceFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_failures_total",
			Help: "Total failed logical slot standby advance operations",
		},
	)
	logicalSlotStandbyAdvanceActiveConflictsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_active_conflicts_total",
			Help: "Total standby logical-slot advance attempts blocked because slot is active (SQLSTATE 55006)",
		},
	)
	logicalSlotStandbyAdvanceSkippedBackoffTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_skipped_backoff_total",
			Help: "Total logical slot standby advance operations skipped due to retry backoff",
		},
	)
	logicalSlotStandbyAdvanceRetrySlots = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_retry_slots",
			Help: "Current number of logical slot standby advance operations waiting for retry backoff",
		},
	)
	logicalSlotStandbyAdvancePendingSlots = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_logical_slot_standby_advance_pending_slots",
			Help: "Current number of pending logical slot standby advance operations in async queue",
		},
	)
	failsafeEnabledGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_failsafe_enabled",
			Help: "Set to 1 when failsafe mode is enabled in cluster spec",
		},
	)
	failsafeStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_keeper_failsafe_state",
			Help: "Keeper local failsafe state",
		},
		[]string{"state"},
	)
	failsafeEntersTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_failsafe_enters_total",
			Help: "Total keeper transitions into failsafe active state",
		},
	)
	failsafeExitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_keeper_failsafe_exits_total",
			Help: "Total keeper transitions out of failsafe active state",
		},
	)
)

// setRole is a helper that controls the targetRole metric by setting only one of the
// possible roles to 1 at any one time.
func setRole(rg *prometheus.GaugeVec, role *common.Role) {
	for _, role := range common.Roles {
		rg.WithLabelValues(string(role)).Set(0)
	}

	if role != nil {
		rg.WithLabelValues(string(*role)).Set(1)
	}
}

func init() {
	prometheus.MustRegister(clusterdataLastValidUpdateSeconds)
	prometheus.MustRegister(targetRoleGauge)
	setRole(targetRoleGauge, nil)
	prometheus.MustRegister(localRoleGauge)
	setRole(localRoleGauge, nil)
	prometheus.MustRegister(needsReloadGauge)
	prometheus.MustRegister(needsRestartGauge)
	prometheus.MustRegister(pgRunningGauge)
	prometheus.MustRegister(pgInRecoveryGauge)
	prometheus.MustRegister(pgTimelineGauge)
	prometheus.MustRegister(pgServerVersionGauge)
	prometheus.MustRegister(pgPendingRestartGauge)
	prometheus.MustRegister(pgStreamingGauge)
	prometheus.MustRegister(lastSyncSuccessSeconds)
	prometheus.MustRegister(reconcileDurationSeconds)
	prometheus.MustRegister(reconcileErrorsTotal)
	prometheus.MustRegister(dcsDegradedGauge)
	prometheus.MustRegister(dcsLastSuccessSeconds)
	prometheus.MustRegister(sleepInterval)
	prometheus.MustRegister(shutdownSeconds)
	prometheus.MustRegister(logicalSlotStandbyAdvanceAttemptsTotal)
	prometheus.MustRegister(logicalSlotStandbyAdvanceSuccessTotal)
	prometheus.MustRegister(logicalSlotStandbyAdvanceFailuresTotal)
	prometheus.MustRegister(logicalSlotStandbyAdvanceActiveConflictsTotal)
	prometheus.MustRegister(logicalSlotStandbyAdvanceSkippedBackoffTotal)
	prometheus.MustRegister(logicalSlotStandbyAdvanceRetrySlots)
	prometheus.MustRegister(logicalSlotStandbyAdvancePendingSlots)
	prometheus.MustRegister(failsafeEnabledGauge)
	prometheus.MustRegister(failsafeStateGauge)
	failsafeStateGauge.WithLabelValues("disabled").Set(1)
	failsafeStateGauge.WithLabelValues("inactive").Set(0)
	failsafeStateGauge.WithLabelValues("active").Set(0)
	failsafeStateGauge.WithLabelValues("expired").Set(0)
	prometheus.MustRegister(failsafeEntersTotal)
	prometheus.MustRegister(failsafeExitsTotal)
}
