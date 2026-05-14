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
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
)

// boolToFloat64 maps boolean values to 1/0 for gauge updates.
func boolToFloat64(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// setFailsafeState updates in-memory failsafe state and transition metrics.
func (p *PostgresKeeper) setFailsafeState(state failsafeState) {
	if p.failsafeState == state {
		return
	}

	prev := p.failsafeState
	p.failsafeState = state
	failsafeStateGauge.WithLabelValues(string(state)).Set(1)
	failsafeStateGauge.WithLabelValues(string(prev)).Set(0)
	if state == failsafeStateActive {
		failsafeEntersTotal.Inc()
	}
	if prev == failsafeStateActive && state != failsafeStateActive {
		failsafeExitsTotal.Inc()
	}
}

// applyFailsafeRuntimeConfig applies failsafe settings from current cluster
// spec to keeper runtime fields.
func (p *PostgresKeeper) applyFailsafeRuntimeConfig(spec *cluster.ClusterSpec) {
	newEnabled := *spec.EnableFailsafeMode
	newProbeInterval := spec.FailsafeProbeInterval.Duration
	newProbeTimeout := spec.FailsafeProbeTimeout.Duration
	newMaxMissingPeers := *spec.FailsafeMaxMissingPeers
	newTTL := spec.FailsafeTTL.Duration

	if p.failsafeEnabled != newEnabled {
		p.baseLog().Info().
			Bool("failsafe_enabled_old", p.failsafeEnabled).
			Bool("failsafe_enabled_new", newEnabled).
			Msg("updating failsafe mode from cluster spec")
		p.failsafeEnabled = newEnabled
	}
	if p.failsafeProbeInterval != newProbeInterval {
		p.baseLog().Info().
			Dur("failsafe_probe_interval_old", p.failsafeProbeInterval).
			Dur("failsafe_probe_interval_new", newProbeInterval).
			Msg("updating failsafe probe interval from cluster spec")
		p.failsafeProbeInterval = newProbeInterval
	}
	if p.failsafeProbeTimeout != newProbeTimeout {
		p.baseLog().Info().
			Dur("failsafe_probe_timeout_old", p.failsafeProbeTimeout).
			Dur("failsafe_probe_timeout_new", newProbeTimeout).
			Msg("updating failsafe probe timeout from cluster spec")
		p.failsafeProbeTimeout = newProbeTimeout
	}
	if p.failsafeMaxMissingPeers != newMaxMissingPeers {
		p.baseLog().Info().
			Uint16("failsafe_max_missing_peers_old", p.failsafeMaxMissingPeers).
			Uint16("failsafe_max_missing_peers_new", newMaxMissingPeers).
			Msg("updating failsafe max missing peers from cluster spec")
		p.failsafeMaxMissingPeers = newMaxMissingPeers
	}
	if p.failsafeTTL != newTTL {
		p.baseLog().Info().
			Dur("failsafe_ttl_old", p.failsafeTTL).
			Dur("failsafe_ttl_new", newTTL).
			Msg("updating failsafe ttl from cluster spec")
		p.failsafeTTL = newTTL
	}

	if p.failsafeEnabled {
		p.setFailsafeState(failsafeStateInactive)
	} else {
		p.setFailsafeState(failsafeStateDisabled)
	}
	failsafeEnabledGauge.Set(boolToFloat64(p.failsafeEnabled))
}

// handleDCSDegraded updates degraded/failsafe state when DCS access fails.
func (p *PostgresKeeper) handleDCSDegraded(now time.Time, cause error) {
	dcsDegradedGauge.Set(1)
	if !p.dcsDegraded {
		p.dcsDegraded = true
		p.dcsDegradedSince = now
		p.baseLog().Warn().
			Err(cause).
			Bool("failsafe_enabled", p.failsafeEnabled).
			Msg("detected DCS degraded condition")
	}

	if !p.failsafeEnabled {
		p.setFailsafeState(failsafeStateDisabled)
		return
	}

	if now.Sub(p.dcsDegradedSince) >= p.failsafeTTL {
		p.setFailsafeState(failsafeStateExpired)
		return
	}

	p.setFailsafeState(failsafeStateActive)
}

// handleDCSRecovered clears degraded markers after DCS access recovers.
func (p *PostgresKeeper) handleDCSRecovered() {
	if !p.dcsDegraded {
		dcsDegradedGauge.Set(0)
		dcsLastSuccessSeconds.SetToCurrentTime()
		return
	}

	p.dcsDegraded = false
	duration := time.Since(p.dcsDegradedSince)
	p.dcsDegradedSince = time.Time{}
	dcsDegradedGauge.Set(0)
	dcsLastSuccessSeconds.SetToCurrentTime()
	p.baseLog().Info().
		Dur("dcs_degraded_duration", duration).
		Bool("failsafe_enabled", p.failsafeEnabled).
		Msg("DCS connectivity recovered")

	if p.failsafeEnabled {
		p.setFailsafeState(failsafeStateInactive)
		return
	}

	p.setFailsafeState(failsafeStateDisabled)
}

// applyRuntimeConfigFromClusterData applies runtime intervals/timeouts and
// failsafe parameters from current cluster data.
func (p *PostgresKeeper) applyRuntimeConfigFromClusterData(cd *cluster.ClusterData) {
	if cd == nil || cd.Cluster == nil {
		return
	}

	spec := cd.Cluster.DefSpec()
	newSleepInterval := spec.SleepInterval.Duration
	newRequestTimeout := spec.RequestTimeout.Duration

	if p.sleepInterval != newSleepInterval {
		p.baseLog().Info().
			Dur("sleep_interval_old", p.sleepInterval).
			Dur("sleep_interval_new", newSleepInterval).
			Msg("updating keeper sleep interval from cluster spec")
		p.sleepInterval = newSleepInterval
	}
	if p.requestTimeout != newRequestTimeout {
		p.baseLog().Info().
			Dur("request_timeout_old", p.requestTimeout).
			Dur("request_timeout_new", newRequestTimeout).
			Msg("updating keeper request timeout from cluster spec")
		p.requestTimeout = newRequestTimeout
		if p.pgm != nil {
			p.pgm.SetRequestTimeout(newRequestTimeout)
		}
	}

	p.applyFailsafeRuntimeConfig(spec)
}
