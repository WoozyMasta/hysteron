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

// Package sentinel runs the sentinel runtime component.
package sentinel

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	lastCheckSuccessSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_sentinel_last_cluster_check_success_seconds",
			Help: "Last time we successfully performed a cluster check as seconds since unix epoch",
		},
		[]string{"cluster_name"},
	)
	isLeaderGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_sentinel_is_leader",
			Help: "Set to 1 if the sentinel is currently a leader",
		},
		[]string{"cluster_name"},
	)
	checkDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hysteron_sentinel_check_duration_seconds",
			Help:    "Duration of sentinel cluster reconciliation checks",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"cluster_name"},
	)
	leaderElectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_sentinel_leader_elections_total",
			Help: "Total number of times this sentinel has been elected as leader",
		},
		[]string{"cluster_name"},
	)
	checkErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_sentinel_check_errors_total",
			Help: "Total number of sentinel reconciliation check errors by stage",
		},
		[]string{"cluster_name", "stage"},
	)
	failoversTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_sentinel_failovers_total",
			Help: "Total number of master transitions decided by sentinel",
		},
		[]string{"cluster_name", "reason"},
	)
	failoverDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hysteron_sentinel_failover_duration_seconds",
			Help:    "Observed duration from master degradation/start marker to master transition",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"cluster_name", "reason"},
	)
)

// Register the static methods on the default Prometheus registry automatically
func init() {
	prometheus.MustRegister(lastCheckSuccessSeconds)
	prometheus.MustRegister(isLeaderGauge)
	prometheus.MustRegister(checkDurationSeconds)
	prometheus.MustRegister(leaderElectionsTotal)
	prometheus.MustRegister(checkErrorsTotal)
	prometheus.MustRegister(failoversTotal)
	prometheus.MustRegister(failoverDurationSeconds)
}
