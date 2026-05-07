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
	leaderCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_sentinel_leader_count",
			Help: "Number of times this sentinel has been elected as leader",
		},
		[]string{"cluster_name"},
	)
)

// Register the static methods on the default Prometheus registry automatically
func init() {
	prometheus.MustRegister(lastCheckSuccessSeconds)
	prometheus.MustRegister(isLeaderGauge)
	prometheus.MustRegister(leaderCountGauge)
}
