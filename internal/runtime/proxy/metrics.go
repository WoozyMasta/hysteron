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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import "github.com/prometheus/client_golang/prometheus"

var (
	checkDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "hysteron_proxy_check_duration_seconds",
			Help:    "Duration of proxy cluster-check cycle",
			Buckets: prometheus.DefBuckets,
		},
	)
	checkErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_proxy_check_errors_total",
			Help: "Total number of proxy cluster-check errors by stage",
		},
		[]string{"stage"},
	)
	backendSwitchesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_proxy_backend_switches_total",
			Help: "Total number of proxy backend destination switches by mode",
		},
		[]string{"mode"},
	)
	routeStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_proxy_route_state",
			Help: "Proxy route state by mode and state label (1 active state, 0 otherwise)",
		},
		[]string{"mode", "state"},
	)
	readOnlyDestinationsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hysteron_proxy_read_only_destinations",
			Help: "Current number of active read-only proxy destinations",
		},
	)
	readOnlyFallbacksTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "hysteron_proxy_read_only_fallbacks_total",
			Help: "Total number of read-only routing fallbacks to primary",
		},
	)
	activeConnectionsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hysteron_proxy_active_connections",
			Help: "Current number of active proxied client connections by mode",
		},
		[]string{"mode"},
	)
	connectErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_proxy_connect_errors_total",
			Help: "Total proxy destination connect/setup errors by mode and reason",
		},
		[]string{"mode", "reason"},
	)
)

func init() {
	prometheus.MustRegister(checkDurationSeconds)
	prometheus.MustRegister(checkErrorsTotal)
	prometheus.MustRegister(backendSwitchesTotal)
	prometheus.MustRegister(routeStateGauge)
	prometheus.MustRegister(readOnlyDestinationsGauge)
	prometheus.MustRegister(readOnlyFallbacksTotal)
	prometheus.MustRegister(activeConnectionsGauge)
	prometheus.MustRegister(connectErrorsTotal)

	routeStateGauge.WithLabelValues(string(proxyModeWritable), "enabled").Set(0)
	routeStateGauge.WithLabelValues(string(proxyModeWritable), "disabled").Set(1)
	routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "enabled").Set(0)
	routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "disabled").Set(1)
	activeConnectionsGauge.WithLabelValues(string(proxyModeWritable)).Set(0)
	activeConnectionsGauge.WithLabelValues(string(proxyModeReadOnly)).Set(0)
}
