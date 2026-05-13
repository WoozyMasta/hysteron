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

package store

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	dcsOperationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hysteron_dcs_operation_duration_seconds",
			Help:    "Duration of store (DCS) operations by backend and operation",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"cluster_name", "backend", "operation"},
	)
	dcsOperationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_dcs_operation_errors_total",
			Help: "Total number of store (DCS) operation errors by backend, operation, and code",
		},
		[]string{"cluster_name", "backend", "operation", "code"},
	)
	dcsWatchResetsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hysteron_dcs_watch_resets_total",
			Help: "Total number of DCS watch/session resets by backend and watcher",
		},
		[]string{"cluster_name", "backend", "watcher"},
	)
)

func init() {
	prometheus.MustRegister(dcsOperationDurationSeconds)
	prometheus.MustRegister(dcsOperationErrorsTotal)
	prometheus.MustRegister(dcsWatchResetsTotal)
}

func observeDCSOperation(
	start time.Time,
	clusterName string,
	backend string,
	operation string,
	err error,
) {
	dcsOperationDurationSeconds.WithLabelValues(clusterName, backend, operation).
		Observe(time.Since(start).Seconds())
	if err != nil {
		dcsOperationErrorsTotal.WithLabelValues(clusterName, backend, operation, dcsErrorCode(err)).
			Inc()
	}
}

func dcsErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return "key_not_found"
	case errors.Is(err, ErrKeyModified):
		return "key_modified"
	case errors.Is(err, ErrElectionNoLeader):
		return "election_no_leader"
	default:
		return "other"
	}
}
