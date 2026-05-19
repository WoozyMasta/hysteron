// Copyright 2015 Sorint.lab
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
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/id"

	"github.com/rs/zerolog"
)

// log is the sentinel component logger; refreshed after logging is configured.
var log zerolog.Logger

func init() {
	log = slog.WithComponent("sentinel")
}

const (
	fakeStandbyName = "hysteronfakestandby"

	msgPGTimelineDiffersFromMaster = "ignoring keeper since its pg timeline " +
		"is different than master timeline"
	msgStandbyLagAboveMax = "ignoring keeper since its lag is above the " +
		"max configured lag"
)

type config struct {
	InitialClusterSpecFile string   `short:"f" long:"initial-cluster-spec" env:"INITIAL_CLUSTER_SPEC" description:"a file providing the initial cluster specification, used only at cluster initialization, ignored if cluster is already initialized"`
	ClusterSpecFiles       []string `long:"cluster-spec" env:"CLUSTER_SPEC" description:"per-cluster initial cluster specification override as <cluster-name>=<path>; can be repeated"`
	runtimecommon.CommonConfig
	Web         webOptions                   `group:"Web" namespace:"web" env-namespace:"WEB"`
	KubeService kubeServicePublishingOptions `group:"Kubernetes Service Publishing"`
}

var cfg config

func (s *Sentinel) electionLoop(ctx context.Context) {
	for {
		s.log.Info().Msg("trying to acquire sentinel leadership")
		electedCh, errCh, err := s.election.RunForElection()
		if err != nil {
			s.log.Error().Err(err).Msg("failed to start sentinel election")
			select {
			case <-ctx.Done():
				s.log.Debug().Msg("stopping election loop")
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}
	inner:
		for {
			select {
			case elected := <-electedCh:
				s.leaderMutex.Lock()
				if elected {
					s.log.Info().Msg("sentinel leadership acquired")
					s.leader = true
					s.leadershipCount++
					s.dcsDegradedSeen = false
					isLeaderGauge.WithLabelValues(s.clusterName).Set(1)
					leaderElectionsTotal.WithLabelValues(s.clusterName).Inc()
				} else {
					if s.leader {
						s.log.Info().Msg("sentinel leadership lost")
					}
					s.leader = false
					isLeaderGauge.WithLabelValues(s.clusterName).Set(0)
				}
				s.leaderMutex.Unlock()

			case err := <-errCh:
				if err != nil {
					s.log.Error().Err(err).Msg("sentinel election loop failed")
					if err := s.election.Stop(); err != nil {
						s.log.Error().Err(err).Msg("failed to stop sentinel election")
					}
				}
				break inner
			case <-ctx.Done():
				s.log.Debug().Msg("stopping election loop")
				if err := s.election.Stop(); err != nil {
					s.log.Error().Err(err).Msg("failed to stop sentinel election")
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

// syncRepl return whether to use synchronous replication based on the current
// cluster spec.
func (s *Sentinel) syncRepl(spec *cluster.ClusterSpec) bool {
	// a cluster standby role means our "master" will act as a cascading standby to
	// the other keepers, in this case we can't use synchronous replication
	return *spec.SynchronousReplication &&
		*spec.Role == cluster.ClusterRoleMaster
}

func (s *Sentinel) setSentinelInfo(
	ctx context.Context,
	ttl time.Duration,
) error {
	sentinelInfo := &cluster.SentinelInfo{
		UID: s.uid,
	}
	sentinelInfo.Hostname, sentinelInfo.NodeName = runtimecommon.ResolveHostNodeMetadata()
	s.log.Debug().
		Str("sentinel_uid", sentinelInfo.UID).
		Msg("sentinel registration payload before write to store")

	if err := s.e.SetSentinelInfo(ctx, sentinelInfo, ttl); err != nil {
		return err
	}
	return nil
}

func sortedStringSetKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// NewSentinel creates a sentinel from command configuration.
func NewSentinel(
	uid string,
	cfg *config,
	clusterName string,
	initialClusterSpecFile string,
	end chan bool,
) (*Sentinel, error) {
	logger := slog.WithComponent("sentinel").With().
		Str(slog.FieldClusterName, clusterName).
		Logger()
	initialClusterSpec, err := loadInitialClusterSpecFromFile(
		logger,
		initialClusterSpecFile,
	)
	if err != nil {
		return nil, err
	}

	e, err := runtimecommon.NewStoreForCluster(&cfg.CommonConfig, clusterName, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	election, err := runtimecommon.NewElectionForCluster(&cfg.CommonConfig, clusterName, uid)
	if err != nil {
		return nil, fmt.Errorf("cannot create election: %v", err)
	}
	publisher, err := newKubeServicePublisher(cfg, clusterName, logger)
	if err != nil {
		return nil, err
	}

	return &Sentinel{
		uid:                  uid,
		cfg:                  cfg,
		e:                    e,
		election:             election,
		kubeServicePublisher: publisher,
		clusterName:          clusterName,
		log:                  logger,
		leader:               false,
		initialClusterSpec:   initialClusterSpec,
		end:                  end,
		UIDFn:                id.UID,
		// This is just to choose a pseudo random keeper so
		// use math.rand (no need for crypto.rand) without an
		// initial seed.
		RandFn: rand.Intn,

		sleepInterval:  cluster.DefaultSleepInterval,
		requestTimeout: cluster.DefaultRequestTimeout,

		dbIncreasingXLogPosObservedAt: make(map[string]time.Time),
		leaderRaceBackoffTimers:       make(map[string]time.Time),
	}, nil
}

// Start runs sentinel leader election and cluster reconciliation loops.
func (s *Sentinel) Start(ctx context.Context) {
	endCh := make(chan struct{})

	timerCh := time.NewTimer(0).C

	go s.electionLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			s.log.Info().Msg("stopping hysteron sentinel")
			if s.end != nil {
				s.end <- true
			}
			return

		case <-timerCh:
			go func() {
				s.clusterSentinelCheck(ctx)
				endCh <- struct{}{}
			}()

		case <-endCh:
			timerCh = time.NewTimer(s.sleepInterval).C
		}
	}
}

func (s *Sentinel) leaderInfo() (bool, uint) {
	s.leaderMutex.Lock()
	defer s.leaderMutex.Unlock()
	return s.leader, s.leadershipCount
}

func (s *Sentinel) clusterSentinelCheck(pctx context.Context) {
	start := time.Now()
	defer func() {
		checkDurationSeconds.WithLabelValues(s.clusterName).Observe(time.Since(start).Seconds())
	}()

	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	e := s.e

	cd, prevCDPair, err := e.GetClusterData(pctx)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "get_cluster_data").Inc()
		if !s.dcsDegradedSeen {
			s.log.Warn().Err(err).Msg("detected DCS degraded condition")
		}
		s.dcsDegradedSeen = true
		s.log.Error().Err(err).Msg("error retrieving cluster data")
		return
	}

	if s.dcsDegradedSeen {
		s.log.Info().Msg("DCS connectivity recovered")
	}
	s.dcsDegradedSeen = false
	if cd != nil {
		if cd.FormatVersion != cluster.CurrentCDFormatVersion {
			checkErrorsTotal.WithLabelValues(s.clusterName, "cluster_data_format").Inc()
			s.log.Error().
				Uint64("format_version", cd.FormatVersion).
				Msg("unsupported cluster data format version")
			return
		}

		if err = cd.Cluster.Spec.Validate(); err != nil {
			checkErrorsTotal.WithLabelValues(s.clusterName, "cluster_data_validate").Inc()
			s.log.Error().Err(err).Msg("cluster data validation failed")
			return
		}

		if cd.Cluster != nil {
			s.sleepInterval = cd.Cluster.DefSpec().SleepInterval.Duration
			s.requestTimeout = cd.Cluster.DefSpec().RequestTimeout.Duration
		}
	}

	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data at start of sentinel reconciliation")

	if cd == nil {
		// Cluster first initialization
		if s.initialClusterSpec == nil {
			s.log.Info().
				Msg("no cluster data available, waiting for it to appear")
			return
		}

		c := cluster.NewCluster(s.UIDFn(), s.initialClusterSpec)
		s.log.Info().Msg("writing initial cluster data")
		newcd := cluster.NewClusterData(c)
		s.log.Debug().
			Fields(cluster.LogSummaryClusterData(newcd)).
			Msg("cluster data to persist on first cluster initialization")
		if _, err = e.AtomicPutClusterData(pctx, newcd, nil); err != nil {
			checkErrorsTotal.WithLabelValues(s.clusterName, "init_put_cluster_data").Inc()
			s.log.Error().Err(err).Msg("error saving cluster data")
		}
		return
	}

	if err = s.setSentinelInfo(pctx, 2*s.sleepInterval); err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "set_sentinel_info").Inc()
		s.log.Error().Err(err).Msg("cannot update sentinel info")
		return
	}

	keepersInfo, err := s.e.GetKeepersInfo(pctx)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "get_keepers_info").Inc()
		s.log.Error().Err(err).Msg("cannot get keepers info")
		return
	}
	s.log.Debug().
		Interface("keepers_info", cluster.LogSummaryKeepersInfo(keepersInfo)).
		Msg("keeper info map from store")

	proxiesInfo, err := s.e.GetProxiesInfo(pctx)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "get_proxies_info").Inc()
		s.log.Error().Err(err).Msg("failed to get proxies info")
		return
	}

	isLeader, leadershipCount := s.leaderInfo()
	if !isLeader {
		if slog.IsTrace() {
			s.log.Trace().
				Uint("leadership_epoch", leadershipCount).
				Msg("skipping cluster reconciliation: not sentinel leader")
		}
		return
	}

	// detect if this is the first check after (re)gaining leadership
	firstRun := false
	if s.lastLeadershipCount != leadershipCount {
		firstRun = true
		s.lastLeadershipCount = leadershipCount
	}

	// if this is the first check after (re)gaining leadership reset all
	// the internal timers
	if firstRun {
		s.log.Info().
			Uint("leadership_epoch", leadershipCount).
			Msg("running post-leadership sanity sweep")
		s.runLeadershipSanitySweep(cd)
	}

	newcd, newKeeperInfoHistories, err := s.updateKeepersStatus(
		cd,
		keepersInfo,
		firstRun,
	)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "update_keepers_status").Inc()
		s.log.Error().Err(err).Msg("failed to update keeper status")
		return
	}

	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(newcd)).
		Msg("cluster data after merging keeper health and reported state")

	activeProxiesInfos, err := s.activeProxiesInfos(proxiesInfo)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "active_proxies_info").Inc()
		s.log.Error().Err(err).Msg("failed to compute active proxy info")
		return
	}

	newcd, err = s.updateCluster(newcd, activeProxiesInfos)
	if err != nil {
		checkErrorsTotal.WithLabelValues(s.clusterName, "update_cluster").Inc()
		s.log.Error().Err(err).Msg("failed to update cluster data")
		return
	}

	if newcd != nil && cd != nil {
		prevMasterUID := cd.Cluster.Status.Master
		nextMasterUID := newcd.Cluster.Status.Master
		if prevMasterUID != nextMasterUID {
			reason := "master_changed"
			switch {
			case prevMasterUID == "" && nextMasterUID != "":
				reason = "master_assigned"
			case prevMasterUID != "" && nextMasterUID == "":
				reason = "master_cleared"
			case prevMasterUID != "":
				if prevMaster, ok := cd.DBs[prevMasterUID]; ok && !prevMaster.Status.Healthy {
					reason = "failed_master"
				}
			}
			failoversTotal.WithLabelValues(s.clusterName, reason).Inc()

			var duration time.Duration
			if prevMasterUID != "" {
				if ts, ok := s.dbErrorTimers[prevMasterUID]; ok && !ts.IsZero() {
					duration = time.Since(ts)
				} else if prevMaster, ok := cd.DBs[prevMasterUID]; ok &&
					!prevMaster.ChangeTime.IsZero() {
					duration = time.Since(prevMaster.ChangeTime)
				}
			}
			if duration > 0 {
				failoverDurationSeconds.WithLabelValues(s.clusterName, reason).
					Observe(duration.Seconds())
			}
		}
	}

	s.log.Debug().
		Fields(cluster.LogSummaryClusterData(newcd)).
		Msg("cluster data after sentinel failover and convergence logic")

	if newcd != nil {
		s.updateChangeTimes(cd, newcd)
		if _, err := e.AtomicPutClusterData(pctx, newcd, prevCDPair); err != nil {
			checkErrorsTotal.WithLabelValues(s.clusterName, "put_cluster_data").Inc()
			s.log.Error().Err(err).Msg("error saving cluster data")
			return
		}
	}

	if s.kubeServicePublisher != nil {
		if err := s.kubeServicePublisher.Publish(pctx, newcd); err != nil {
			checkErrorsTotal.WithLabelValues(s.clusterName, "kube_service_publish").Inc()
			s.log.Error().Err(err).Msg("failed to publish Kubernetes Services")
			return
		}
	}

	// Save the new keeperInfoHistories only on successful cluster data
	// update or in the next run we'll think that the saved keeperInfo was
	// already applied.
	s.keeperInfoHistories = newKeeperInfoHistories

	// Update db convergence timers using the new cluster data
	s.updateDBConvergenceInfos(newcd)

	// We only update this metric when we've completed all actions in this method
	// successfully. That enables us to alert on when Sentinels are failing to
	// correctly sync.
	s.lastCheckSuccessUnixNano.Store(time.Now().UnixNano())
	lastCheckSuccessSeconds.WithLabelValues(s.clusterName).SetToCurrentTime()
}
