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

package proxy

import (
	"context"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	slog "github.com/woozymasta/hysteron/internal/log"
)

// Check reads the cluster data and applies the right proxy configuration.
func (c *ClusterChecker) Check() error {
	start := time.Now()
	defer func() {
		checkDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	cd, _, err := c.e.GetClusterData(context.TODO())
	if err != nil {
		checkErrorsTotal.WithLabelValues("get_cluster_data").Inc()
		return fmt.Errorf("cannot get cluster data: %v", err)
	}

	if c.writable != nil {
		if err = c.writable.start(); err != nil {
			checkErrorsTotal.WithLabelValues("start_writable_listener").Inc()
			return fmt.Errorf("failed to start writable proxy: %v", err)
		}
	}
	if c.readOnly != nil {
		if err = c.readOnly.start(); err != nil {
			checkErrorsTotal.WithLabelValues("start_read_only_listener").Inc()
			return fmt.Errorf("failed to start read-only proxy: %v", err)
		}
	}

	log.Debug().
		Fields(cluster.LogSummaryClusterData(cd)).
		Msg("cluster data snapshot after store read")
	if cd == nil {
		log.Info().
			Msg("no clusterdata available, closing connections to master")
		c.clearDestinations()
		return nil
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		checkErrorsTotal.WithLabelValues("unsupported_clusterdata_format").Inc()
		c.clearDestinations()
		return fmt.Errorf("unsupported clusterdata format version: %d", cd.FormatVersion)
	}
	if err = cd.Cluster.Spec.Validate(); err != nil {
		checkErrorsTotal.WithLabelValues("invalid_cluster_spec").Inc()
		c.clearDestinations()
		return fmt.Errorf("clusterdata validation failed: %v", err)
	}

	cdProxyCheckInterval := cd.Cluster.DefSpec().ProxyCheckInterval.Duration
	cdProxyTimeout := cd.Cluster.DefSpec().ProxyTimeout.Duration

	// use the greater between the current proxy timeout and the one defined in the cluster spec if they're different.
	// in this way we're updating our proxyInfo using a timeout that is greater or equal the current active timeout timer.
	c.configMutex.Lock()
	proxyTimeout := max(c.proxyTimeout, cdProxyTimeout)
	c.configMutex.Unlock()

	proxy := cd.Proxy
	if proxy == nil {
		log.Info().
			Msg("no proxy object available, closing connections to master")
		c.clearDestinations()
		if c.writable != nil {
			// ignore errors on setting proxy info
			if err = c.SetProxyInfo(cluster.NoGeneration, proxyTimeout); err != nil {
				log.Error().Err(err).Msg("failed to update proxyInfo")
				return nil
			}
		}
		c.updateRuntimeConfig(cdProxyCheckInterval, cdProxyTimeout)
		return nil
	}

	db, ok := cd.DBs[proxy.Spec.MasterDBUID]
	if !ok {
		log.Info().
			Str(slog.FieldDBUID, proxy.Spec.MasterDBUID).
			Msg("no db object for master uid, closing connections to master")
		c.clearDestinations()
		if c.writable != nil {
			// ignore errors on setting proxy info
			if err = c.SetProxyInfo(proxy.Generation, proxyTimeout); err != nil {
				log.Error().Err(err).Msg("failed to update proxyInfo")
				return nil
			}
		}
		c.updateRuntimeConfig(cdProxyCheckInterval, cdProxyTimeout)
		return nil
	}

	addr, err := net.ResolveTCPAddr(
		"tcp",
		net.JoinHostPort(db.Status.ListenAddress, db.Status.Port),
	)
	if err != nil {
		checkErrorsTotal.WithLabelValues("resolve_master_address").Inc()
		log.Error().Err(err).Msg("cannot resolve db address")
		c.clearDestinations()
		return nil
	}
	log.Info().
		Str("tcp_addr", addr.String()).
		Msg("resolved current master address")
	if c.writable != nil {
		if err = c.SetProxyInfo(proxy.Generation, proxyTimeout); err != nil {
			checkErrorsTotal.WithLabelValues("set_proxy_info").Inc()
			// if we failed to update our proxy info when a master is defined we
			// cannot ignore this error since the sentinel won't know that we exist
			// and are sending connections to a master so, when electing a new
			// master, it'll not wait for us to close connections to the old one.
			return fmt.Errorf("failed to update proxyInfo: %v", err)
		}
	}
	c.updateRuntimeConfig(cdProxyCheckInterval, cdProxyTimeout)

	// start proxing only if we are inside enabledProxies, this ensures that the
	// sentinel has read our proxyinfo and knows we are alive
	if slices.Contains(proxy.Spec.EnabledProxies, c.uid) {
		log.Info().
			Str("tcp_addr", addr.String()).
			Msg("proxying connections to current master")
		c.setWritableDestination(addr)
	} else {
		log.Info().
			Str("tcp_addr", addr.String()).
			Msg("not proxying because this proxy is not enabled in cluster data")
		c.setWritableDestination(nil)
	}
	c.setReadOnlyDestinations(c.readOnlyDestinations(cd, db))

	return nil
}

// TimeoutChecker closes proxy connections when cluster checks stop succeeding.
func (c *ClusterChecker) TimeoutChecker(checkOkCh chan struct{}) {
	c.configMutex.Lock()
	timeoutTimer := time.NewTimer(c.proxyTimeout)
	c.configMutex.Unlock()

	for {
		select {
		case <-timeoutTimer.C:
			checkErrorsTotal.WithLabelValues("check_timeout").Inc()
			log.Info().Msg("check timeout timer fired")
			// if the check timeouts close all connections and stop listening
			// (for example to avoid load balancers forward connections to us
			// since we aren't ready or in a bad state)
			c.clearDestinations()
			if c.stopListening {
				c.writable.stop()
				c.readOnly.stop()
			}

		case <-checkOkCh:
			log.Debug().Msg("check ok message received")

			// ignore if stop succeeded or not due to timer already expired
			timeoutTimer.Stop()

			c.configMutex.Lock()
			timeoutTimer = time.NewTimer(c.proxyTimeout)
			c.configMutex.Unlock()
		}
	}
}

// Start runs the cluster checker loop.
func (c *ClusterChecker) Start() error {
	checkOkCh := make(chan struct{})
	checkCh := make(chan error)
	timerCh := time.NewTimer(0).C
	var writableEndCh <-chan error
	if c.writable != nil {
		writableEndCh = c.writable.endTCPProxyCh
	}
	var readOnlyEndCh <-chan error
	if c.readOnly != nil {
		readOnlyEndCh = c.readOnly.endTCPProxyCh
	}

	// TimeoutChecker force-closes destinations if Check blocks or stops
	// succeeding. A future context-driven checker flow could replace this
	// watchdog once store/check plumbing is fully context-aware.
	go c.TimeoutChecker(checkOkCh)

	for {
		select {
		case <-timerCh:
			go func() {
				checkCh <- c.Check()
			}()
		case err := <-checkCh:
			if err != nil {
				checkErrorsTotal.WithLabelValues("check_failed").Inc()
				// don't report check ok since it returned an error
				log.Error().
					Err(err).
					Msg("cluster check failed")
			} else {
				// report that check was ok
				checkOkCh <- struct{}{}
			}
			c.configMutex.Lock()
			timerCh = time.NewTimer(c.proxyCheckInterval).C
			c.configMutex.Unlock()

		case err := <-writableEndCh:
			if err != nil {
				checkErrorsTotal.WithLabelValues("writable_proxy_runtime").Inc()
				return fmt.Errorf("writable proxy error: %v", err)
			}
		case err := <-readOnlyEndCh:
			if err != nil {
				checkErrorsTotal.WithLabelValues("read_only_proxy_runtime").Inc()
				return fmt.Errorf("read-only proxy error: %v", err)
			}
		}
	}
}
