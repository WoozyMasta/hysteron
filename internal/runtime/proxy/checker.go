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
	"sync"
	"time"

	"github.com/woozymasta/hysteron/internal/cluster"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/utils/id"
)

// ClusterChecker keeps the local TCP proxy aligned with cluster data.
type ClusterChecker struct {
	// External cluster store client.
	e store.Store
	// Writable TCP proxy listener.
	writable *proxyListener
	// Optional read-only TCP proxy listener.
	readOnly *proxyListener
	// Proxy instance UID.
	uid string
	// Last writable destination string for switch detection.
	lastWritableDestination string
	// Last read-only destination signature for switch detection.
	lastReadOnlyDestinations string
	// Read-only routing options.
	readOnlyOptions readOnlyOptions
	// Interval between periodic proxy checks.
	proxyCheckInterval time.Duration
	// TTL/liveness timeout advertised for this proxy.
	proxyTimeout time.Duration
	// Guards mutable runtime configuration updates.
	configMutex sync.Mutex
	// Stop listener when critical store errors happen.
	stopListening bool
}

// NewClusterChecker creates a ClusterChecker from proxy configuration.
func NewClusterChecker(
	uid string,
	runtimeConfig proxyConfig,
) (*ClusterChecker, error) {
	writableEnabled, readOnlyEnabled, err := validateProxyListeners(runtimeConfig)
	if err != nil {
		return nil, err
	}

	clusterStore, err := runtimecommon.NewStore(&runtimeConfig.CommonConfig, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	clusterChecker := &ClusterChecker{
		uid:                uid,
		readOnlyOptions:    runtimeConfig.ReadOnly,
		stopListening:      runtimeConfig.StopListening,
		e:                  clusterStore,
		proxyCheckInterval: cluster.DefaultProxyCheckInterval,
		proxyTimeout:       cluster.DefaultProxyTimeout,
	}
	if writableEnabled {
		clusterChecker.writable = &proxyListener{
			mode:          proxyModeWritable,
			listenAddress: runtimeConfig.Writable.ListenAddress,
			port:          runtimeConfig.Writable.Port,
			endTCPProxyCh: make(chan error),
		}
	}
	if readOnlyEnabled {
		listenAddress := runtimeConfig.ReadOnly.ListenAddress
		if listenAddress == "" {
			listenAddress = runtimeConfig.Writable.ListenAddress
		}
		clusterChecker.readOnly = &proxyListener{
			mode:          proxyModeReadOnly,
			listenAddress: listenAddress,
			port:          runtimeConfig.ReadOnly.Port,
			endTCPProxyCh: make(chan error),
		}
	}
	return clusterChecker, nil
}

// SetProxyInfo updates this proxy's liveness and generation information.
func (c *ClusterChecker) SetProxyInfo(
	ctx context.Context,
	generation int64,
	proxyTimeout time.Duration,
) error {
	listeners := make([]cluster.ProxyListenerInfo, 0, 2)
	if c.writable != nil {
		listeners = append(listeners, cluster.ProxyListenerInfo{
			Mode:    string(proxyModeWritable),
			Address: c.writable.listenAddress,
			Port:    c.writable.port,
			Active:  c.writable.isActive(),
		})
	}
	if c.readOnly != nil {
		listeners = append(listeners, cluster.ProxyListenerInfo{
			Mode:    string(proxyModeReadOnly),
			Address: c.readOnly.listenAddress,
			Port:    c.readOnly.port,
			Active:  c.readOnly.isActive(),
		})
	}
	proxyInfo := &cluster.ProxyInfo{
		InfoUID:      id.UID(),
		UID:          c.uid,
		Generation:   generation,
		ProxyTimeout: proxyTimeout,
		Listeners:    listeners,
	}
	proxyInfo.Hostname, proxyInfo.NodeName = runtimecommon.ResolveHostNodeMetadata()
	log.Debug().
		Fields(cluster.LogSummaryProxyInfo(proxyInfo)).
		Msg("proxy registration payload before write to store")

	if err := c.e.SetProxyInfo(ctx, proxyInfo, 2*proxyTimeout); err != nil {
		return err
	}
	return nil
}

// updateRuntimeConfig applies check interval/timeout values from cluster spec.
func (c *ClusterChecker) updateRuntimeConfig(proxyCheckInterval, proxyTimeout time.Duration) {
	c.configMutex.Lock()
	defer c.configMutex.Unlock()
	c.proxyCheckInterval = proxyCheckInterval
	c.proxyTimeout = proxyTimeout
}

// runtimeConfigSnapshot returns current periodic check interval and timeout.
func (c *ClusterChecker) runtimeConfigSnapshot() (time.Duration, time.Duration) {
	c.configMutex.Lock()
	defer c.configMutex.Unlock()
	return c.proxyCheckInterval, c.proxyTimeout
}

// clearDestinations disables both writable and read-only routing destinations.
func (c *ClusterChecker) clearDestinations() {
	c.setWritableDestination(nil)
	c.setReadOnlyDestinations(nil)
}
