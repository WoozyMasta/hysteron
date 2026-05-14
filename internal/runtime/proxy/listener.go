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
	"fmt"
	"net"
	"time"

	"github.com/woozymasta/hysteron/internal/tcpproxy"
)

// start initializes and starts the underlying TCP proxy listener once.
func (l *proxyListener) start() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.tcpProxy != nil {
		return nil
	}

	log.Info().
		Str("proxy_mode", string(l.mode)).
		Str("listen_address", l.listenAddress).
		Str("port", l.port).
		Msg("starting proxy listener")
	listenAddr := net.JoinHostPort(l.listenAddress, l.port)
	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("error resolving tcp addr %q: %v", listenAddr, err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf("error listening on tcp addr %q: %v", addr.String(), err)
	}

	pp := tcpproxy.New(listener, tcpproxy.Options{
		KeepAlive: true,
		KeepAliveIdle: time.Duration(
			cfg.KeepAlive.Idle,
		) * time.Second,
		KeepAliveCount: cfg.KeepAlive.Count,
		KeepAliveInterval: time.Duration(
			cfg.KeepAlive.Interval,
		) * time.Second,
		OnActiveConnectionsDelta: func(delta int) {
			activeConnectionsGauge.WithLabelValues(string(l.mode)).Add(float64(delta))
		},
		OnConnectError: func(reason string) {
			connectErrorsTotal.WithLabelValues(string(l.mode), reason).Inc()
		},
	})

	l.tcpProxy = pp
	go func() {
		l.endTCPProxyCh <- pp.Start()
	}()

	return nil
}

// stop shuts down the underlying TCP proxy listener when it is active.
func (l *proxyListener) stop() {
	if l == nil {
		return
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.tcpProxy != nil {
		log.Info().
			Str("proxy_mode", string(l.mode)).
			Msg("stopping proxy listener")
		l.tcpProxy.Stop()
		l.tcpProxy = nil
	}
}

// setDestination updates listener backend set to a single destination.
func (l *proxyListener) setDestination(addr *net.TCPAddr) {
	l.setDestinations([]*net.TCPAddr{addr})
}

// setDestinations updates listener backend destination pool.
func (l *proxyListener) setDestinations(addrs []*net.TCPAddr) {
	if l == nil {
		return
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.tcpProxy != nil {
		l.tcpProxy.SetDestinations(addrs)
	}
}

// isActive reports whether the listener currently has a running tcpproxy.
func (l *proxyListener) isActive() bool {
	if l == nil {
		return false
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.tcpProxy != nil
}
