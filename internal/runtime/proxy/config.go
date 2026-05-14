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
	"errors"
	"net"
	"sync"

	"github.com/woozymasta/hysteron/internal/tcpproxy"
	"github.com/woozymasta/hysteron/internal/utils/readonly"
	"github.com/woozymasta/hysteron/internal/utils/units"
)

// readOnlyOptions controls the optional read-only proxy listener and routing.
type readOnlyOptions struct {
	ListenAddress   string                   `long:"listen-address" env:"LISTEN_ADDRESS" description:"read-only proxy listening address"`
	Port            string                   `long:"port" env:"PORT" description:"read-only proxy listening port"`
	ReplicaPriority readonly.ReplicaPriority `long:"replica-priority" env:"REPLICA_PRIORITY" description:"read-only replica priority policy" default:"sync" choices:"sync;async;any"`
	MaxLag          units.BytesValue         `long:"max-lag" env:"MAX_LAG" description:"maximum standby WAL lag in bytes for read-only routing" default:"0"`
	NoFallback      bool                     `long:"no-fallback" env:"NO_FALLBACK" description:"do not route read-only connections to primary when no eligible standby exists" xor:"read-only-primary-policy"`
	IncludePrimary  bool                     `long:"include-primary" env:"INCLUDE_PRIMARY" description:"include primary in the normal read-only backend pool" xor:"read-only-primary-policy"`
}

// writableOptions controls the writable proxy listener.
type writableOptions struct {
	ListenAddress   string `short:"l" long:"listen-address" env:"LISTEN_ADDRESS" default:"127.0.0.1" description:"proxy listening address"`
	Port            string `short:"p" long:"port" env:"PORT" default:"5432" description:"proxy listening port"`
	DisableListener bool   `long:"disable-writable-listener" env:"DISABLE_WRITABLE_LISTENER" description:"disable the writable proxy listener"`
}

// proxyMode identifies listener mode for routing and metrics labels.
type proxyMode string

const (
	proxyModeWritable proxyMode = "writable"
	proxyModeReadOnly proxyMode = "read-only"
)

// proxyDestination describes one candidate backend destination.
type proxyDestination struct {
	addr  *net.TCPAddr // Resolved backend TCP address.
	dbUID string       // Cluster DB UID associated with this destination.
	lag   uint64       // Replication lag in bytes relative to current primary.
}

// proxyListener owns one tcpproxy listener instance and its mutable state.
type proxyListener struct {
	tcpProxy      *tcpproxy.Proxy // Active listener runtime (nil when stopped).
	endTCPProxyCh chan error      // Async terminal errors from tcpproxy.Start.
	mode          proxyMode       // Listener role: writable or read-only.
	listenAddress string          // Bind address for the listener.
	port          string          // Bind port for the listener.
	mutex         sync.Mutex      // Guards tcpProxy lifecycle and destination updates.
}

// tcpKeepAliveOptions tunes TCP keep-alive settings on accepted client
// connections. Long names and env keys are derived from the enclosing
// `tcp-keepalive`/`TCP_KEEPALIVE` namespace.
type tcpKeepAliveOptions struct {
	Idle     int `long:"idle"     env:"IDLE"     default:"0" validate-min:"0" description:"set tcp keepalive idle (seconds)"`
	Count    int `long:"count"    env:"COUNT"    default:"0" validate-min:"0" description:"set tcp keepalive probe count number"`
	Interval int `long:"interval" env:"INTERVAL" default:"0" validate-min:"0" description:"set tcp keepalive interval (seconds)"`
}

// cfg holds process-global proxy runtime configuration.
var cfg = proxyConfig{StopListening: true}

// validateProxyListeners checks listener topology constraints and returns
// which listeners are effectively enabled.
func validateProxyListeners(runtimeConfig proxyConfig) (writableEnabled, readOnlyEnabled bool, err error) {
	writableEnabled = !runtimeConfig.Writable.DisableListener
	readOnlyEnabled = runtimeConfig.ReadOnly.Port != ""
	if !writableEnabled && !readOnlyEnabled {
		return false, false, errors.New("at least one proxy listener must be enabled")
	}
	if !writableEnabled {
		return false, readOnlyEnabled, nil
	}
	if !readOnlyEnabled {
		return writableEnabled, false, nil
	}

	readOnlyListenAddress := runtimeConfig.ReadOnly.ListenAddress
	if readOnlyListenAddress == "" {
		readOnlyListenAddress = runtimeConfig.Writable.ListenAddress
	}
	if runtimeConfig.Writable.ListenAddress == readOnlyListenAddress && runtimeConfig.Writable.Port == runtimeConfig.ReadOnly.Port {
		return false, false, errors.New("writable and read-only proxy listeners cannot use the same address and port")
	}
	return writableEnabled, readOnlyEnabled, nil
}
