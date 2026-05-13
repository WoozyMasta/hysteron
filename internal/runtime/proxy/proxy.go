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

// Package proxy runs the proxy runtime component.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/woozymasta/hysteron/internal/cluster"
	"github.com/woozymasta/hysteron/internal/common"
	stconfig "github.com/woozymasta/hysteron/internal/config"
	slog "github.com/woozymasta/hysteron/internal/log"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/store"
	"github.com/woozymasta/hysteron/internal/tcpproxy"
	"github.com/woozymasta/hysteron/internal/utils/id"
	readonly "github.com/woozymasta/hysteron/internal/utils/readonly"
	units "github.com/woozymasta/hysteron/internal/utils/units"

	"github.com/rs/zerolog"
	"github.com/woozymasta/flags"
)

// log is the proxy component logger; refreshed after logging is configured.
var log zerolog.Logger

func init() {
	log = slog.WithComponent("proxy")
}

type config struct {
	Writable writableOptions `group:"Writable Proxy"`
	ReadOnly readOnlyOptions `group:"Read-Only Proxy" namespace:"read-only" env-namespace:"READ_ONLY"`
	runtimecommon.CommonConfig

	KeepAlive     tcpKeepAliveOptions `group:"TCP Keep-Alive" namespace:"tcp-keepalive" env-namespace:"TCP_KEEPALIVE"`
	StopListening bool                `long:"stop-listening" env:"STOP_LISTENING" description:"stop listening on store error (default true)"`
}

type readOnlyOptions struct {
	ListenAddress   string                   `long:"listen-address"   env:"LISTEN_ADDRESS"   description:"read-only proxy listening address"`
	Port            string                   `long:"port"             env:"PORT"             description:"read-only proxy listening port"`
	ReplicaPriority readonly.ReplicaPriority `long:"replica-priority" env:"REPLICA_PRIORITY" description:"read-only replica priority policy"                                             default:"sync" choices:"sync;async;any"`
	MaxLag          units.BytesValue         `long:"max-lag"          env:"MAX_LAG"          description:"maximum standby WAL lag in bytes for read-only routing"                        default:"0"`
	NoFallback      bool                     `long:"no-fallback"      env:"NO_FALLBACK"      description:"do not route read-only connections to primary when no eligible standby exists" xor:"read-only-primary-policy"`
	IncludePrimary  bool                     `long:"include-primary"  env:"INCLUDE_PRIMARY"  description:"include primary in the normal read-only backend pool"                          xor:"read-only-primary-policy"`
}

type writableOptions struct {
	ListenAddress   string `short:"l" long:"listen-address"            env:"LISTEN_ADDRESS"            default:"127.0.0.1" description:"proxy listening address"`
	Port            string `short:"p" long:"port"                      env:"PORT"                      default:"5432"      description:"proxy listening port"`
	DisableListener bool   `long:"disable-writable-listener" env:"DISABLE_WRITABLE_LISTENER" description:"disable the writable proxy listener"`
}

type proxyMode string

const (
	proxyModeWritable proxyMode = "writable"
	proxyModeReadOnly proxyMode = "read-only"
)

type proxyDestination struct {
	addr  *net.TCPAddr
	dbUID string
	lag   uint64
}

type proxyListener struct {
	tcpProxy      *tcpproxy.Proxy
	endTCPProxyCh chan error
	mode          proxyMode
	listenAddress string
	port          string
	mutex         sync.Mutex
}

// tcpKeepAliveOptions tunes TCP keep-alive settings on accepted client
// connections. Long names and env keys are derived from the enclosing
// `tcp-keepalive`/`TCP_KEEPALIVE` namespace.
type tcpKeepAliveOptions struct {
	Idle     int `long:"idle"     env:"IDLE"     default:"0" validate-min:"0" description:"set tcp keepalive idle (seconds)"`
	Count    int `long:"count"    env:"COUNT"    default:"0" validate-min:"0" description:"set tcp keepalive probe count number"`
	Interval int `long:"interval" env:"INTERVAL" default:"0" validate-min:"0" description:"set tcp keepalive interval (seconds)"`
}

var cfg = config{StopListening: true}

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
	cfg config,
) (*ClusterChecker, error) {
	writableEnabled, readOnlyEnabled, err := validateProxyListeners(cfg)
	if err != nil {
		return nil, err
	}

	e, err := runtimecommon.NewStore(&cfg.CommonConfig, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	cc := &ClusterChecker{
		uid:                uid,
		readOnlyOptions:    cfg.ReadOnly,
		stopListening:      cfg.StopListening,
		e:                  e,
		proxyCheckInterval: cluster.DefaultProxyCheckInterval,
		proxyTimeout:       cluster.DefaultProxyTimeout,
	}
	if writableEnabled {
		cc.writable = &proxyListener{
			mode:          proxyModeWritable,
			listenAddress: cfg.Writable.ListenAddress,
			port:          cfg.Writable.Port,
			endTCPProxyCh: make(chan error),
		}
	}
	if readOnlyEnabled {
		listenAddress := cfg.ReadOnly.ListenAddress
		if listenAddress == "" {
			listenAddress = cfg.Writable.ListenAddress
		}
		cc.readOnly = &proxyListener{
			mode:          proxyModeReadOnly,
			listenAddress: listenAddress,
			port:          cfg.ReadOnly.Port,
			endTCPProxyCh: make(chan error),
		}
	}
	return cc, nil
}

func validateProxyListeners(cfg config) (writableEnabled, readOnlyEnabled bool, err error) {
	writableEnabled = !cfg.Writable.DisableListener
	readOnlyEnabled = cfg.ReadOnly.Port != ""
	if !writableEnabled && !readOnlyEnabled {
		return false, false, errors.New("at least one proxy listener must be enabled")
	}
	if !writableEnabled {
		return false, readOnlyEnabled, nil
	}
	if !readOnlyEnabled {
		return writableEnabled, false, nil
	}

	readOnlyListenAddress := cfg.ReadOnly.ListenAddress
	if readOnlyListenAddress == "" {
		readOnlyListenAddress = cfg.Writable.ListenAddress
	}
	if cfg.Writable.ListenAddress == readOnlyListenAddress && cfg.Writable.Port == cfg.ReadOnly.Port {
		return false, false, errors.New("writable and read-only proxy listeners cannot use the same address and port")
	}
	return writableEnabled, readOnlyEnabled, nil
}

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
		return fmt.Errorf(
			"error resolving tcp addr %q: %v",
			listenAddr,
			err,
		)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf(
			"error listening on tcp addr %q: %v",
			addr.String(),
			err,
		)
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

func (l *proxyListener) setDestination(addr *net.TCPAddr) {
	l.setDestinations([]*net.TCPAddr{addr})
}

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

// SetProxyInfo updates this proxy's liveness and generation information.
func (c *ClusterChecker) SetProxyInfo(
	generation int64,
	proxyTimeout time.Duration,
) error {
	proxyInfo := &cluster.ProxyInfo{
		InfoUID:      id.UID(),
		UID:          c.uid,
		Generation:   generation,
		ProxyTimeout: proxyTimeout,
	}
	log.Debug().
		Fields(cluster.LogSummaryProxyInfo(proxyInfo)).
		Msg("proxy registration payload before write to store")

	if err := c.e.SetProxyInfo(context.TODO(), proxyInfo, 2*proxyTimeout); err != nil {
		return err
	}
	return nil
}

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
		return fmt.Errorf(
			"unsupported clusterdata format version: %d",
			cd.FormatVersion,
		)
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

func (c *ClusterChecker) updateRuntimeConfig(proxyCheckInterval, proxyTimeout time.Duration) {
	c.configMutex.Lock()
	defer c.configMutex.Unlock()
	c.proxyCheckInterval = proxyCheckInterval
	c.proxyTimeout = proxyTimeout
}

func (c *ClusterChecker) clearDestinations() {
	c.setWritableDestination(nil)
	c.setReadOnlyDestinations(nil)
}

func (c *ClusterChecker) setWritableDestination(addr *net.TCPAddr) {
	if c.writable != nil {
		next := ""
		if addr != nil {
			next = addr.String()
		}
		if c.lastWritableDestination != next {
			backendSwitchesTotal.WithLabelValues(string(proxyModeWritable)).Inc()
			c.lastWritableDestination = next
		}
		if addr == nil {
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "enabled").Set(0)
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "disabled").Set(1)
		} else {
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "enabled").Set(1)
			routeStateGauge.WithLabelValues(string(proxyModeWritable), "disabled").Set(0)
		}
		c.writable.setDestination(addr)
	}
}

func (c *ClusterChecker) setReadOnlyDestinations(addrs []*net.TCPAddr) {
	if c.readOnly != nil {
		keys := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			if addr != nil {
				keys = append(keys, addr.String())
			}
		}
		sort.Strings(keys)
		next := strings.Join(keys, ",")
		if c.lastReadOnlyDestinations != next {
			backendSwitchesTotal.WithLabelValues(string(proxyModeReadOnly)).Inc()
			c.lastReadOnlyDestinations = next
		}
		readOnlyDestinationsGauge.Set(float64(len(keys)))
		if len(keys) == 0 {
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "enabled").Set(0)
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "disabled").Set(1)
		} else {
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "enabled").Set(1)
			routeStateGauge.WithLabelValues(string(proxyModeReadOnly), "disabled").Set(0)
		}
		c.readOnly.setDestinations(addrs)
	}
}

func (c *ClusterChecker) readOnlyDestinations(cd *cluster.ClusterData, primary *cluster.DB) []*net.TCPAddr {
	if c.readOnly == nil || primary == nil {
		return nil
	}

	syncStandbys, asyncStandbys := c.readOnlyStandbyCandidates(cd, primary)
	selected := readonly.SelectPriority(c.readOnlyOptions.ReplicaPriority, syncStandbys, asyncStandbys)
	if c.readOnlyOptions.IncludePrimary {
		if primaryDest, ok := readOnlyDestinationFromDB(primary, 0); ok {
			selected = append(selected, primaryDest)
			log.Debug().
				Str(slog.FieldDBUID, primary.UID).
				Msg("including primary in read-only proxy destination pool")
		}
	}
	if len(selected) == 0 && !c.readOnlyOptions.NoFallback {
		if primaryDest, ok := readOnlyDestinationFromDB(primary, 0); ok {
			log.Info().
				Str(slog.FieldDBUID, primary.UID).
				Uint64("max_lag", uint64(c.readOnlyOptions.MaxLag)).
				Msg("read-only proxy falling back to primary")
			readOnlyFallbacksTotal.Inc()
			selected = append(selected, primaryDest)
		}
	}

	addrs := make([]*net.TCPAddr, 0, len(selected))
	for _, dest := range selected {
		addrs = append(addrs, dest.addr)
	}
	return addrs
}

func (c *ClusterChecker) readOnlyStandbyCandidates(cd *cluster.ClusterData, primary *cluster.DB) ([]proxyDestination, []proxyDestination) {
	syncStandbySet := map[string]struct{}{}
	for _, dbUID := range primary.Status.SynchronousStandbys {
		syncStandbySet[dbUID] = struct{}{}
	}

	dbUIDs := make([]string, 0, len(cd.DBs))
	for dbUID := range cd.DBs {
		dbUIDs = append(dbUIDs, dbUID)
	}
	sort.Strings(dbUIDs)

	var syncStandbys []proxyDestination
	var asyncStandbys []proxyDestination
	for _, dbUID := range dbUIDs {
		db := cd.DBs[dbUID]
		if db == nil || db.UID == primary.UID || db.Spec == nil {
			continue
		}
		if db.Spec.Role != common.RoleStandby || !readonly.DBStatusEligible(db) {
			continue
		}

		lag := readonly.XLogLag(primary.Status.XLogPos, db.Status.XLogPos)
		if lag > uint64(c.readOnlyOptions.MaxLag) {
			continue
		}
		dest, ok := readOnlyDestinationFromDB(db, lag)
		if !ok {
			continue
		}
		if _, ok := syncStandbySet[db.UID]; ok {
			syncStandbys = append(syncStandbys, dest)
		} else {
			asyncStandbys = append(asyncStandbys, dest)
		}
	}
	return syncStandbys, asyncStandbys
}

func readOnlyDestinationFromDB(db *cluster.DB, lag uint64) (proxyDestination, bool) {
	addr, err := net.ResolveTCPAddr(
		"tcp",
		net.JoinHostPort(db.Status.ListenAddress, db.Status.Port),
	)
	if err != nil {
		log.Error().
			Err(err).
			Str(slog.FieldDBUID, db.UID).
			Msg("cannot resolve read-only db address")
		return proxyDestination{}, false
	}
	return proxyDestination{
		dbUID: db.UID,
		addr:  addr,
		lag:   lag,
	}, true
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

// newParser creates a parser for runtime proxy options. Built-in helper
// commands stay available; subcommands are optional because the proxy
// is a daemon.
func newParser() *flags.Parser {
	parser := runtimecommon.NewParser("hysteron proxy", "HYSTERON", &cfg, 0)
	parser.SubcommandsOptional = true
	return parser
}

// Run starts proxy with externally prepared common config and optional
// proxy-specific CLI arguments.
func Run(commonConfig stconfig.CommonConfig, args []string) error {
	cfg.CommonConfig = runtimecommon.FromConfigCommon(commonConfig)
	parser := newParser()
	if _, err := parser.ParseArgs(args); err != nil {
		return err
	}
	if parser.Active != nil {
		return nil
	}
	return runProxy()
}

func runProxy() error {
	closer, err := runtimecommon.InitLogging(&cfg.CommonConfig)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	log = slog.WithComponent("proxy")
	tcpproxy.SetLogger(log)
	defer runtimecommon.CloseLogging(closer, &log)

	if err := runtimecommon.CheckClusterName(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid cluster name: %w", err)
	}
	if err := runtimecommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		return fmt.Errorf("invalid common configuration: %w", err)
	}

	runtimecommon.SetMetrics(&cfg.CommonConfig, "proxy")

	uid := id.UID()
	log.Info().Str(slog.FieldProxyUID, uid).Msg("proxy UID assigned")

	if cfg.Metrics.ListenAddress != "" {
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			server := &http.Server{
				Addr:              cfg.Metrics.ListenAddress,
				ReadHeaderTimeout: 5 * time.Second,
			}
			err := server.ListenAndServe()
			if err != nil {
				log.Fatal().
					Err(err).
					Str("addr", cfg.Metrics.ListenAddress).
					Msg("metrics HTTP server failed")
			}
		}()
	}

	clusterChecker, err := NewClusterChecker(uid, cfg)
	if err != nil {
		return fmt.Errorf("failed to create cluster checker: %w", err)
	}
	if err = clusterChecker.Start(); err != nil {
		return fmt.Errorf("cluster checker stopped: %w", err)
	}
	return nil
}
