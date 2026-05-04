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

// Package cmd implements the stolon-proxy command.
package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sorintlab/stolon/cmd"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
	slog "github.com/sorintlab/stolon/internal/log"
	"github.com/sorintlab/stolon/internal/store"
	"github.com/sorintlab/stolon/internal/tcpproxy"

	"github.com/davecgh/go-spew/spew"
	"github.com/woozymasta/flags"
	"go.uber.org/zap"
)

var log = slog.S()

type config struct {
	ListenAddress string `short:"l" long:"listen-address" env:"LISTEN_ADDRESS" default:"127.0.0.1" description:"proxy listening address"`
	Port          string `short:"p" long:"port" env:"PORT" default:"5432" description:"proxy listening port"`
	cmd.CommonConfig

	KeepAlive     tcpKeepAliveOptions `group:"TCP Keep-Alive" namespace:"tcp-keepalive" env-namespace:"TCP_KEEPALIVE"`
	StopListening bool                `long:"stop-listening" env:"STOP_LISTENING" description:"stop listening on store error (default true)"`
}

// tcpKeepAliveOptions tunes TCP keep-alive settings on accepted client
// connections. Long names and env keys are derived from the enclosing
// `tcp-keepalive`/`TCP_KEEPALIVE` namespace.
type tcpKeepAliveOptions struct {
	Idle     int `long:"idle" env:"IDLE" default:"0" validate-min:"0" description:"set tcp keepalive idle (seconds)"`
	Count    int `long:"count" env:"COUNT" default:"0" validate-min:"0" description:"set tcp keepalive probe count number"`
	Interval int `long:"interval" env:"INTERVAL" default:"0" validate-min:"0" description:"set tcp keepalive interval (seconds)"`
}

var cfg = config{StopListening: true}

// ClusterChecker keeps the local TCP proxy aligned with cluster data.
type ClusterChecker struct {
	// External cluster store client.
	e store.Store
	// Active TCP proxy instance (nil when stopped).
	tcpProxy *tcpproxy.Proxy
	// Asynchronous TCP proxy termination/error channel.
	endTCPProxyCh chan error
	// Proxy instance UID.
	uid string
	// Local listen address.
	listenAddress string
	// Local listen port.
	port string
	// Interval between periodic proxy checks.
	proxyCheckInterval time.Duration
	// TTL/liveness timeout advertised for this proxy.
	proxyTimeout time.Duration
	// Guards tcpProxy lifecycle and destination updates.
	tcpProxyMutex sync.Mutex
	// Guards mutable runtime configuration updates.
	configMutex sync.Mutex
	// Stop listener when critical store errors happen.
	stopListening bool
}

// NewClusterChecker creates a ClusterChecker from proxy configuration.
func NewClusterChecker(uid string, cfg config) (*ClusterChecker, error) {
	e, err := cmd.NewStore(&cfg.CommonConfig, true)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}

	return &ClusterChecker{
		uid:           uid,
		listenAddress: cfg.ListenAddress,
		port:          cfg.Port,
		stopListening: cfg.StopListening,
		e:             e,
		endTCPProxyCh: make(chan error),

		proxyCheckInterval: cluster.DefaultProxyCheckInterval,
		proxyTimeout:       cluster.DefaultProxyTimeout,
	}, nil
}

func (c *ClusterChecker) startTCPProxy() error {
	c.tcpProxyMutex.Lock()
	defer c.tcpProxyMutex.Unlock()
	if c.tcpProxy != nil {
		return nil
	}

	log.Infow("Starting proxying")
	listenAddr := net.JoinHostPort(c.listenAddress, c.port)
	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("error resolving tcp addr %q: %v", listenAddr, err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf("error listening on tcp addr %q: %v", addr.String(), err)
	}

	pp := tcpproxy.New(listener, tcpproxy.Options{
		KeepAlive:         true,
		KeepAliveIdle:     time.Duration(cfg.KeepAlive.Idle) * time.Second,
		KeepAliveCount:    cfg.KeepAlive.Count,
		KeepAliveInterval: time.Duration(cfg.KeepAlive.Interval) * time.Second,
	})

	c.tcpProxy = pp
	go func() {
		c.endTCPProxyCh <- pp.Start()
	}()

	return nil
}

func (c *ClusterChecker) stopTCPProxy() {
	c.tcpProxyMutex.Lock()
	defer c.tcpProxyMutex.Unlock()
	if c.tcpProxy != nil {
		log.Infow("Stopping listening")
		c.tcpProxy.Stop()
		c.tcpProxy = nil
	}
}

func (c *ClusterChecker) setProxyDestination(addr *net.TCPAddr) {
	c.tcpProxyMutex.Lock()
	defer c.tcpProxyMutex.Unlock()
	if c.tcpProxy != nil {
		c.tcpProxy.SetDestination(addr)
	}
}

// SetProxyInfo updates this proxy's liveness and generation information.
func (c *ClusterChecker) SetProxyInfo(generation int64, proxyTimeout time.Duration) error {
	proxyInfo := &cluster.ProxyInfo{
		InfoUID:      common.UID(),
		UID:          c.uid,
		Generation:   generation,
		ProxyTimeout: proxyTimeout,
	}
	log.Debugf("proxyInfo dump: %s", spew.Sdump(proxyInfo))

	if err := c.e.SetProxyInfo(context.TODO(), proxyInfo, 2*proxyTimeout); err != nil {
		return err
	}
	return nil
}

// Check reads the cluster data and applies the right proxy configuration.
func (c *ClusterChecker) Check() error {
	cd, _, err := c.e.GetClusterData(context.TODO())
	if err != nil {
		return fmt.Errorf("cannot get cluster data: %v", err)
	}

	// Start proxy if not active.
	if err = c.startTCPProxy(); err != nil {
		return fmt.Errorf("failed to start proxy: %v", err)
	}

	log.Debugf("cd dump: %s", spew.Sdump(cd))
	if cd == nil {
		log.Infow("no clusterdata available, closing connections to master")
		c.setProxyDestination(nil)
		return nil
	}
	if cd.FormatVersion != cluster.CurrentCDFormatVersion {
		c.setProxyDestination(nil)
		return fmt.Errorf("unsupported clusterdata format version: %d", cd.FormatVersion)
	}
	if err = cd.Cluster.Spec.Validate(); err != nil {
		c.setProxyDestination(nil)
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
		log.Infow("no proxy object available, closing connections to master")
		c.setProxyDestination(nil)
		// ignore errors on setting proxy info
		if err = c.SetProxyInfo(cluster.NoGeneration, proxyTimeout); err != nil {
			log.Errorw("failed to update proxyInfo", zap.Error(err))
		} else {
			// update proxyCheckinterval and proxyTimeout only if we successfully updated our proxy info
			c.configMutex.Lock()
			c.proxyCheckInterval = cdProxyCheckInterval
			c.proxyTimeout = cdProxyTimeout
			c.configMutex.Unlock()
		}
		return nil
	}

	db, ok := cd.DBs[proxy.Spec.MasterDBUID]
	if !ok {
		log.Infow("no db object available, closing connections to master", "db", proxy.Spec.MasterDBUID)
		c.setProxyDestination(nil)
		// ignore errors on setting proxy info
		if err = c.SetProxyInfo(proxy.Generation, proxyTimeout); err != nil {
			log.Errorw("failed to update proxyInfo", zap.Error(err))
		} else {
			// update proxyCheckinterval and proxyTimeout only if we successfully updated our proxy info
			c.configMutex.Lock()
			c.proxyCheckInterval = cdProxyCheckInterval
			c.proxyTimeout = cdProxyTimeout
			c.configMutex.Unlock()
		}
		return nil
	}

	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(db.Status.ListenAddress, db.Status.Port))
	if err != nil {
		log.Errorw("cannot resolve db address", zap.Error(err))
		c.setProxyDestination(nil)
		return nil
	}
	log.Infow("master address", "address", addr)
	if err = c.SetProxyInfo(proxy.Generation, proxyTimeout); err != nil {
		// if we failed to update our proxy info when a master is defined we
		// cannot ignore this error since the sentinel won't know that we exist
		// and are sending connections to a master so, when electing a new
		// master, it'll not wait for us to close connections to the old one.
		return fmt.Errorf("failed to update proxyInfo: %v", err)
	}
	// update proxyCheckInterval and proxyTimeout only if we successfully updated our proxy info
	c.configMutex.Lock()
	c.proxyCheckInterval = cdProxyCheckInterval
	c.proxyTimeout = cdProxyTimeout
	c.configMutex.Unlock()

	// start proxing only if we are inside enabledProxies, this ensures that the
	// sentinel has read our proxyinfo and knows we are alive
	if slices.Contains(proxy.Spec.EnabledProxies, c.uid) {
		log.Infow("proxying to master address", "address", addr)
		c.setProxyDestination(addr)
	} else {
		log.Infow("not proxying to master address since we aren't in the enabled proxies list", "address", addr)
		c.setProxyDestination(nil)
	}

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
			log.Infow("check timeout timer fired")
			// if the check timeouts close all connections and stop listening
			// (for example to avoid load balancers forward connections to us
			// since we aren't ready or in a bad state)
			c.setProxyDestination(nil)
			if c.stopListening {
				c.stopTCPProxy()
			}

		case <-checkOkCh:
			log.Debugw("check ok message received")

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

	// TODO(sgotti) TimeoutCecker is needed to forcefully close connection also
	// if the Check method is blocked somewhere.
	// The idiomatic/cleaner solution will be to use a context instead of this
	// TimeoutChecker, but that requires broader store and checker plumbing.
	go c.TimeoutChecker(checkOkCh)

	for {
		select {
		case <-timerCh:
			go func() {
				checkCh <- c.Check()
			}()
		case err := <-checkCh:
			if err != nil {
				// don't report check ok since it returned an error
				log.Infow("check function error", zap.Error(err))
			} else {
				// report that check was ok
				checkOkCh <- struct{}{}
			}
			c.configMutex.Lock()
			timerCh = time.NewTimer(c.proxyCheckInterval).C
			c.configMutex.Unlock()

		case err := <-c.endTCPProxyCh:
			if err != nil {
				return fmt.Errorf("proxy error: %v", err)
			}
		}
	}
}

// Execute runs the stolon-proxy command.
func Execute() {
	parser := NewParser()
	if _, err := parser.Parse(); err != nil {
		os.Exit(cmd.ParseErrorExitCode(err))
	}
	proxy(parser)
}

// NewParser creates a parser for stolon-proxy. Built-in helper
// commands stay available; subcommands are optional because the proxy
// is a daemon.
func NewParser() *flags.Parser {
	parser := cmd.NewParser("stolon-proxy", "STPROXY", &cfg, 0)
	parser.SubcommandsOptional = true
	return parser
}

func proxy(parser *flags.Parser) {
	switch cfg.Log.Level {
	case "error":
		slog.SetLevel(zap.ErrorLevel)
	case "warn":
		slog.SetLevel(zap.WarnLevel)
	case "info":
		slog.SetLevel(zap.InfoLevel)
	case "debug":
		slog.SetLevel(zap.DebugLevel)
	}
	if cfg.Debug {
		slog.SetDebug()
	}
	if cmd.IsLogColorRequested(parser, &cfg.CommonConfig) {
		log = slog.SColor()
	}
	if slog.IsDebug() {
		if cmd.IsLogColorRequested(parser, &cfg.CommonConfig) {
			stdlog := slog.StdLogColor()
			tcpproxy.SetLogger(stdlog)
		} else {
			stdlog := slog.StdLog()
			tcpproxy.SetLogger(stdlog)
		}
	}

	if err := cmd.CheckClusterName(&cfg.CommonConfig); err != nil {
		log.Fatalf(err.Error())
	}
	if err := cmd.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		log.Fatalf(err.Error())
	}

	cmd.SetMetrics(&cfg.CommonConfig, "proxy")

	uid := common.UID()
	log.Infow("proxy uid", "uid", uid)

	if cfg.MetricsListenAddress != "" {
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			server := &http.Server{
				Addr:              cfg.MetricsListenAddress,
				ReadHeaderTimeout: 5 * time.Second,
			}
			err := server.ListenAndServe()
			if err != nil {
				log.Fatalf("metrics http server error", zap.Error(err))
			}
		}()
	}

	clusterChecker, err := NewClusterChecker(uid, cfg)
	if err != nil {
		log.Fatalf("cannot create cluster checker: %v", err)
	}
	if err = clusterChecker.Start(); err != nil {
		log.Fatalf("cluster checker ended with error: %v", err)
	}
}
