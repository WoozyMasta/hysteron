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

// Package tcpproxy provides a small TCP proxy with a runtime-swappable
// destination.
package tcpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var log zerolog.Logger = zerolog.Nop()

// SetLogger sets the logger used by TCP proxies.
func SetLogger(l zerolog.Logger) {
	log = l
}

// Options configures a Proxy.
type Options struct {
	// OnActiveConnectionsDelta, when set, is called with +1 when a proxied
	// connection becomes active and -1 when it ends.
	OnActiveConnectionsDelta func(delta int)
	// OnConnectError, when set, is called on connection setup failures.
	OnConnectError func(reason string)
	// KeepAliveIdle sets idle duration before the first keepalive probe.
	KeepAliveIdle time.Duration
	// KeepAliveCount sets the number of probes before considering the peer dead.
	KeepAliveCount int
	// KeepAliveInterval sets interval between keepalive probes.
	KeepAliveInterval time.Duration
	// KeepAlive enables TCP keepalive on accepted client connections.
	KeepAlive bool
}

// Proxy forwards TCP connections from a listener to current destinations.
type Proxy struct {
	// listener accepts inbound client connections.
	listener *net.TCPListener
	// destinations are current backend addresses keyed by TCP address string.
	destinations map[string]*destination
	// stopCh signals proxy shutdown.
	stopCh chan struct{}
	// destinationOrder stores backend address keys in round-robin order.
	destinationOrder []string
	// options stores keepalive configuration.
	options Options
	// nextDestination is next index into destinationOrder.
	nextDestination int
	// stopOnce ensures Stop is idempotent.
	stopOnce sync.Once
	// mu guards destination and closeConns swaps.
	mu sync.Mutex
}

// New creates a TCP proxy around listener.
func New(listener *net.TCPListener, options Options) *Proxy {
	return &Proxy{
		listener:     listener,
		destinations: map[string]*destination{},
		stopCh:       make(chan struct{}),
		options:      options,
	}
}

// SetDestination changes the TCP destination and closes active connections.
// A nil destination disables proxying for new connections.
func (p *Proxy) SetDestination(addr *net.TCPAddr) {
	if addr == nil {
		p.SetDestinations(nil)
		return
	}
	p.SetDestinations([]*net.TCPAddr{addr})
}

// SetDestinations changes the destination set. Active connections to removed
// destinations are closed. Active connections to retained destinations keep
// using their selected backend.
func (p *Proxy) SetDestinations(addrs []*net.TCPAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()

	next := map[string]*destination{}
	nextOrder := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		key := addr.String()
		if _, ok := next[key]; ok {
			continue
		}
		if existing, ok := p.destinations[key]; ok {
			next[key] = existing
		} else {
			next[key] = &destination{
				addr:       cloneTCPAddr(addr),
				closeConns: make(chan struct{}),
			}
		}
		nextOrder = append(nextOrder, key)
	}

	if sameDestinationSet(p.destinationOrder, nextOrder) {
		return
	}

	for key, current := range p.destinations {
		if _, ok := next[key]; !ok {
			close(current.closeConns)
		}
	}
	p.destinations = next
	p.destinationOrder = nextOrder
	if len(p.destinationOrder) == 0 {
		p.nextDestination = 0
	} else {
		p.nextDestination %= len(p.destinationOrder)
	}
}

// Start accepts connections until Stop is called or the listener fails.
func (p *Proxy) Start() error {
	for {
		conn, err := p.listener.AcceptTCP()
		if err != nil {
			select {
			case <-p.stopCh:
				return nil
			default:
				return fmt.Errorf("accept tcp connection: %w", err)
			}
		}

		if p.options.KeepAlive {
			if err := p.setupKeepAlive(conn); err != nil {
				_ = conn.Close()
				return fmt.Errorf("set tcp keepalive: %w", err)
			}
		}

		go p.proxyConn(conn)
	}
}

// Stop stops accepting new connections and closes active proxied connections.
func (p *Proxy) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		_ = p.listener.Close()

		p.mu.Lock()
		for _, dest := range p.destinations {
			close(dest.closeConns)
		}
		p.destinations = map[string]*destination{}
		p.destinationOrder = nil
		p.nextDestination = 0
		p.mu.Unlock()
	})
}

func (p *Proxy) proxyConn(src *net.TCPConn) {
	dest := p.destination()
	defer func() {
		log.Debug().
			Str("local_addr", connLocalAddr(src)).
			Str("remote_addr", connRemoteAddr(src)).
			Msg("closing source connection")
		_ = src.Close()
	}()

	if dest == nil {
		if p.options.OnConnectError != nil {
			p.options.OnConnectError("no_destination")
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-dest.closeConns:
			cancel()
		case <-ctx.Done():
		}
	}()

	var dialer net.Dialer
	destConn, err := dialer.DialContext(ctx, "tcp", dest.addr.String())
	if err != nil {
		if p.options.OnConnectError != nil {
			p.options.OnConnectError("dial")
		}
		log.Debug().
			Err(err).
			Stringer("destination_addr", dest.addr).
			Msg("failed to dial proxy destination")
		return
	}

	dst, ok := destConn.(*net.TCPConn)
	if !ok {
		_ = destConn.Close()
		if p.options.OnConnectError != nil {
			p.options.OnConnectError("non_tcp_destination")
		}
		log.Error().
			Str("destination_type", fmt.Sprintf("%T", destConn)).
			Msg("destination connection is not TCP")
		return
	}
	if p.options.OnActiveConnectionsDelta != nil {
		p.options.OnActiveConnectionsDelta(1)
		defer p.options.OnActiveConnectionsDelta(-1)
	}
	defer func() {
		log.Debug().
			Str("local_addr", connLocalAddr(dst)).
			Str("remote_addr", connRemoteAddr(dst)).
			Msg("closing destination connection")
		_ = dst.Close()
	}()

	done := make(chan struct{}, 2)
	var closeBoth sync.Once
	closeConnections := func() {
		_ = src.Close()
		_ = dst.Close()
	}

	go copyAndClose(done, &closeBoth, closeConnections, dst, src, "source", "destination")
	go copyAndClose(done, &closeBoth, closeConnections, src, dst, "destination", "source")

	select {
	case <-done:
		<-done
		log.Debug().Msg("proxy connection copy completed")
	case <-dest.closeConns:
		log.Debug().
			Stringer("destination_addr", dest.addr).
			Msg("closing connections to removed proxy destination")
		closeBoth.Do(closeConnections)
		<-done
		<-done
	}
}

func (p *Proxy) destination() *destination {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.destinationOrder) == 0 {
		return nil
	}
	key := p.destinationOrder[p.nextDestination%len(p.destinationOrder)]
	p.nextDestination = (p.nextDestination + 1) % len(p.destinationOrder)
	return p.destinations[key].clone()
}

type destination struct {
	addr       *net.TCPAddr
	closeConns chan struct{}
}

func (d *destination) clone() *destination {
	return &destination{
		addr:       cloneTCPAddr(d.addr),
		closeConns: d.closeConns,
	}
}

func (p *Proxy) setupKeepAlive(conn *net.TCPConn) error {
	if p.options.KeepAliveIdle == 0 &&
		p.options.KeepAliveInterval == 0 &&
		p.options.KeepAliveCount == 0 {
		return conn.SetKeepAlive(true)
	}

	config := net.KeepAliveConfig{
		Enable:   true,
		Idle:     -1,
		Interval: -1,
		Count:    -1,
	}
	if p.options.KeepAliveIdle > 0 {
		config.Idle = p.options.KeepAliveIdle
	}
	if p.options.KeepAliveInterval > 0 {
		config.Interval = p.options.KeepAliveInterval
	}
	if p.options.KeepAliveCount > 0 {
		config.Count = p.options.KeepAliveCount
	}
	if err := conn.SetKeepAliveConfig(config); err != nil {
		log.Warn().
			Err(err).
			Msg("failed to set TCP keepalive config, using default keepalive")
		return conn.SetKeepAlive(true)
	}
	return nil
}

func copyAndClose(
	done chan<- struct{},
	closeBoth *sync.Once,
	closeConnections func(),
	dst io.Writer,
	src io.Reader,
	srcName string,
	dstName string,
) {
	defer func() {
		closeBoth.Do(closeConnections)
		done <- struct{}{}
	}()

	n, err := io.Copy(dst, src)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		log.Debug().
			Err(err).
			Str("source", srcName).
			Str("destination", dstName).
			Msg("proxy connection copy failed")
	}
	log.Debug().
		Int64("bytes", n).
		Str("source", srcName).
		Str("destination", dstName).
		Msg("proxy connection copy ended")
}

func connLocalAddr(conn net.Conn) string {
	if conn == nil || conn.LocalAddr() == nil {
		return ""
	}
	return conn.LocalAddr().String()
}

func connRemoteAddr(conn net.Conn) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}

func sameDestinationSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneTCPAddr(addr *net.TCPAddr) *net.TCPAddr {
	if addr == nil {
		return nil
	}

	clone := *addr
	if addr.IP != nil {
		clone.IP = append(net.IP(nil), addr.IP...)
	}
	return &clone
}
