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
)

// Logger mimics Go's standard logger. Only Print functions are needed.
type Logger interface {
	Print(...any)
	Printf(string, ...any)
	Println(...any)
}

type nopLogger struct{}

func (l *nopLogger) Print(...any)          {}
func (l *nopLogger) Printf(string, ...any) {}
func (l *nopLogger) Println(...any)        {}

var log Logger = &nopLogger{}

// SetLogger sets the logger used by TCP proxies.
func SetLogger(l Logger) {
	if l == nil {
		log = &nopLogger{}
		return
	}
	log = l
}

// Options configures a Proxy.
type Options struct {
	KeepAlive         bool
	KeepAliveIdle     time.Duration
	KeepAliveCount    int
	KeepAliveInterval time.Duration
}

// Proxy forwards TCP connections from a listener to the current destination.
type Proxy struct {
	listener *net.TCPListener

	mu         sync.Mutex
	destAddr   *net.TCPAddr
	closeConns chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}

	options Options
}

// New creates a TCP proxy around listener.
func New(listener *net.TCPListener, options Options) *Proxy {
	return &Proxy{
		listener:   listener,
		closeConns: make(chan struct{}),
		stopCh:     make(chan struct{}),
		options:    options,
	}
}

// SetDestination changes the TCP destination and closes active connections.
// A nil destination disables proxying for new connections.
func (p *Proxy) SetDestination(addr *net.TCPAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if sameAddr(p.destAddr, addr) {
		return
	}

	close(p.closeConns)
	p.closeConns = make(chan struct{})
	p.destAddr = addr
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
		close(p.closeConns)
		p.closeConns = make(chan struct{})
		p.destAddr = nil
		p.mu.Unlock()
	})
}

func (p *Proxy) proxyConn(src *net.TCPConn) {
	closeConns, destAddr := p.destination()
	defer func() {
		log.Printf("closing source connection: %v", src)
		_ = src.Close()
	}()

	if destAddr == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-closeConns:
			cancel()
		case <-ctx.Done():
		}
	}()

	var dialer net.Dialer
	destConn, err := dialer.DialContext(ctx, "tcp", destAddr.String())
	if err != nil {
		log.Printf("dial destination %s: %v", destAddr, err)
		return
	}

	dst, ok := destConn.(*net.TCPConn)
	if !ok {
		_ = destConn.Close()
		log.Printf("destination connection is not TCP: %T", destConn)
		return
	}
	defer func() {
		log.Printf("closing destination connection: %v", dst)
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
		log.Printf("all io copy goroutines done")
	case <-closeConns:
		log.Printf("closing all connections")
		closeBoth.Do(closeConnections)
		<-done
		<-done
	}
}

func (p *Proxy) destination() (<-chan struct{}, *net.TCPAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeConns, cloneTCPAddr(p.destAddr)
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
		log.Printf("set tcp keepalive config failed, using default keepalive: %v", err)
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
		log.Printf("copy %s to %s: %v", srcName, dstName, err)
	}
	log.Printf("ending. copied %d bytes from %s to %s", n, srcName, dstName)
}

func sameAddr(a, b *net.TCPAddr) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.String() == b.String()
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
