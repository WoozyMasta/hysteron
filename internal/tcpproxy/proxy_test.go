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

package tcpproxy

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestProxyForwardsToDestination(t *testing.T) {
	backend := startEchoServer(t, "one")
	proxy, proxyAddr, proxyDone := startProxy(t)
	proxy.SetDestination(backend)

	conn := dialTCP(t, proxyAddr)
	defer conn.Close()

	if got := roundTrip(t, conn, "ping"); got != "one:ping" {
		t.Fatalf("unexpected response: %q", got)
	}

	proxy.Stop()
	waitProxy(t, proxyDone)
}

func TestProxyDropsConnectionsWithoutDestination(t *testing.T) {
	proxy, proxyAddr, proxyDone := startProxy(t)

	conn := dialTCP(t, proxyAddr)
	defer conn.Close()

	if _, err := fmt.Fprintln(conn, "ping"); err != nil {
		t.Fatalf("write to proxy: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := bufio.NewReader(conn).ReadString('\n')
	if err == nil {
		t.Fatal("expected proxy to close connection without destination")
	}

	proxy.Stop()
	waitProxy(t, proxyDone)
}

func TestProxyDestinationChangeClosesActiveConnections(t *testing.T) {
	firstBackend := startEchoServer(t, "one")
	secondBackend := startEchoServer(t, "two")
	proxy, proxyAddr, proxyDone := startProxy(t)
	proxy.SetDestination(firstBackend)

	firstConn := dialTCP(t, proxyAddr)
	defer firstConn.Close()
	if got := roundTrip(t, firstConn, "before"); got != "one:before" {
		t.Fatalf("unexpected first backend response: %q", got)
	}

	proxy.SetDestination(secondBackend)

	_ = firstConn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := bufio.NewReader(firstConn).ReadString('\n')
	if err == nil {
		t.Fatal("expected first connection to close after destination change")
	}

	secondConn := dialTCP(t, proxyAddr)
	defer secondConn.Close()
	if got := roundTrip(t, secondConn, "after"); got != "two:after" {
		t.Fatalf("unexpected second backend response: %q", got)
	}

	proxy.Stop()
	waitProxy(t, proxyDone)
}

func TestProxyStopIsIdempotent(t *testing.T) {
	proxy, proxyAddr, proxyDone := startProxy(t)
	conn := dialTCP(t, proxyAddr)
	defer conn.Close()

	proxy.Stop()
	proxy.Stop()
	waitProxy(t, proxyDone)
}

func startProxy(t *testing.T) (*Proxy, *net.TCPAddr, <-chan error) {
	t.Helper()

	listener, err := net.ListenTCP("tcp", tcpLocalhost(t))
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}

	proxy := New(listener, Options{})
	done := make(chan error, 1)
	go func() {
		done <- proxy.Start()
	}()

	return proxy, listener.Addr().(*net.TCPAddr), done
}

func startEchoServer(t *testing.T, prefix string) *net.TCPAddr {
	t.Helper()

	listener, err := net.ListenTCP("tcp", tcpLocalhost(t))
	if err != nil {
		t.Fatalf("listen backend: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	go func() {
		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				return
			}

			go func(conn *net.TCPConn) {
				defer conn.Close()
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					_, _ = fmt.Fprintf(conn, "%s:%s\n", prefix, scanner.Text())
				}
			}(conn)
		}
	}()

	return listener.Addr().(*net.TCPAddr)
}

func dialTCP(t *testing.T, addr *net.TCPAddr) *net.TCPConn {
	t.Helper()

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

func roundTrip(t *testing.T, conn *net.TCPConn, msg string) string {
	t.Helper()

	if _, err := fmt.Fprintln(conn, msg); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return strings.TrimSpace(line)
}

func waitProxy(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("proxy stopped with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxy to stop")
	}
}

func tcpLocalhost(t *testing.T) *net.TCPAddr {
	t.Helper()

	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve localhost: %v", err)
	}
	return addr
}
