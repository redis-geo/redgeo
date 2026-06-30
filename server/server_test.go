package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
)

// startTestServer spins up an in-memory single-node server on a random port
// and returns its address and a stop func.
func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	eng, err := engine.New(context.Background(), engine.Config{ReplicaID: "test-replica"})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	store := crdtstore.NewStore(eng)

	// Pick a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := New(addr, store)
	ready := make(chan error, 1)
	go func() { _ = srv.Start(ready) }()
	if err := <-ready; err != nil {
		t.Fatalf("server start: %v", err)
	}
	return addr, func() { _ = srv.Stop(); _ = eng.Close() }
}

// respConn is a tiny RESP client for tests.
type respConn struct {
	c net.Conn
	r *bufio.Reader
}

func dial(t *testing.T, addr string) *respConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &respConn{c: c, r: bufio.NewReader(c)}
}

// cmd sends a command as a RESP array of bulk strings and returns the raw
// first reply line (sans CRLF), reading a bulk body if present.
func (rc *respConn) cmd(t *testing.T, args ...string) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := rc.r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "$") {
		// bulk string: read the body line too
		body, err := rc.r.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return strings.TrimRight(body, "\r\n")
	}
	return line
}

func (rc *respConn) close() { rc.c.Close() }

func TestPingEndToEnd(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	rc := dial(t, addr)
	defer rc.close()

	// redcon's WriteAny renders the string as a bulk reply (matching redka).
	if got := rc.cmd(t, "PING"); got != "PONG" {
		t.Errorf("PING = %q, want PONG", got)
	}
	if got := rc.cmd(t, "PING", "hello"); got != "hello" {
		t.Errorf("PING hello = %q, want hello", got)
	}
	if got := rc.cmd(t, "ECHO", "world"); got != "world" {
		t.Errorf("ECHO world = %q, want world", got)
	}
}

func TestSelectAndUnknown(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	rc := dial(t, addr)
	defer rc.close()

	if got := rc.cmd(t, "SELECT", "3"); got != "+OK" {
		t.Errorf("SELECT 3 = %q, want +OK", got)
	}
	if got := rc.cmd(t, "SELECT", "99"); !strings.Contains(got, "out of range") {
		t.Errorf("SELECT 99 = %q, want out-of-range error", got)
	}
	if got := rc.cmd(t, "NOSUCHCMD"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("NOSUCHCMD = %q, want -ERR", got)
	}
	if got := rc.cmd(t, "COMMAND"); got != "+OK" {
		t.Errorf("COMMAND = %q, want +OK", got)
	}
}
