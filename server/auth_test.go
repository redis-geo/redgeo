package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
)

// startConfigured starts a server after letting the test configure it.
func startConfigured(t *testing.T, configure func(*Server)) (string, *Server, func()) {
	t.Helper()
	eng, err := engine.New(context.Background(), engine.Config{ReplicaID: "cfg"})
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	srv := New(addr, crdtstore.NewStore(eng))
	configure(srv)
	ready := make(chan error, 1)
	go func() { _ = srv.Start(ready) }()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	return addr, srv, func() { _ = srv.Stop(); _ = eng.Close() }
}

func TestAuth(t *testing.T) {
	addr, _, stop := startConfigured(t, func(s *Server) { s.SetPassword("s3cret") })
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	for _, cmd := range [][]string{{"PING"}, {"GET", "k"}, {"SET", "k", "v"}, {"MULTI"}, {"PUBLISH", "c", "m"}, {"INFO"}, {"EVAL", "return 1", "0"}} {
		if r := cl.do(t, cmd...); r.Kind != '-' || !strings.HasPrefix(r.Str, "NOAUTH") {
			t.Fatalf("%v before AUTH = %+v", cmd, r)
		}
	}
	for _, bad := range [][]string{{"AUTH", "wrong"}, {"AUTH", "default", "wrong"}, {"AUTH", "root", "s3cret"}, {"AUTH", ""}} {
		if r := cl.do(t, bad...); r.Kind != '-' || !strings.HasPrefix(r.Str, "WRONGPASS") {
			t.Fatalf("%v = %+v", bad, r)
		}
	}
	if r := cl.do(t, "PING"); !strings.HasPrefix(r.Str, "NOAUTH") {
		t.Fatalf("failed AUTH let a command through: %+v", r)
	}
	if r := cl.do(t, "AUTH"); r.Kind != '-' {
		t.Fatalf("AUTH without args = %+v", r)
	}
	if r := cl.do(t, "AUTH", "s3cret"); r.Str != "OK" {
		t.Fatalf("AUTH = %+v", r)
	}
	if r := cl.do(t, "SET", "k", "v"); r.Str != "OK" {
		t.Fatalf("SET after AUTH = %+v", r)
	}
	// Authentication is per connection.
	other := dialC(t, addr)
	defer other.close()
	if r := other.do(t, "GET", "k"); !strings.HasPrefix(r.Str, "NOAUTH") {
		t.Fatalf("second connection inherited auth: %+v", r)
	}
	// HELLO can authenticate and negotiate in one step; without creds it refuses.
	if r := other.do(t, "HELLO", "3"); !strings.HasPrefix(r.Str, "NOAUTH") {
		t.Fatalf("HELLO without creds = %+v", r)
	}
	if r := other.do(t, "HELLO", "2", "AUTH", "default", "nope"); !strings.HasPrefix(r.Str, "WRONGPASS") {
		t.Fatalf("HELLO bad creds = %+v", r)
	}
	if r := other.do(t, "HELLO", "2", "AUTH", "default", "s3cret"); r.Kind != '*' {
		t.Fatalf("HELLO AUTH = %+v", r)
	}
	if r := other.do(t, "GET", "k"); r.Str != "v" {
		t.Fatalf("GET after HELLO AUTH = %+v", r)
	}
}

func TestAuthNotConfigured(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()
	if r := cl.do(t, "AUTH", "x"); r.Kind != '-' || !strings.Contains(r.Str, "without any password configured") {
		t.Fatalf("AUTH on an open server = %+v", r)
	}
	if r := cl.do(t, "PING"); r.Str != "PONG" {
		t.Fatalf("PING = %+v", r)
	}
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "redgeo-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestTLS(t *testing.T) {
	cert := selfSigned(t)
	addr, _, stop := startConfigured(t, func(s *Server) {
		s.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		s.SetPassword("pw")
	})
	defer stop()

	pool := x509.NewCertPool()
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	pool.AddCert(leaf)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()
	cl := &client{c: conn, r: bufio.NewReader(conn)}
	if r := cl.do(t, "AUTH", "pw"); r.Str != "OK" {
		t.Fatalf("AUTH over TLS = %+v", r)
	}
	if r := cl.do(t, "SET", "k", "v"); r.Str != "OK" {
		t.Fatalf("SET over TLS = %+v", r)
	}
	// A plaintext client gets nothing usable from the TLS listener.
	plain, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plain.SetDeadline(time.Now().Add(time.Second))
	plain.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, 16)
	if n, _ := plain.Read(buf); strings.HasPrefix(string(buf[:n]), "+PONG") {
		t.Fatal("plaintext PING was served on a TLS listener")
	}
}

func TestMetricsAndInfo(t *testing.T) {
	addr, srv, stop := startConfigured(t, func(*Server) {})
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()
	cl.do(t, "SET", "k", "v")
	cl.do(t, "SET", "k2", "v")
	cl.do(t, "GET", "k")
	cl.do(t, "LPUSH", "k", "x") // wrong type: counted as a failure
	cl.do(t, "NOSUCHCOMMAND")

	rec := httptest.NewRecorder()
	srv.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`redgeo_info{version="`, `redgeo_commands_total{cmd="set"} 2`, `redgeo_commands_total{cmd="get"} 1`,
		`redgeo_command_errors_total{cmd="lpush"} 1`, `redgeo_connections_active 1`, `redgeo_partition_generation{partition="0"} 0`,
		`redgeo_crdt_heads `, `redgeo_command_duration_seconds_sum{cmd="set"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
	info := cl.do(t, "INFO").Str
	for _, want := range []string{"# Commandstats", "cmdstat_set:calls=2,", "failed_calls=1", "connected_clients:1", "partition0:generation=0", "total_commands_processed:"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO missing %q\n%s", want, info)
		}
	}
	// Garbage command names cannot grow the label set without bound.
	for i := 0; i < maxCmdLabels+50; i++ {
		cl.do(t, "NOSUCH"+strings.Repeat("X", i%7)+string(rune('a'+i%26))+strings.Repeat("y", i/26))
	}
	_, _ = srv.h.metrics.snapshot()
	if n := len(srv.h.metrics.cmds); n > maxCmdLabels+1 {
		t.Fatalf("%d metric labels", n)
	}
}
