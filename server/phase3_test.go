package server

import (
	"strconv"
	"testing"
)

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("atoi %q: %v", s, err)
	}
	return n
}

func TestTTLCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	cl.do(t, "SET", "k", "v")
	// No TTL yet.
	if r := cl.do(t, "TTL", "k"); r.Str != "-1" {
		t.Fatalf("TTL no-expiry = %+v, want -1", r)
	}
	// Missing key.
	if r := cl.do(t, "TTL", "missing"); r.Str != "-2" {
		t.Fatalf("TTL missing = %+v, want -2", r)
	}
	// EXPIRE then TTL.
	if r := cl.do(t, "EXPIRE", "k", "100"); r.Str != "1" {
		t.Fatalf("EXPIRE = %+v", r)
	}
	if ttl := atoi(t, cl.do(t, "TTL", "k").Str); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL = %d, want ~100", ttl)
	}
	if pttl := atoi(t, cl.do(t, "PTTL", "k").Str); pttl < 90000 || pttl > 100000 {
		t.Fatalf("PTTL = %d, want ~100000", pttl)
	}
	// PERSIST clears it.
	if r := cl.do(t, "PERSIST", "k"); r.Str != "1" {
		t.Fatalf("PERSIST = %+v", r)
	}
	if r := cl.do(t, "TTL", "k"); r.Str != "-1" {
		t.Fatalf("TTL after PERSIST = %+v, want -1", r)
	}
	// EXPIRE on a missing key returns 0.
	if r := cl.do(t, "EXPIRE", "missing", "10"); r.Str != "0" {
		t.Fatalf("EXPIRE missing = %+v, want 0", r)
	}
	// SETEX sets value + TTL.
	if r := cl.do(t, "SETEX", "sx", "50", "hello"); r.Str != "OK" {
		t.Fatalf("SETEX = %+v", r)
	}
	if r := cl.do(t, "GET", "sx"); r.Str != "hello" {
		t.Fatalf("GET sx = %+v", r)
	}
	if ttl := atoi(t, cl.do(t, "TTL", "sx").Str); ttl < 40 || ttl > 50 {
		t.Fatalf("TTL sx = %d, want ~50", ttl)
	}
}
