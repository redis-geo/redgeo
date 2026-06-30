package server

import (
	"sort"
	"strings"
	"testing"
)

// strs flattens an array reply of bulk strings.
func strs(r reply) []string {
	out := make([]string, 0, len(r.Arr))
	for _, e := range r.Arr {
		out = append(out, e.Str)
	}
	return out
}

// sortedStrs returns a sorted copy for order-insensitive comparison.
func sortedStrs(r reply) []string {
	s := strs(r)
	sort.Strings(s)
	return s
}

func TestHashes(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// HSET returns count of new fields.
	if r := cl.do(t, "HSET", "h", "f1", "v1", "f2", "v2"); r.Str != "2" {
		t.Fatalf("HSET = %+v", r)
	}
	if r := cl.do(t, "HSET", "h", "f1", "v1b"); r.Str != "0" { // f1 updated, not new
		t.Fatalf("HSET update = %+v", r)
	}
	if r := cl.do(t, "HGET", "h", "f1"); r.Str != "v1b" {
		t.Fatalf("HGET = %+v", r)
	}
	if r := cl.do(t, "HGET", "h", "nope"); !r.Null {
		t.Fatalf("HGET missing = %+v", r)
	}
	if r := cl.do(t, "HLEN", "h"); r.Str != "2" {
		t.Fatalf("HLEN = %+v", r)
	}
	if r := cl.do(t, "HEXISTS", "h", "f2"); r.Str != "1" {
		t.Fatalf("HEXISTS = %+v", r)
	}

	// HGETALL (order-insensitive)
	got := map[string]string{}
	r := cl.do(t, "HGETALL", "h")
	fv := strs(r)
	for i := 0; i+1 < len(fv); i += 2 {
		got[fv[i]] = fv[i+1]
	}
	if got["f1"] != "v1b" || got["f2"] != "v2" {
		t.Fatalf("HGETALL = %v", got)
	}

	// HDEL
	if r := cl.do(t, "HDEL", "h", "f1", "nope"); r.Str != "1" {
		t.Fatalf("HDEL = %+v", r)
	}
	if r := cl.do(t, "HLEN", "h"); r.Str != "1" {
		t.Fatalf("HLEN after HDEL = %+v", r)
	}
	// Deleting the last field removes the key.
	cl.do(t, "HDEL", "h", "f2")
	if r := cl.do(t, "EXISTS", "h"); r.Str != "0" {
		t.Fatalf("empty hash should not exist: %+v", r)
	}

	// WRONGTYPE: HSET on a string key errors.
	cl.do(t, "SET", "str", "x")
	if r := cl.do(t, "HSET", "str", "f", "v"); r.Kind != '-' {
		t.Fatalf("HSET on string = %+v, want error", r)
	}
}

func TestSetsOrSetSemantics(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	if r := cl.do(t, "SADD", "s", "a", "b", "c", "a"); r.Str != "3" { // a dup not counted
		t.Fatalf("SADD = %+v", r)
	}
	if r := cl.do(t, "SCARD", "s"); r.Str != "3" {
		t.Fatalf("SCARD = %+v", r)
	}
	if r := cl.do(t, "SISMEMBER", "s", "b"); r.Str != "1" {
		t.Fatalf("SISMEMBER = %+v", r)
	}
	if r := cl.do(t, "SISMEMBER", "s", "z"); r.Str != "0" {
		t.Fatalf("SISMEMBER absent = %+v", r)
	}
	if got := sortedStrs(cl.do(t, "SMEMBERS", "s")); strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("SMEMBERS = %v", got)
	}
	if r := cl.do(t, "SREM", "s", "a", "z"); r.Str != "1" {
		t.Fatalf("SREM = %+v", r)
	}

	// Set algebra.
	cl.do(t, "SADD", "s1", "a", "b", "c", "d")
	cl.do(t, "SADD", "s2", "c", "d", "e")
	if got := sortedStrs(cl.do(t, "SINTER", "s1", "s2")); strings.Join(got, ",") != "c,d" {
		t.Fatalf("SINTER = %v", got)
	}
	if got := sortedStrs(cl.do(t, "SDIFF", "s1", "s2")); strings.Join(got, ",") != "a,b" {
		t.Fatalf("SDIFF = %v", got)
	}
	if got := sortedStrs(cl.do(t, "SUNION", "s1", "s2")); strings.Join(got, ",") != "a,b,c,d,e" {
		t.Fatalf("SUNION = %v", got)
	}
	// SUNIONSTORE writes the result.
	if r := cl.do(t, "SUNIONSTORE", "dest", "s1", "s2"); r.Str != "5" {
		t.Fatalf("SUNIONSTORE = %+v", r)
	}
	if r := cl.do(t, "SCARD", "dest"); r.Str != "5" {
		t.Fatalf("SCARD dest = %+v", r)
	}
}
