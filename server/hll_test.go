package server

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestHyperLogLog(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	if r := cl.do(t, "PFADD", "hll", "a", "b", "c"); r.Str != "1" {
		t.Fatalf("PFADD = %+v", r)
	}
	if r := cl.do(t, "PFADD", "hll", "a"); r.Str != "0" {
		t.Fatalf("PFADD dup = %+v", r)
	}
	if r := cl.do(t, "PFCOUNT", "hll"); r.Str != "3" {
		t.Fatalf("PFCOUNT = %+v", r)
	}
	cl.do(t, "PFADD", "hll2", "c", "d")
	if r := cl.do(t, "PFCOUNT", "hll", "hll2"); r.Str != "4" {
		t.Fatalf("PFCOUNT union = %+v", r)
	}
	if r := cl.do(t, "PFMERGE", "dst", "hll", "hll2"); r.Str != "OK" {
		t.Fatalf("PFMERGE = %+v", r)
	}
	if r := cl.do(t, "PFCOUNT", "dst"); r.Str != "4" {
		t.Fatalf("PFCOUNT dst = %+v", r)
	}
	if r := cl.do(t, "TYPE", "hll"); r.Str != "string" {
		t.Fatalf("TYPE = %+v", r)
	}
	// PFADD with no elements creates the key.
	if r := cl.do(t, "PFADD", "empty"); r.Str != "1" {
		t.Fatalf("PFADD empty = %+v", r)
	}
	if r := cl.do(t, "PFCOUNT", "empty"); r.Str != "0" {
		t.Fatalf("PFCOUNT empty = %+v", r)
	}
	// Wrong type.
	cl.do(t, "SET", "s", "x")
	if r := cl.do(t, "PFADD", "s", "a"); r.Kind != '-' {
		t.Fatalf("PFADD on string = %+v", r)
	}
	cl.do(t, "SADD", "set", "x")
	if r := cl.do(t, "PFCOUNT", "set"); r.Kind != '-' {
		t.Fatalf("PFCOUNT on set = %+v", r)
	}
	// The sketch is a Redis-format string: GET/STRLEN see it, SET can restore it.
	blob := cl.do(t, "GET", "hll")
	if len(blob.Str) != 27 || blob.Str[:5] != "HYLL\x01" { // a small sketch is sparse, as in Redis
		t.Fatalf("GET hll = %d bytes %q", len(blob.Str), blob.Str[:5])
	}
	if r := cl.do(t, "STRLEN", "hll"); r.Str != "27" {
		t.Fatalf("STRLEN = %+v", r)
	}
	cl.do(t, "SET", "copy", blob.Str)
	if r := cl.do(t, "PFCOUNT", "copy"); r.Str != "3" {
		t.Fatalf("restored PFCOUNT = %+v", r)
	}
	if r := cl.do(t, "TYPE", "copy"); r.Str != "string" {
		t.Fatalf("restored TYPE = %+v", r)
	}
	// Larger cardinality.
	for i := 0; i < 1000; i++ {
		cl.do(t, "PFADD", "big", fmt.Sprint("e", i))
	}
	r := cl.do(t, "PFCOUNT", "big")
	if n, err := strconv.Atoi(r.Str); err != nil || n < 970 || n > 1030 {
		t.Fatalf("PFCOUNT big = %+v want ~1000", r)
	}
}

func TestPFDebugAndSelfTest(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	if r := cl.do(t, "PFSELFTEST"); r.Str != "OK" {
		t.Fatalf("PFSELFTEST = %+v", r)
	}
	if r := cl.do(t, "PFSELFTEST", "x"); r.Kind != '-' {
		t.Fatalf("PFSELFTEST with args = %+v", r)
	}

	cl.do(t, "PFADD", "h", "a", "b", "c")
	if r := cl.do(t, "PFDEBUG", "ENCODING", "h"); r.Str != "sparse" {
		t.Fatalf("ENCODING = %+v", r)
	}
	r := cl.do(t, "PFDEBUG", "DECODE", "h")
	if r.Kind != '$' || !strings.Contains(r.Str, "v:") || !strings.Contains(r.Str, "z:") && !strings.Contains(r.Str, "Z:") {
		t.Fatalf("DECODE = %+v", r)
	}
	if r := cl.do(t, "PFDEBUG", "TODENSE", "h"); r.Str != "1" {
		t.Fatalf("TODENSE = %+v", r)
	}
	if r := cl.do(t, "PFDEBUG", "TODENSE", "h"); r.Str != "0" {
		t.Fatalf("TODENSE again = %+v", r)
	}
	if r := cl.do(t, "PFDEBUG", "ENCODING", "h"); r.Str != "dense" {
		t.Fatalf("ENCODING after TODENSE = %+v", r)
	}
	if r := cl.do(t, "STRLEN", "h"); r.Str != "12304" {
		t.Fatalf("STRLEN dense = %+v", r)
	}
	if r := cl.do(t, "PFDEBUG", "DECODE", "h"); r.Kind != '-' || !strings.Contains(r.Str, "not sparse") {
		t.Fatalf("DECODE dense = %+v", r)
	}
	if r := cl.do(t, "PFCOUNT", "h"); r.Str != "3" {
		t.Fatalf("PFCOUNT after TODENSE = %+v", r)
	}

	cl.do(t, "PFADD", "g", "x", "y")
	r = cl.do(t, "PFDEBUG", "GETREG", "g")
	if len(r.Arr) != 16384 {
		t.Fatalf("GETREG = %d registers", len(r.Arr))
	}
	nz := 0
	for _, e := range r.Arr {
		if e.Str != "0" {
			nz++
		}
	}
	if nz == 0 || nz > 2 {
		t.Fatalf("%d non-zero registers", nz)
	}
	if r := cl.do(t, "PFDEBUG", "ENCODING", "g"); r.Str != "dense" { // GETREG densifies, as in Redis
		t.Fatalf("ENCODING after GETREG = %+v", r)
	}

	if r := cl.do(t, "PFDEBUG", "ENCODING", "nokey"); r.Kind != '-' || !strings.Contains(r.Str, "does not exist") {
		t.Fatalf("missing key = %+v", r)
	}
	cl.do(t, "SET", "s", "x")
	for _, bad := range [][]string{{"PFDEBUG", "ENCODING", "s"}, {"PFDEBUG", "BOGUS", "h"}, {"PFDEBUG", "ENCODING"}, {"PFDEBUG"}} {
		if r := cl.do(t, bad...); r.Kind != '-' {
			t.Fatalf("%v = %+v want error", bad, r)
		}
	}
	// Restoring a dense dump keeps it dense; a sparse one stays sparse.
	blob := cl.do(t, "GET", "h").Str
	cl.do(t, "SET", "h2", blob)
	if r := cl.do(t, "PFDEBUG", "ENCODING", "h2"); r.Str != "dense" {
		t.Fatalf("restored dense = %+v", r)
	}
	if r := cl.do(t, "GET", "h2"); r.Str != blob {
		t.Fatal("dense value changed across SET/GET")
	}
}
