package server

import (
	"fmt"
	"strconv"
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
	// Larger cardinality.
	for i := 0; i < 1000; i++ {
		cl.do(t, "PFADD", "big", fmt.Sprint("e", i))
	}
	r := cl.do(t, "PFCOUNT", "big")
	if n, err := strconv.Atoi(r.Str); err != nil || n < 970 || n > 1030 {
		t.Fatalf("PFCOUNT big = %+v want ~1000", r)
	}
}
