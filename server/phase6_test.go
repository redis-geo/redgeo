package server

import (
	"strings"
	"testing"
)

func TestLists(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// RPUSH appends; LPUSH prepends.
	if r := cl.do(t, "RPUSH", "l", "b", "c", "d"); r.Str != "3" {
		t.Fatalf("RPUSH = %+v", r)
	}
	if r := cl.do(t, "LPUSH", "l", "a"); r.Str != "4" {
		t.Fatalf("LPUSH = %+v", r)
	}
	if r := cl.do(t, "LLEN", "l"); r.Str != "4" {
		t.Fatalf("LLEN = %+v", r)
	}
	// Order is a,b,c,d.
	if got := strs(cl.do(t, "LRANGE", "l", "0", "-1")); strings.Join(got, ",") != "a,b,c,d" {
		t.Fatalf("LRANGE = %v", got)
	}
	if r := cl.do(t, "LINDEX", "l", "0"); r.Str != "a" {
		t.Fatalf("LINDEX 0 = %+v", r)
	}
	if r := cl.do(t, "LINDEX", "l", "-1"); r.Str != "d" {
		t.Fatalf("LINDEX -1 = %+v", r)
	}

	// LPOP / RPOP.
	if r := cl.do(t, "LPOP", "l"); r.Str != "a" {
		t.Fatalf("LPOP = %+v", r)
	}
	if r := cl.do(t, "RPOP", "l"); r.Str != "d" {
		t.Fatalf("RPOP = %+v", r)
	}
	// Now b,c.
	if got := strs(cl.do(t, "LRANGE", "l", "0", "-1")); strings.Join(got, ",") != "b,c" {
		t.Fatalf("LRANGE after pops = %v", got)
	}

	// LINSERT before c -> b, x, c.
	if r := cl.do(t, "LINSERT", "l", "BEFORE", "c", "x"); r.Str != "3" {
		t.Fatalf("LINSERT = %+v", r)
	}
	if got := strs(cl.do(t, "LRANGE", "l", "0", "-1")); strings.Join(got, ",") != "b,x,c" {
		t.Fatalf("LRANGE after LINSERT = %v", got)
	}

	// LSET.
	if r := cl.do(t, "LSET", "l", "1", "Y"); r.Str != "OK" {
		t.Fatalf("LSET = %+v", r)
	}
	if got := strs(cl.do(t, "LRANGE", "l", "0", "-1")); strings.Join(got, ",") != "b,Y,c" {
		t.Fatalf("LRANGE after LSET = %v", got)
	}

	// LREM (remove all matching).
	cl.do(t, "RPUSH", "dups", "a", "b", "a", "c", "a")
	if r := cl.do(t, "LREM", "dups", "0", "a"); r.Str != "3" {
		t.Fatalf("LREM = %+v", r)
	}
	if got := strs(cl.do(t, "LRANGE", "dups", "0", "-1")); strings.Join(got, ",") != "b,c" {
		t.Fatalf("LRANGE after LREM = %v", got)
	}

	// LTRIM.
	cl.do(t, "RPUSH", "tr", "0", "1", "2", "3", "4")
	if r := cl.do(t, "LTRIM", "tr", "1", "3"); r.Str != "OK" {
		t.Fatalf("LTRIM = %+v", r)
	}
	if got := strs(cl.do(t, "LRANGE", "tr", "0", "-1")); strings.Join(got, ",") != "1,2,3" {
		t.Fatalf("LRANGE after LTRIM = %v", got)
	}

	// RPOPLPUSH.
	cl.do(t, "RPUSH", "src", "1", "2", "3")
	if r := cl.do(t, "RPOPLPUSH", "src", "dst"); r.Str != "3" {
		t.Fatalf("RPOPLPUSH = %+v", r)
	}
	if got := strs(cl.do(t, "LRANGE", "dst", "0", "-1")); strings.Join(got, ",") != "3" {
		t.Fatalf("dst after RPOPLPUSH = %v", got)
	}

	// WRONGTYPE.
	cl.do(t, "SET", "s", "v")
	if r := cl.do(t, "LPUSH", "s", "x"); r.Kind != '-' {
		t.Fatalf("LPUSH on string = %+v, want error", r)
	}
}
