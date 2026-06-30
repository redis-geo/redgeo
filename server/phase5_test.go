package server

import (
	"strings"
	"testing"
)

func TestSortedSets(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// ZADD returns count of new members.
	if r := cl.do(t, "ZADD", "z", "1", "a", "2", "b", "3", "c"); r.Str != "3" {
		t.Fatalf("ZADD = %+v", r)
	}
	// Re-adding with a new score updates (0 new).
	if r := cl.do(t, "ZADD", "z", "5", "a"); r.Str != "0" {
		t.Fatalf("ZADD update = %+v", r)
	}
	if r := cl.do(t, "ZSCORE", "z", "a"); r.Str != "5" {
		t.Fatalf("ZSCORE a = %+v", r)
	}
	if r := cl.do(t, "ZCARD", "z"); r.Str != "3" {
		t.Fatalf("ZCARD = %+v", r)
	}

	// Order is now b(2), c(3), a(5).
	if got := strs(cl.do(t, "ZRANGE", "z", "0", "-1")); strings.Join(got, ",") != "b,c,a" {
		t.Fatalf("ZRANGE = %v", got)
	}
	if got := strs(cl.do(t, "ZREVRANGE", "z", "0", "-1")); strings.Join(got, ",") != "a,c,b" {
		t.Fatalf("ZREVRANGE = %v", got)
	}
	// WITHSCORES.
	if got := strs(cl.do(t, "ZRANGE", "z", "0", "0", "WITHSCORES")); strings.Join(got, ",") != "b,2" {
		t.Fatalf("ZRANGE WITHSCORES = %v", got)
	}
	// ZRANK (ascending position): b=0, c=1, a=2.
	if r := cl.do(t, "ZRANK", "z", "a"); r.Str != "2" {
		t.Fatalf("ZRANK a = %+v", r)
	}
	if r := cl.do(t, "ZREVRANK", "z", "a"); r.Str != "0" {
		t.Fatalf("ZREVRANK a = %+v", r)
	}

	// ZRANGEBYSCORE.
	if got := strs(cl.do(t, "ZRANGEBYSCORE", "z", "2", "3")); strings.Join(got, ",") != "b,c" {
		t.Fatalf("ZRANGEBYSCORE = %v", got)
	}
	if r := cl.do(t, "ZCOUNT", "z", "2", "5"); r.Str != "3" {
		t.Fatalf("ZCOUNT = %+v", r)
	}

	// ZINCRBY.
	if r := cl.do(t, "ZINCRBY", "z", "10", "b"); r.Str != "12" {
		t.Fatalf("ZINCRBY = %+v", r)
	}

	// ZREM.
	if r := cl.do(t, "ZREM", "z", "c", "nope"); r.Str != "1" {
		t.Fatalf("ZREM = %+v", r)
	}
	if r := cl.do(t, "ZCARD", "z"); r.Str != "2" {
		t.Fatalf("ZCARD after ZREM = %+v", r)
	}

	// ZUNIONSTORE with default SUM aggregate.
	cl.do(t, "ZADD", "z1", "1", "x", "2", "y")
	cl.do(t, "ZADD", "z2", "3", "y", "4", "z")
	if r := cl.do(t, "ZUNIONSTORE", "out", "2", "z1", "z2"); r.Str != "3" {
		t.Fatalf("ZUNIONSTORE = %+v", r)
	}
	// y should be 2+3=5.
	if r := cl.do(t, "ZSCORE", "out", "y"); r.Str != "5" {
		t.Fatalf("ZSCORE out y = %+v (want summed 5)", r)
	}
	// ZINTERSTORE keeps only y.
	if r := cl.do(t, "ZINTERSTORE", "ix", "2", "z1", "z2"); r.Str != "1" {
		t.Fatalf("ZINTERSTORE = %+v", r)
	}
	if got := strs(cl.do(t, "ZRANGE", "ix", "0", "-1")); strings.Join(got, ",") != "y" {
		t.Fatalf("ZINTERSTORE result = %v", got)
	}

	// WRONGTYPE.
	cl.do(t, "SET", "str", "v")
	if r := cl.do(t, "ZADD", "str", "1", "a"); r.Kind != '-' {
		t.Fatalf("ZADD on string = %+v, want error", r)
	}
}
