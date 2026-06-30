package server

import "testing"

// TestRESP3Types verifies that after HELLO 3 the server emits RESP3-typed
// replies (map, double, null), while RESP2 connections still get the legacy
// encodings.
func TestRESP3Types(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// --- RESP3 connection ---
	c3 := dialC(t, addr)
	defer c3.close()
	if r := c3.do(t, "HELLO", "3"); r.Kind != '%' {
		t.Fatalf("HELLO 3 reply kind = %q, want '%%' (map)", r.Kind)
	}

	c3.do(t, "HSET", "h", "f1", "v1", "f2", "v2")
	if r := c3.do(t, "HGETALL", "h"); r.Kind != '%' || len(r.Arr) != 4 {
		t.Fatalf("HGETALL RESP3 = kind %q len %d, want map of 2 pairs", r.Kind, len(r.Arr))
	}

	c3.do(t, "ZADD", "z", "3.5", "m")
	if r := c3.do(t, "ZSCORE", "z", "m"); r.Kind != ',' || r.Str != "3.5" {
		t.Fatalf("ZSCORE RESP3 = kind %q str %q, want ',' double 3.5", r.Kind, r.Str)
	}

	if r := c3.do(t, "GET", "missing"); r.Kind != '_' {
		t.Fatalf("GET missing RESP3 = kind %q, want '_' null", r.Kind)
	}

	// --- RESP2 connection (default) sees legacy encodings ---
	c2 := dialC(t, addr)
	defer c2.close()
	if r := c2.do(t, "HGETALL", "h"); r.Kind != '*' || len(r.Arr) != 4 {
		t.Fatalf("HGETALL RESP2 = kind %q, want '*' array", r.Kind)
	}
	if r := c2.do(t, "ZSCORE", "z", "m"); r.Kind != '$' || r.Str != "3.5" {
		t.Fatalf("ZSCORE RESP2 = kind %q str %q, want bulk string", r.Kind, r.Str)
	}
	if r := c2.do(t, "GET", "missing"); r.Kind != '$' || !r.Null {
		t.Fatalf("GET missing RESP2 = kind %q null %v, want null bulk", r.Kind, r.Null)
	}
}
