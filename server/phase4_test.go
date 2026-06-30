package server

import "testing"

func TestCounters(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	if r := cl.do(t, "INCR", "c"); r.Str != "1" {
		t.Fatalf("INCR = %+v", r)
	}
	if r := cl.do(t, "INCRBY", "c", "10"); r.Str != "11" {
		t.Fatalf("INCRBY = %+v", r)
	}
	if r := cl.do(t, "DECR", "c"); r.Str != "10" {
		t.Fatalf("DECR = %+v", r)
	}
	if r := cl.do(t, "DECRBY", "c", "4"); r.Str != "6" {
		t.Fatalf("DECRBY = %+v", r)
	}
	// GET reflects the counter total.
	if r := cl.do(t, "GET", "c"); r.Str != "6" {
		t.Fatalf("GET counter = %+v", r)
	}

	// INCRBYFLOAT on a fresh key.
	if r := cl.do(t, "INCRBYFLOAT", "f", "3.5"); r.Str != "3.5" {
		t.Fatalf("INCRBYFLOAT = %+v", r)
	}
	if r := cl.do(t, "INCRBYFLOAT", "f", "1.5"); r.Str != "5" {
		t.Fatalf("INCRBYFLOAT = %+v", r)
	}

	// Flavor rejection: SET on a counter, INCR on a plain string.
	if r := cl.do(t, "SET", "c", "x"); r.Kind != '-' {
		t.Fatalf("SET on counter = %+v, want error", r)
	}
	cl.do(t, "SET", "plain", "100")
	if r := cl.do(t, "INCR", "plain"); r.Kind != '-' {
		t.Fatalf("INCR on plain string = %+v, want error", r)
	}

	// HINCRBY.
	if r := cl.do(t, "HINCRBY", "h", "field", "7"); r.Str != "7" {
		t.Fatalf("HINCRBY = %+v", r)
	}
	if r := cl.do(t, "HINCRBY", "h", "field", "3"); r.Str != "10" {
		t.Fatalf("HINCRBY = %+v", r)
	}
}
