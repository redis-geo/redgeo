package server

import (
	"strings"
	"testing"
)

func TestHelloAndInfo(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// HELLO 3 negotiates RESP3 and returns the handshake map (flat array).
	r := cl.do(t, "HELLO", "3")
	m := map[string]string{}
	for i := 0; i+1 < len(r.Arr); i += 2 {
		m[r.Arr[i].Str] = r.Arr[i+1].Str
	}
	if m["server"] != "redgeo" || m["proto"] != "3" {
		t.Fatalf("HELLO map = %v", m)
	}
	if m["id"] == "" {
		t.Fatalf("HELLO missing replica id: %v", m)
	}
	// Unsupported protocol version errors.
	if r := cl.do(t, "HELLO", "9"); r.Kind != '-' {
		t.Fatalf("HELLO 9 = %+v, want NOPROTO error", r)
	}

	// INFO reports the replication (CRDT) section and keyspace.
	cl.do(t, "SET", "a", "1")
	cl.do(t, "SET", "b", "2")
	info := cl.do(t, "INFO")
	for _, want := range []string{"# Replication", "crdt_heads:", "crdt_max_height:", "replica_id:", "db0:keys=2"} {
		if !strings.Contains(info.Str, want) {
			t.Fatalf("INFO missing %q in:\n%s", want, info.Str)
		}
	}
}
