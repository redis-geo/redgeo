package server

import "testing"

// TestExecReadYourWrites verifies that sequential commands inside one EXEC see
// each other's effects (the txn overlay) and that all writes land atomically.
func TestExecReadYourWrites(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	cl.do(t, "MULTI")
	cl.do(t, "INCR", "c") // 1
	cl.do(t, "INCR", "c") // 2 — must see the first increment within the txn
	cl.do(t, "INCR", "c") // 3
	cl.do(t, "RPUSH", "l", "a")
	cl.do(t, "RPUSH", "l", "b")
	cl.do(t, "LLEN", "l") // 2 — must see both pushes within the txn
	r := cl.do(t, "EXEC")
	if len(r.Arr) != 6 {
		t.Fatalf("EXEC arr len = %d, want 6: %+v", len(r.Arr), r)
	}
	if r.Arr[0].Str != "1" || r.Arr[1].Str != "2" || r.Arr[2].Str != "3" {
		t.Fatalf("INCR read-your-writes = %v,%v,%v want 1,2,3", r.Arr[0].Str, r.Arr[1].Str, r.Arr[2].Str)
	}
	if r.Arr[5].Str != "2" {
		t.Fatalf("LLEN within txn = %v, want 2", r.Arr[5].Str)
	}

	// After commit, all effects are durably visible.
	if g := cl.do(t, "GET", "c"); g.Str != "3" {
		t.Fatalf("GET c after EXEC = %+v, want 3", g)
	}
	if g := cl.do(t, "LLEN", "l"); g.Str != "2" {
		t.Fatalf("LLEN after EXEC = %+v, want 2", g)
	}
}
