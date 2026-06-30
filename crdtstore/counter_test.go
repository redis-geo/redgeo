package crdtstore

import (
	"context"
	"testing"
)

// TestPNCounterSum demonstrates the §6.4 correctness property: each replica
// writes only its own component, and the global value is the sum — so a remote
// replica's increment composes with the local one instead of clobbering it.
func TestPNCounterSum(t *testing.T) {
	st := newTestStore(t) // replica "r1"
	red := st.Redka(0)
	ctx := context.Background()

	// Local replica increments by 5.
	if v, err := red.Str().Incr("c", 5); err != nil || v != 5 {
		t.Fatalf("INCRBY 5 = %d,%v want 5", v, err)
	}

	// Simulate a *different* replica's component arriving via replication:
	// write its own component key directly. The local replica never touches it.
	if err := st.eng.Put(ctx, counterSlot(0, "c", "r2-remote"), []byte("3")); err != nil {
		t.Fatalf("inject remote component: %v", err)
	}

	// GET now reflects the SUM of both components (5 + 3), not last-writer-wins.
	v, err := red.Str().Get("c")
	if err != nil || v.String() != "8" {
		t.Fatalf("GET after remote component = %q,%v want 8", v, err)
	}

	// A further local increment only changes the local component; total = 10.
	if v, err := red.Str().Incr("c", 2); err != nil || v != 10 {
		t.Fatalf("INCRBY 2 = %d,%v want 10", v, err)
	}
	// Remote component is untouched.
	comps, _ := st.counterComponentsInt(ctx, 0, "c")
	if comps["r1"] != 7 || comps["r2-remote"] != 3 {
		t.Fatalf("components = %v want r1=7 r2-remote=3", comps)
	}
}

// TestCounterStringFlavorRejection verifies counters and plain strings don't
// mix (§6.4): INCR on a plain string errors, and SET on a counter errors.
func TestCounterStringFlavorRejection(t *testing.T) {
	st := newTestStore(t)
	red := st.Redka(0)

	// Plain string then INCR -> reject (even though "10" looks like an int).
	if err := red.Str().Set("s", "10"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := red.Str().Incr("s", 1); err == nil {
		t.Fatal("INCR on a plain string should be rejected")
	}

	// Counter then SET -> reject.
	if _, err := red.Str().Incr("c", 1); err != nil {
		t.Fatalf("incr new counter: %v", err)
	}
	if err := red.Str().Set("c", "x"); err == nil {
		t.Fatal("SET on a counter should be rejected")
	}

	// INCR (int) on a float counter -> reject.
	if _, err := red.Str().IncrFloat("f", 1.5); err != nil {
		t.Fatalf("incrbyfloat new: %v", err)
	}
	if _, err := red.Str().Incr("f", 1); err == nil {
		t.Fatal("INCR on a float counter should be rejected")
	}
}
