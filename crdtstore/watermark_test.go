package crdtstore

import (
	"context"
	"errors"
	"testing"

	"github.com/redis-geo/redgeo/hlc"
)

func TestWatermarkSafeCut(t *testing.T) {
	// Unknown membership => never safe.
	if _, ok := NewWatermark(nil).SafeCut(); ok {
		t.Fatal("empty expected set must refuse a cut")
	}

	w := NewWatermark([]string{"r0", "r1"})
	if _, ok := w.SafeCut(); ok {
		t.Fatal("no observations yet must refuse")
	}
	w.Observe("r0", hlc.Stamp{WallMS: 100})
	if _, ok := w.SafeCut(); ok {
		t.Fatal("r1 still unknown must refuse")
	}
	w.Observe("r1", hlc.Stamp{WallMS: 50})
	cut, ok := w.SafeCut()
	if !ok || cut.WallMS != 50 {
		t.Fatalf("SafeCut = %v,%v want {50},true (min of high-water)", cut, ok)
	}
	// High-water is monotonic; an older observation doesn't lower the cut.
	w.Observe("r1", hlc.Stamp{WallMS: 40})
	if cut, _ := w.SafeCut(); cut.WallMS != 50 {
		t.Fatalf("cut regressed to %v", cut)
	}
}

func TestCompactorGatedOnWatermark(t *testing.T) {
	st := newTestStore(t) // replica "r1"
	red := st.Redka(0)
	ctx := context.Background()
	if err := red.Str().Set("k", "v"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Watermark expecting an unseen replica => compaction must refuse.
	wmBad := NewWatermark([]string{"r1", "r2-never-seen"})
	if _, err := NewCompactor(st, wmBad).Compact(ctx); !errors.Is(err, ErrNotSafeToCompact) {
		t.Fatalf("compact with incomplete watermark = %v, want ErrNotSafeToCompact", err)
	}

	// Watermark expecting only the local replica => ObserveFromSlots confirms
	// it, a cut is established, and compaction proceeds (snapshot only until
	// PurgeDAG lands).
	wmOK := NewWatermark([]string{"r1"})
	rep, err := NewCompactor(st, wmOK).Compact(ctx)
	if err != nil {
		t.Fatalf("compact with complete watermark: %v", err)
	}
	if rep.LiveKeys != 1 {
		t.Fatalf("LiveKeys = %d, want 1", rep.LiveKeys)
	}
}

// TestCompactionRotationReclaims verifies that compaction rotates the DAG:
// live keys survive, deleted keys' tombstones are dropped (DAG height resets to
// a fresh genesis), and the store keeps working afterward.
func TestCompactionRotationReclaims(t *testing.T) {
	st := newTestStore(t) // replica "r1"
	red := st.Redka(0)
	ctx := context.Background()

	// Create churn: many sets + deletes accumulate tombstones and DAG height.
	for i := 0; i < 30; i++ {
		_ = red.Str().Set("tmp", "v")
		_, _ = red.Key().Delete("tmp")
	}
	_ = red.Str().Set("keep", "alive")
	_ = red.Str().Set("keep2", "alive2")

	before := st.eng.Stats(ctx)
	if before.MaxHeight == 0 {
		t.Fatal("expected non-zero DAG height after churn")
	}

	wm := NewWatermark([]string{"r1"})
	rep, err := NewCompactor(st, wm).Compact(ctx)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !rep.Performed {
		t.Fatal("compaction should have rotated")
	}

	// Live keys survive the rotation.
	if v, err := st.Redka(0).Str().Get("keep"); err != nil || v.String() != "alive" {
		t.Fatalf("keep after rotation = %q,%v want alive", v, err)
	}
	if v, err := st.Redka(0).Str().Get("keep2"); err != nil || v.String() != "alive2" {
		t.Fatalf("keep2 after rotation = %q,%v", v, err)
	}
	// Deleted key stays gone.
	if _, err := st.Redka(0).Str().Get("tmp"); err == nil {
		t.Fatal("tmp should remain absent after rotation")
	}
	// The fresh genesis DAG has far less height than the churned one.
	after := st.eng.Stats(ctx)
	if after.MaxHeight >= before.MaxHeight {
		t.Fatalf("DAG height not reclaimed: before=%d after=%d", before.MaxHeight, after.MaxHeight)
	}

	// The store still accepts writes after rotation.
	if err := st.Redka(0).Str().Set("post", "x"); err != nil {
		t.Fatalf("write after rotation: %v", err)
	}
	if v, _ := st.Redka(0).Str().Get("post"); v.String() != "x" {
		t.Fatal("post-rotation write not readable")
	}
}
