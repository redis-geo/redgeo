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
