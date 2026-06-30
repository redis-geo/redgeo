package crdtstore

import (
	"context"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/engine"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	eng, err := engine.New(context.Background(), engine.Config{ReplicaID: "r1"})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return NewStore(eng)
}

func TestLazyExpiryAndSweeper(t *testing.T) {
	// Freeze the clock so expiry is deterministic.
	orig := nowMS
	defer func() { nowMS = orig }()
	cur := int64(1_000_000)
	nowMS = func() int64 { return cur }

	st := newTestStore(t)
	red := st.Redka(0)
	ctx := context.Background()

	// Write a key that expires at t=1_000_100.
	if err := red.Str().Set("k", "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := red.Key().Expire("k", 100*time.Second); err != nil { // etime = cur + 100_000ms
		t.Fatalf("expire: %v", err)
	}
	if ok, _ := red.Key().Exists("k"); !ok {
		t.Fatal("key should exist before expiry")
	}

	// Advance past expiry: lazy filter must hide it.
	cur = 1_100_001
	if ok, _ := red.Key().Exists("k"); ok {
		t.Fatal("key should be lazily expired")
	}
	if _, err := red.Str().Get("k"); err == nil {
		t.Fatal("GET should miss expired key")
	}

	// Before sweeping, the meta slot still physically exists in the store.
	pre, err := st.eng.QueryPrefix(ctx, metaBase(0, "k"), true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(pre) == 0 {
		t.Fatal("expected meta slot to still be present before sweep")
	}

	// Sweep reclaims it (writes a delete tombstone over the meta).
	n, err := st.SweepExpired(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
	// After sweep, meta winner is a deleted slot → key absent.
	if _, _, ok, _ := st.readMeta(ctx, 0, "k"); ok {
		t.Fatal("meta should read as deleted after sweep")
	}
	// A second sweep finds nothing.
	if n, _ := st.SweepExpired(ctx); n != 0 {
		t.Fatalf("second sweep = %d, want 0", n)
	}
}
