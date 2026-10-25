package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
)

func newEng(t *testing.T, cfg Config) *Engine {
	t.Helper()
	if cfg.ReplicaID == "" {
		cfg.ReplicaID = "r1"
	}
	e, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func mustGet(t *testing.T, e *Engine, k string, want string) {
	t.Helper()
	v, err := e.Get(context.Background(), ds.NewKey(k))
	if err != nil || string(v) != want {
		t.Fatalf("Get(%s) = %q,%v want %q", k, v, err, want)
	}
}

// TestRotationPersistsGeneration: a rotated, persisted node must come back on
// its latest generation after a restart (not on the purged generation 0).
func TestRotationPersistsGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := newEng(t, Config{PebbleDir: dir})
	e.Put(ctx, ds.NewKey("/00/a"), []byte("1"))
	e.Put(ctx, ds.NewKey("/00/gone"), []byte("x"))
	e.Delete(ctx, ds.NewKey("/00/gone"))
	for i := 0; i < 3; i++ {
		if _, err := e.RotatePartition(ctx, 0); err != nil {
			t.Fatal(err)
		}
	}
	if g := e.Generation(0); g != 3 {
		t.Fatalf("generation = %d", g)
	}
	e.Put(ctx, ds.NewKey("/00/b"), []byte("2"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = newEng(t, Config{PebbleDir: dir})
	defer e.Close()
	if g := e.Generation(0); g != 3 || e.InDoubt(0) {
		t.Fatalf("restored generation = %d in-doubt=%v", g, e.InDoubt(0))
	}
	mustGet(t, e, "/00/a", "1")
	mustGet(t, e, "/00/b", "2")
	if ok, _ := e.Has(ctx, ds.NewKey("/00/gone")); ok {
		t.Fatal("deleted key resurrected")
	}
}

// TestManyRotationsNoCollateralPurge guards purgeNamespace's prefix handling:
// purging generation 1 must not touch generation 10-19.
func TestManyRotationsNoCollateralPurge(t *testing.T) {
	ctx := context.Background()
	e := newEng(t, Config{})
	defer e.Close()
	e.Put(ctx, ds.NewKey("/00/k"), []byte("v"))
	for i := 0; i < 13; i++ {
		if _, err := e.RotatePartition(ctx, 0); err != nil {
			t.Fatalf("rotation %d: %v", i, err)
		}
		mustGet(t, e, "/00/k", "v")
	}
}

func TestRollbackRestoresOldGeneration(t *testing.T) {
	ctx := context.Background()
	e := newEng(t, Config{})
	defer e.Close()
	e.Put(ctx, ds.NewKey("/00/k"), []byte("v"))

	// Rollback after Stage only.
	e.Freeze(ctx, 0)
	if _, _, err := e.Stage(ctx, 0, true); err != nil {
		t.Fatal(err)
	}
	if !e.InDoubt(0) {
		t.Fatal("staged rotation should be in doubt")
	}
	if err := e.Rollback(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// Rollback after Activate.
	if _, _, err := e.Stage(ctx, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := e.Activate(ctx, 0, false); err != nil {
		t.Fatal(err)
	}
	if e.Generation(0) != 1 {
		t.Fatalf("activated generation = %d", e.Generation(0))
	}
	if err := e.Rollback(ctx, 0); err != nil {
		t.Fatal(err)
	}
	e.Unfreeze(0)
	if e.Generation(0) != 0 || e.InDoubt(0) {
		t.Fatalf("after rollback: gen=%d in-doubt=%v", e.Generation(0), e.InDoubt(0))
	}
	mustGet(t, e, "/00/k", "v")
	if err := e.Put(ctx, ds.NewKey("/00/k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	// The rolled-back generation can be rotated again afterwards.
	if _, err := e.RotatePartition(ctx, 0); err != nil {
		t.Fatal(err)
	}
	mustGet(t, e, "/00/k2", "v2")
}

func TestFreezeBlocksWritesNotReads(t *testing.T) {
	ctx := context.Background()
	e := newEng(t, Config{})
	defer e.Close()
	e.Put(ctx, ds.NewKey("/00/k"), []byte("v"))
	if err := e.Freeze(ctx, 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Put(ctx, ds.NewKey("/00/k2"), []byte("v2")) }()
	mustGet(t, e, "/00/k", "v") // reads continue while frozen
	select {
	case err := <-done:
		t.Fatalf("write completed while frozen: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	e.Unfreeze(0)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mustGet(t, e, "/00/k2", "v2")
}

func TestBatchSpanningRotationFails(t *testing.T) {
	ctx := context.Background()
	e := newEng(t, Config{})
	defer e.Close()
	b, _ := e.Batch(ctx)
	b.Put(ctx, ds.NewKey("/00/k"), []byte("v"))
	if _, err := e.RotatePartition(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(ctx); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("commit across rotation = %v, want ErrGenerationChanged", err)
	}
	b2, _ := e.Batch(ctx)
	b2.Put(ctx, ds.NewKey("/00/k"), []byte("v"))
	if err := b2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
