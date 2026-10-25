package crdtstore

import (
	"fmt"
	"testing"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

func items(prefix string, from, to int) [][]byte {
	var out [][]byte
	for i := from; i < to; i++ {
		out = append(out, []byte(fmt.Sprintf("%s-%d", prefix, i)))
	}
	return out
}

func TestBloomGeometry(t *testing.T) {
	p := bloomParams{errRate: 0.01, capacity: 100, expansion: 2}
	c, bits, k, err := p.layer(0)
	if err != nil || c != 100 || bits != 959 || k != 7 {
		t.Fatalf("layer 0 = cap %d bits %d k %d err %v (want 100, 959, 7)", c, bits, k, err)
	}
	c1, bits1, k1, _ := p.layer(1) // twice the capacity at half the error rate
	if c1 != 200 || bits1 <= 2*bits || k1 != 8 {
		t.Fatalf("layer 1 = cap %d bits %d k %d", c1, bits1, k1)
	}
	if _, _, _, err := (bloomParams{errRate: 0.01, capacity: 100}).layer(1); err == nil {
		t.Fatal("non-scaling filter has a second layer")
	}
	for _, bad := range []bloomParams{{0, 100, 2}, {1, 100, 2}, {0.01, 0, 2}, {0.01, 1 << 50, 2}, {0.01, 100, -1}} {
		if bad.validate() == nil {
			t.Errorf("%+v validated", bad)
		}
	}
	if (bloomParams{errRate: 0.01, capacity: 100, expansion: 2}).validate() != nil {
		t.Error("good params rejected")
	}
}

func TestBloomBasics(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	if err := sk.BFReserve("bf", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 1000, Expansion: 2}); err != nil {
		t.Fatal(err)
	}
	if err := sk.BFReserve("bf", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 1000, Expansion: 2}); err != ErrSketchExists {
		t.Fatalf("second reserve = %v", err)
	}
	added, err := sk.BFAdd("bf", items("in", 0, 500), nil)
	if err != nil {
		t.Fatal(err)
	}
	newCount := 0
	for _, a := range added {
		if a {
			newCount++
		}
	}
	if newCount < 490 { // a few can collide with earlier items (false positives)
		t.Fatalf("only %d of 500 distinct items reported new", newCount)
	}
	// No false negatives.
	found, _ := sk.BFExists("bf", items("in", 0, 500))
	for i, f := range found {
		if !f {
			t.Fatalf("item %d missing", i)
		}
	}
	// Re-adding reports nothing new.
	again, _ := sk.BFAdd("bf", items("in", 0, 500), nil)
	for i, a := range again {
		if a {
			t.Fatalf("item %d re-added as new", i)
		}
	}
	// False positives stay near the target rate.
	fp := 0
	probes := items("out", 0, 10000)
	res, _ := sk.BFExists("bf", probes)
	for _, f := range res {
		if f {
			fp++
		}
	}
	if fp > 300 { // target 1% (=100) at full capacity; this filter is half full
		t.Fatalf("%d false positives in 10000 probes", fp)
	}
	info, err := sk.BFInfo("bf")
	if err != nil || info.Filters != 1 || info.Capacity != 1000 || info.Items != int64(newCount) || info.Expansion != 2 || info.SizeBytes < 1000 {
		t.Fatalf("info = %+v,%v", info, err)
	}

	// Missing key: nothing is contained; auto-create needs opts.
	if r, err := sk.BFExists("nokey", items("x", 0, 2)); err != nil || r[0] || r[1] {
		t.Fatalf("exists on missing = %v,%v", r, err)
	}
	if _, err := sk.BFAdd("nokey", items("x", 0, 1), nil); err != core.ErrNotFound {
		t.Fatalf("add without create = %v", err)
	}
	if _, err := sk.BFAdd("auto", items("x", 0, 3), &restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := sk.BFInfo("nokey"); err != core.ErrNotFound {
		t.Fatalf("info on missing = %v", err)
	}
	// A different type on the key is refused.
	st.Redka(0).Str().Set("str", "x")
	if _, err := sk.BFAdd("str", items("x", 0, 1), nil); err != core.ErrKeyType {
		t.Fatalf("add on a string = %v", err)
	}
	if ty, _ := st.Redka(0).Key().Get("bf"); ty.TypeName() != "MBbloom--" {
		t.Fatalf("type = %s", ty.TypeName())
	}
}

func TestBloomScalesAndNonScaling(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.BFReserve("s", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2})
	if _, err := sk.BFAdd("s", items("n", 0, 1000), nil); err != nil {
		t.Fatal(err)
	}
	info, _ := sk.BFInfo("s")
	if info.Filters < 3 || info.Capacity < 1000 {
		t.Fatalf("filter did not scale: %+v", info)
	}
	found, _ := sk.BFExists("s", items("n", 0, 1000))
	for i, f := range found {
		if !f {
			t.Fatalf("item %d lost across layers", i)
		}
	}
	fp := 0
	res, _ := sk.BFExists("s", items("miss", 0, 10000))
	for _, f := range res {
		if f {
			fp++
		}
	}
	if fp > 300 {
		t.Fatalf("%d false positives after scaling", fp)
	}

	sk.BFReserve("fixed", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 50, Expansion: 0})
	_, err := sk.BFAdd("fixed", items("n", 0, 200), nil)
	if err == nil {
		t.Fatal("a full non-scaling filter accepted more items")
	}
	if got, _ := sk.BFInfo("fixed"); got.Items != 50 || got.Filters != 1 || got.Expansion != 0 {
		t.Fatalf("fixed info = %+v", got)
	}
}

func TestBloomDeleteRenameRecreate(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.BFReserve("a", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2})
	sk.BFAdd("a", items("v", 0, 50), nil)
	if err := st.Redka(0).Key().Rename("a", "b"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Redka(0).Key().Exists("a"); ok {
		t.Fatal("old key still exists")
	}
	found, _ := sk.BFExists("b", items("v", 0, 50))
	for i, f := range found {
		if !f {
			t.Fatalf("renamed filter lost item %d", i)
		}
	}
	if info, _ := sk.BFInfo("b"); info.Items != 50 {
		t.Fatalf("renamed items = %d", info.Items)
	}
	st.Redka(0).Key().Delete("b")
	if ok, _ := st.Redka(0).Key().Exists("b"); ok {
		t.Fatal("deleted filter exists")
	}
	// Re-creating with the same parameters must not see the old bits.
	sk.BFReserve("b", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2})
	if info, _ := sk.BFInfo("b"); info.Items != 0 {
		t.Fatalf("recreated filter has %d items", info.Items)
	}
	hits := 0
	res, _ := sk.BFExists("b", items("v", 0, 50))
	for _, f := range res {
		if f {
			hits++
		}
	}
	if hits > 3 {
		t.Fatalf("recreated filter still contains %d old items", hits)
	}
}

// Replicas add disjoint items while partitioned; after healing every replica
// contains everything (bits merge by OR) and the insert counts add up.
func TestBloomConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0].Redka(0).Sketch(), stores[1].Redka(0).Sketch()
	if err := a.BFReserve("bf", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 1000, Expansion: 2}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "r1 has the filter", func() bool { _, err := b.BFInfo("bf"); return err == nil })

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	a.BFAdd("bf", items("a", 0, 300), nil)
	b.BFAdd("bf", items("b", 0, 300), nil)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)

	for i, s := range []interface {
		BFExists(string, [][]byte) ([]bool, error)
		BFInfo(string) (restypes.BloomInfo, error)
	}{a, b} {
		i, s := i, s
		eventually(t, fmt.Sprintf("r%d contains both sets", i), func() bool {
			ra, _ := s.BFExists("bf", items("a", 0, 300))
			rb, _ := s.BFExists("bf", items("b", 0, 300))
			for _, f := range append(ra, rb...) {
				if !f {
					return false
				}
			}
			info, _ := s.BFInfo("bf")
			return info.Items >= 590 && info.Items <= 600
		})
	}
}

// Two replicas that auto-create a filter with the default parameters at the
// same time share one namespace, so neither loses its inserts.
func TestBloomConcurrentAutoCreate(t *testing.T) {
	stores, net := newCluster(t, 2)
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	def := &restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2}
	a, b := stores[0].Redka(0).Sketch(), stores[1].Redka(0).Sketch()
	a.BFAdd("k", items("a", 0, 20), def)
	b.BFAdd("k", items("b", 0, 20), def)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i, s := range []interface {
		BFExists(string, [][]byte) ([]bool, error)
	}{a, b} {
		i, s := i, s
		eventually(t, fmt.Sprintf("r%d has both", i), func() bool {
			ra, _ := s.BFExists("k", items("a", 0, 20))
			rb, _ := s.BFExists("k", items("b", 0, 20))
			for _, f := range append(ra, rb...) {
				if !f {
					return false
				}
			}
			return true
		})
	}
}

// Concurrent reservations with different parameters: one wins everywhere, and
// every replica agrees on which (the loser's inserts are simply ignored).
func TestBloomConflictingReserve(t *testing.T) {
	stores, net := newCluster(t, 2)
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	stores[0].Redka(0).Sketch().BFReserve("k", restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2})
	stores[1].Redka(0).Sketch().BFReserve("k", restypes.BloomOpts{ErrorRate: 0.05, Capacity: 500, Expansion: 4})
	stores[0].Redka(0).Sketch().BFAdd("k", items("a", 0, 10), nil)
	stores[1].Redka(0).Sketch().BFAdd("k", items("b", 0, 10), nil)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	eventually(t, "replicas agree on the winning parameters", func() bool {
		i0, e0 := stores[0].Redka(0).Sketch().BFInfo("k")
		i1, e1 := stores[1].Redka(0).Sketch().BFInfo("k")
		return e0 == nil && e1 == nil && i0.Capacity == i1.Capacity && i0.Items == i1.Items
	})
}
