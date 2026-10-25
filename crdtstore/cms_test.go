package crdtstore

import (
	"fmt"
	"testing"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

func one(s string) [][]byte { return [][]byte{[]byte(s)} }

func TestCMSBasics(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	if err := sk.CMSInit("c", 2000, 5); err != nil {
		t.Fatal(err)
	}
	if err := sk.CMSInit("c", 2000, 5); err != redis.ErrSketchExists {
		t.Fatalf("second init = %v", err)
	}
	for _, bad := range [][2]int{{0, 5}, {5, 0}, {-1, 3}, {1 << 40, 5}} {
		if sk.CMSInit("bad", bad[0], bad[1]) == nil {
			t.Errorf("CMSInit(%v) accepted", bad)
		}
	}
	got, err := sk.CMSIncr("c", [][]byte{[]byte("apple"), []byte("pear"), []byte("apple")}, []int64{3, 1, 4})
	if err != nil || got[0] != 3 || got[1] != 1 || got[2] != 7 {
		t.Fatalf("INCRBY = %v,%v", got, err)
	}
	q, _ := sk.CMSQuery("c", [][]byte{[]byte("apple"), []byte("pear"), []byte("never")})
	if q[0] != 7 || q[1] != 1 || q[2] != 0 {
		t.Fatalf("QUERY = %v", q)
	}
	w, d, n, err := sk.CMSInfo("c")
	if err != nil || w != 2000 || d != 5 || n != 8 {
		t.Fatalf("INFO = %d %d %d %v", w, d, n, err)
	}
	if _, err := sk.CMSIncr("c", one("x"), []int64{0}); err == nil {
		t.Fatal("zero increment accepted")
	}
	if _, err := sk.CMSIncr("c", one("x"), []int64{1<<62 + 1<<62 - 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := sk.CMSIncr("c", one("x"), []int64{1 << 62}); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, err := sk.CMSIncr("none", one("x"), []int64{1}); err != core.ErrNotFound {
		t.Fatalf("missing key = %v", err)
	}
	if _, err := sk.CMSQuery("none", one("x")); err != core.ErrNotFound {
		t.Fatalf("query missing = %v", err)
	}
	st.Redka(0).Str().Set("s", "v")
	if _, err := sk.CMSQuery("s", one("x")); err != core.ErrKeyType {
		t.Fatalf("query on a string = %v", err)
	}
	if ty, _ := st.Redka(0).Key().Get("c"); ty.TypeName() != "CMSk-TYPE" {
		t.Fatalf("type = %s", ty.TypeName())
	}
}

// A count-min sketch never underestimates, and over-estimates by at most
// roughly (2/width)·N for most items.
func TestCMSAccuracy(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.CMSInit("c", 1000, 6)
	truth := map[string]int64{}
	var total int64
	for i := 0; i < 100; i++ {
		item := fmt.Sprintf("item-%d", i)
		n := int64(1 + (i*37)%50)
		truth[item] = n
		total += n
		if _, err := sk.CMSIncr("c", one(item), []int64{n}); err != nil {
			t.Fatal(err)
		}
	}
	bound := float64(total) * 2 / 1000 * 3 // 3x the expected error
	over := 0
	for item, n := range truth {
		q, _ := sk.CMSQuery("c", one(item))
		if q[0] < n {
			t.Fatalf("%s underestimated: %d < %d", item, q[0], n)
		}
		if float64(q[0]-n) > bound {
			over++
		}
	}
	if over > 5 { // a few items may exceed the bound (probabilistic)
		t.Fatalf("%d of 100 items exceeded the error bound %.1f", over, bound)
	}
}

func TestCMSMerge(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	for _, k := range []string{"a", "b", "dest"} {
		sk.CMSInit(k, 500, 4)
	}
	sk.CMSInit("other", 100, 4)
	sk.CMSIncr("a", [][]byte{[]byte("x"), []byte("y")}, []int64{5, 2})
	sk.CMSIncr("b", [][]byte{[]byte("x"), []byte("z")}, []int64{1, 9})
	sk.CMSIncr("dest", one("old"), []int64{100}) // overwritten by the merge

	if err := sk.CMSMerge("dest", []string{"a", "b"}, []int64{1, 1}); err != nil {
		t.Fatal(err)
	}
	q, _ := sk.CMSQuery("dest", [][]byte{[]byte("x"), []byte("y"), []byte("z"), []byte("old")})
	if q[0] != 6 || q[1] != 2 || q[2] != 9 || q[3] != 0 {
		t.Fatalf("merged = %v (want 6 2 9 0)", q)
	}
	if _, _, n, _ := sk.CMSInfo("dest"); n != 17 {
		t.Fatalf("merged total = %d", n)
	}
	// Weights scale (and may be negative); a sketch can be merged into itself.
	if err := sk.CMSMerge("dest", []string{"a", "b"}, []int64{3, -1}); err != nil {
		t.Fatal(err)
	}
	q, _ = sk.CMSQuery("dest", [][]byte{[]byte("x"), []byte("y"), []byte("z")})
	if q[0] != 14 || q[1] != 6 || q[2] != 0 { // 3*5-1, 3*2, 3*0-9 clamped
		t.Fatalf("weighted = %v (want 14 6 0)", q)
	}
	if err := sk.CMSMerge("a", []string{"a", "a"}, []int64{1, 1}); err != nil {
		t.Fatal(err)
	}
	if q, _ := sk.CMSQuery("a", one("x")); q[0] != 10 {
		t.Fatalf("self merge = %v", q)
	}
	// Errors leave dest untouched.
	if err := sk.CMSMerge("dest", []string{"other"}, []int64{1}); err != redis.ErrCMSDims {
		t.Fatalf("mismatched dims = %v", err)
	}
	if err := sk.CMSMerge("dest", []string{"missing"}, []int64{1}); err != core.ErrNotFound {
		t.Fatalf("missing source = %v", err)
	}
	if err := sk.CMSMerge("missing", []string{"a"}, []int64{1}); err != core.ErrNotFound {
		t.Fatalf("missing dest = %v", err)
	}
	if q, _ := sk.CMSQuery("dest", one("x")); q[0] != 14 {
		t.Fatalf("dest changed by failed merges: %v", q)
	}
}

func TestCMSRenameDeleteRecreate(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.CMSInit("a", 300, 3)
	sk.CMSIncr("a", [][]byte{[]byte("k")}, []int64{42})
	if err := st.Redka(0).Key().Rename("a", "b"); err != nil {
		t.Fatal(err)
	}
	if q, err := sk.CMSQuery("b", one("k")); err != nil || q[0] != 42 {
		t.Fatalf("renamed = %v,%v", q, err)
	}
	if _, _, n, _ := sk.CMSInfo("b"); n != 42 {
		t.Fatalf("renamed total = %d", n)
	}
	st.Redka(0).Key().Delete("b")
	if _, err := sk.CMSQuery("b", one("k")); err != core.ErrNotFound {
		t.Fatalf("deleted = %v", err)
	}
	sk.CMSInit("b", 300, 3)
	if q, _ := sk.CMSQuery("b", one("k")); q[0] != 0 {
		t.Fatalf("recreated sketch kept %d", q[0])
	}
}

// Increments on partitioned replicas add up after healing; a merge replicates.
func TestCMSConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0].Redka(0).Sketch(), stores[1].Redka(0).Sketch()
	a.CMSInit("c", 400, 4)
	a.CMSInit("d", 400, 4)
	eventually(t, "r1 has the sketches", func() bool {
		_, _, _, e1 := b.CMSInfo("c")
		_, _, _, e2 := b.CMSInfo("d")
		return e1 == nil && e2 == nil
	})

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	a.CMSIncr("c", [][]byte{[]byte("hot"), []byte("a-only")}, []int64{10, 1})
	b.CMSIncr("c", [][]byte{[]byte("hot"), []byte("b-only")}, []int64{5, 2})
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i, s := range []interface {
		CMSQuery(string, [][]byte) ([]int64, error)
		CMSInfo(string) (int, int, int64, error)
	}{a, b} {
		i, s := i, s
		eventually(t, fmt.Sprintf("r%d sums the increments", i), func() bool {
			q, _ := s.CMSQuery("c", [][]byte{[]byte("hot"), []byte("a-only"), []byte("b-only")})
			_, _, n, _ := s.CMSInfo("c")
			return len(q) == 3 && q[0] == 15 && q[1] == 1 && q[2] == 2 && n == 18
		})
	}

	// A merge done on one replica produces the same result on the other.
	if err := b.CMSMerge("d", []string{"c"}, []int64{2}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "r0 sees the merged sketch", func() bool {
		q, _ := a.CMSQuery("d", one("hot"))
		_, _, n, _ := a.CMSInfo("d")
		return len(q) == 1 && q[0] == 30 && n == 36
	})
	// Concurrent merges and increments still add up (merge is "set to target",
	// so a racing increment on the destination is kept on top of it).
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	a.CMSIncr("d", one("hot"), []int64{1})
	b.CMSIncr("d", one("hot"), []int64{1})
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	eventually(t, "both increments counted", func() bool {
		qa, _ := a.CMSQuery("d", one("hot"))
		qb, _ := b.CMSQuery("d", one("hot"))
		return qa[0] == 32 && qb[0] == 32
	})
}
