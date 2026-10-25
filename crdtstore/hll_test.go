package crdtstore

import (
	"fmt"
	"testing"
)

func hllElems(from, to int) [][]byte {
	var out [][]byte
	for i := from; i < to; i++ {
		out = append(out, []byte(fmt.Sprintf("elem-%d", i)))
	}
	return out
}

// TestHLLAccuracy: the estimator stays within a few percent across sparse and
// dense regimes (standard error ≈ 0.81%).
func TestHLLAccuracy(t *testing.T) {
	for _, n := range []int{1, 10, 1000, 100000} {
		var h hllSketch
		for _, e := range hllElems(0, n) {
			h.add(e)
		}
		got := h.count()
		if diff := float64(got-int64(n)) / float64(n); diff > 0.03 || diff < -0.03 {
			t.Fatalf("n=%d estimate=%d (off by %.1f%%)", n, got, diff*100)
		}
	}
	var empty hllSketch
	if empty.count() != 0 {
		t.Fatalf("empty count = %d", empty.count())
	}
}

func TestHLLEncodeRoundTrip(t *testing.T) {
	for _, n := range []int{0, 50, 20000} { // sparse, sparse, dense
		var h hllSketch
		for _, e := range hllElems(0, n) {
			h.add(e)
		}
		got, ok := decodeHLL(h.encode())
		if !ok || *got != h {
			t.Fatalf("n=%d: round trip mismatch (ok=%v)", n, ok)
		}
	}
	if _, ok := decodeHLL([]byte{9}); ok {
		t.Fatal("bad format accepted")
	}
}

func TestHLLCommands(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()

	if ch, err := str.PFAdd("a", hllElems(0, 100)...); err != nil || !ch {
		t.Fatalf("PFADD = %v,%v", ch, err)
	}
	if ch, _ := str.PFAdd("a", hllElems(0, 100)...); ch {
		t.Fatal("re-adding the same elements reported a change")
	}
	str.PFAdd("b", hllElems(50, 150)...)
	if n, _ := str.PFCount("a", "b", "missing"); n < 145 || n > 155 {
		t.Fatalf("PFCOUNT union = %d want ~150", n)
	}
	if err := str.PFMerge("c", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if n, _ := str.PFCount("c"); n < 145 || n > 155 {
		t.Fatalf("PFCOUNT merged = %d want ~150", n)
	}

	// Doesn't mix with plain strings or counters (§6.4).
	str.Set("plain", "x")
	if _, err := str.PFAdd("plain", []byte("e")); err == nil {
		t.Fatal("PFADD on plain string should fail")
	}
	if err := str.Set("a", "x"); err == nil {
		t.Fatal("SET on an HLL should fail")
	}
	if _, err := str.Get("a"); err == nil {
		t.Fatal("GET on an HLL should fail")
	}

	// RENAME and DEL.
	if err := st.Redka(0).Key().Rename("a", "a2"); err != nil {
		t.Fatal(err)
	}
	if n, _ := str.PFCount("a2"); n < 95 || n > 105 {
		t.Fatalf("renamed count = %d", n)
	}
	if n, _ := st.Redka(0).Key().Delete("a2"); n != 1 {
		t.Fatalf("DEL = %d", n)
	}
	if n, _ := str.PFCount("a2"); n != 0 {
		t.Fatalf("count after DEL = %d", n)
	}
}

// TestHLLConvergence: concurrent PFADDs on partitioned replicas merge to the
// union after heal (register-wise max).
func TestHLLConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	stores[0].Redka(0).Str().PFAdd("h", hllElems(0, 1000)...)
	stores[1].Redka(0).Str().PFAdd("h", hllElems(500, 1500)...)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i := 0; i < 2; i++ {
		i := i
		eventually(t, fmt.Sprintf("r%d sees union ~1500", i), func() bool {
			n, _ := stores[i].Redka(0).Str().PFCount("h")
			return n >= 1450 && n <= 1550
		})
	}
}
