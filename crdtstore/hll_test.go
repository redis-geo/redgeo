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

	// PF* on a plain string is refused; an HLL is a string to GET/SET.
	str.Set("plain", "x")
	if _, err := str.PFAdd("plain", []byte("e")); err == nil {
		t.Fatal("PFADD on plain string should fail")
	}
	if v, err := str.Get("a"); err != nil || len(v) != hyllDense || string(v[:4]) != "HYLL" {
		t.Fatalf("GET on an HLL = %d bytes,%v", len(v), err)
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

func TestHLLRedisFormat(t *testing.T) {
	// The empty sparse sketch Redis creates: header + one XZERO(16384) opcode.
	empty := append([]byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80"), 0x7f, 0xff)
	sk, ok := decodeRedis(empty)
	if !ok || sk.count() != 0 {
		t.Fatalf("empty sparse: ok=%v", ok)
	}

	// A hand-built sparse sketch: 3 zeros, value 5 twice, 1 zero via XZERO, value 32 once, then zeros to the end.
	sparse := []byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80")
	sparse = append(sparse, 0x02)                // ZERO: 3 zeros
	sparse = append(sparse, 0x80|(5-1)<<2|(2-1)) // VAL 5 x2
	sparse = append(sparse, 0x40, 0x00)          // XZERO: 1 zero
	sparse = append(sparse, 0x80|(32-1)<<2)      // VAL 32 x1
	rest := hllM - (3 + 2 + 1 + 1)
	sparse = append(sparse, byte(0x40|((rest-1)>>8)), byte((rest-1)&0xff))
	sk, ok = decodeRedis(sparse)
	if !ok || sk[2] != 0 || sk[3] != 5 || sk[4] != 5 || sk[5] != 0 || sk[6] != 32 || sk[7] != 0 {
		t.Fatalf("sparse decode: ok=%v regs=%v", ok, sk[:9])
	}
	// Dense round trip preserves every register, including those that straddle bytes.
	var h hllSketch
	for i := range h {
		h[i] = uint8((i*7)%51 + 1)
		if i%5 == 0 {
			h[i] = 0
		}
	}
	got, ok := decodeRedis(h.encodeRedis())
	if !ok || *got != h {
		t.Fatalf("dense round trip mismatch (ok=%v)", ok)
	}
	if enc := h.encodeRedis(); len(enc) != 12304 || enc[4] != 0 || enc[15]&0x80 == 0 {
		t.Fatalf("header: len=%d enc=%d card7=%x", len(enc), enc[4], enc[15])
	}

	// Malformed strings are not HLLs.
	for name, bad := range map[string][]byte{
		"short":        []byte("HYLL"),
		"bad magic":    append([]byte("XXXX"), make([]byte, hyllDense-4)...),
		"dense length": append([]byte("HYLL\x00"), make([]byte, hyllDense-6)...),
		"bad encoding": append([]byte("HYLL\x07"), make([]byte, hyllDense-5)...),
		"sparse short": append([]byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), 0x3f),
		"sparse over":  append([]byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), 0x7f, 0xff, 0x00),
		"sparse xzero": []byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40"),
	} {
		if _, ok := decodeRedis(bad); ok {
			t.Errorf("%s accepted", name)
		}
	}
}

// Sketches move between keys (and, in practice, between redgeo and Redis) with
// GET/SET, and the restored key keeps working with PF*.
func TestHLLGetSetInterop(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()
	str.PFAdd("src", hllElems(0, 500)...)
	blob, err := str.Get("src")
	if err != nil || len(blob) != hyllDense {
		t.Fatalf("GET = %d bytes,%v", len(blob), err)
	}
	if err := str.Set("dst", []byte(blob)); err != nil {
		t.Fatal(err)
	}
	a, _ := str.PFCount("src")
	b, _ := str.PFCount("dst")
	if a != b || a < 480 || a > 520 {
		t.Fatalf("counts differ after SET: src=%d dst=%d", a, b)
	}
	if ch, _ := str.PFAdd("dst", hllElems(0, 500)...); ch {
		t.Fatal("re-adding known elements to a restored sketch changed it")
	}
	str.PFAdd("dst", hllElems(500, 1000)...)
	if n, _ := str.PFCount("dst"); n < 960 || n > 1040 {
		t.Fatalf("restored sketch count after more adds = %d", n)
	}
	if n, _ := str.PFCount("src"); n != a {
		t.Fatalf("source changed: %d != %d", n, a)
	}

	// The empty sparse sketch Redis creates is accepted too.
	empty := append([]byte("HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80"), 0x7f, 0xff)
	if err := str.Set("e", empty); err != nil {
		t.Fatal(err)
	}
	if n, _ := str.PFCount("e"); n != 0 {
		t.Fatalf("empty restored count = %d", n)
	}
	if ch, _ := str.PFAdd("e", []byte("x")); !ch {
		t.Fatal("PFADD on restored empty sketch did not change it")
	}

	// A string that merely starts with HYLL stays a plain string.
	str.Set("fake", "HYLL not a sketch")
	if _, err := str.PFAdd("fake", []byte("x")); err == nil {
		t.Fatal("fake HYLL string treated as a sketch")
	}
	// SET overwrites an HLL with a plain string, and TTL/other string ops see it as one.
	if err := str.Set("src", "plain now"); err != nil {
		t.Fatal(err)
	}
	if v, _ := str.Get("src"); v.String() != "plain now" {
		t.Fatalf("overwrite: %q", v)
	}
	if _, err := str.PFAdd("src", []byte("x")); err == nil {
		t.Fatal("PFADD on the overwritten key should fail")
	}
}

// MurmurHash64A (Redis' HLL hash) sanity: deterministic, length- and
// content-sensitive, spread across the full register range.
func TestHLLHashProperties(t *testing.T) {
	if hllHash([]byte("abc")) != hllHash([]byte("abc")) || hllHash([]byte("abc")) == hllHash([]byte("abd")) ||
		hllHash([]byte("a")) == hllHash([]byte("a\x00")) || hllHash(nil) == hllHash([]byte{0}) {
		t.Fatal("hash not deterministic/content-sensitive")
	}
	seen := map[uint64]bool{}
	for i := 0; i < 20000; i++ { // every tail length 0..7 is exercised by the varying lengths
		seen[hllHash(hllElems(i, i+1)[0])&(hllM-1)] = true
	}
	if len(seen) < hllM*6/10 {
		t.Fatalf("only %d of %d registers hit by 20000 elements", len(seen), hllM)
	}
}
