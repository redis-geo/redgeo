package crdtstore

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/redis-geo/redgeo/restypes"
)

func TestBitCountPos(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()
	str.Set("foobar", "foobar")
	count := func(r restypes.BitRange) int64 { n, _ := str.BitCount("foobar", r); return n }
	if n := count(restypes.BitRange{}); n != 26 {
		t.Fatalf("BITCOUNT = %d want 26", n)
	}
	if n := count(restypes.BitRange{Start: 0, End: 0, HasStart: true, HasEnd: true}); n != 4 {
		t.Fatalf("BITCOUNT 0 0 = %d want 4", n)
	}
	if n := count(restypes.BitRange{Start: 1, End: 1, HasStart: true, HasEnd: true}); n != 6 {
		t.Fatalf("BITCOUNT 1 1 = %d want 6", n)
	}
	if n := count(restypes.BitRange{Start: 5, End: 30, HasStart: true, HasEnd: true, InBits: true}); n != 17 {
		t.Fatalf("BITCOUNT 5 30 BIT = %d want 17", n)
	}
	if n := count(restypes.BitRange{Start: -2, End: -1, HasStart: true, HasEnd: true}); n != 7 {
		t.Fatalf("BITCOUNT -2 -1 = %d want 7", n)
	}

	pos := func(k string, bit bool, r restypes.BitRange) int64 { n, _ := str.BitPos(k, bit, r); return n }
	str.Set("a", "\xff\xf0\x00")
	str.Set("b", "\x00\xff\xf0")
	str.Set("z", "\x00\x00\x00")
	str.Set("o", "\xff\xff\xff")
	if n := pos("a", false, restypes.BitRange{}); n != 12 {
		t.Fatalf("BITPOS a 0 = %d", n)
	}
	if n := pos("b", true, restypes.BitRange{}); n != 8 {
		t.Fatalf("BITPOS b 1 = %d", n)
	}
	if n := pos("b", true, restypes.BitRange{Start: 2, HasStart: true}); n != 16 {
		t.Fatalf("BITPOS b 1 2 = %d", n)
	}
	if n := pos("z", true, restypes.BitRange{}); n != -1 {
		t.Fatalf("BITPOS z 1 = %d", n)
	}
	if n := pos("o", false, restypes.BitRange{}); n != 24 {
		t.Fatalf("BITPOS o 0 = %d want 24", n)
	}
	if n := pos("o", false, restypes.BitRange{Start: 0, End: 2, HasStart: true, HasEnd: true}); n != -1 {
		t.Fatalf("BITPOS o 0 0 2 = %d want -1", n)
	}
	if n := pos("missing", false, restypes.BitRange{}); n != 0 {
		t.Fatalf("BITPOS missing 0 = %d", n)
	}
	if n := pos("missing", true, restypes.BitRange{}); n != -1 {
		t.Fatalf("BITPOS missing 1 = %d", n)
	}
}

func TestBitmapSetGet(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()

	// Offsets spanning several chunks; GET assembles the string.
	offs := []uint64{0, 7, 8, 4095, 4096, 10000}
	for _, o := range offs {
		if old, err := str.SetBit("bm", o, true); err != nil || old {
			t.Fatalf("SETBIT %d = %v,%v", o, old, err)
		}
	}
	if old, _ := str.SetBit("bm", 7, true); !old {
		t.Fatal("second SETBIT should report old=1")
	}
	for _, o := range offs {
		if on, _ := str.GetBit("bm", o); !on {
			t.Fatalf("GETBIT %d = 0", o)
		}
	}
	if on, _ := str.GetBit("bm", 1); on {
		t.Fatal("GETBIT 1 = 1")
	}
	if n, _ := str.BitCount("bm", restypes.BitRange{}); n != int64(len(offs)) {
		t.Fatalf("BITCOUNT = %d", n)
	}
	v, err := str.Get("bm")
	if err != nil || len(v) != 10000/8+1 {
		t.Fatalf("GET len = %d,%v want %d", len(v), err, 10000/8+1)
	}
	if str.Set("bm", "x") != nil {
		t.Fatal("SET should overwrite a bitmap")
	}
	if v, _ := str.Get("bm"); string(v) != "x" {
		t.Fatalf("after SET = %q", v)
	}
}

func TestBitmapFromStringAndOps(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()
	str.Set("s", "\x00")
	if old, err := str.SetBit("s", 0, true); err != nil || old {
		t.Fatal(old, err)
	}
	if v, _ := str.Get("s"); !bytes.Equal(v, []byte{0x80}) {
		t.Fatalf("converted = %x", v)
	}
	if _, err := str.Incr("ctr", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := str.SetBit("ctr", 0, true); err == nil {
		t.Fatal("SETBIT on a counter should fail")
	}

	str.Set("k1", "foobar")
	str.Set("k2", "abcdef")
	for op, want := range map[string][]byte{
		"and": []byte("`bc`ab"),
		"or":  []byte("goofev"),
		"xor": []byte{7, 13, 12, 6, 4, 20},
	} {
		if n, err := str.BitOp(op, "dst", "k1", "k2"); err != nil || n != 6 {
			t.Fatalf("BITOP %s = %d,%v", op, n, err)
		}
		if v, _ := str.Get("dst"); !bytes.Equal(v, want) {
			t.Fatalf("BITOP %s = %q want %q", op, v, want)
		}
	}
	if n, _ := str.BitOp("not", "dst", "k1"); n != 6 {
		t.Fatal(n)
	}
	if v, _ := str.Get("dst"); v[0] != ^byte('f') {
		t.Fatalf("NOT = %x", v)
	}
	// Shorter operand is zero-padded; empty result deletes dest.
	str.Set("short", "a")
	if n, _ := str.BitOp("or", "dst", "short", "k1"); n != 6 {
		t.Fatal(n)
	}
	if n, _ := str.BitOp("and", "dst", "nokey1", "nokey2"); n != 0 {
		t.Fatal(n)
	}
	if ok, _ := st.Redka(0).Key().Exists("dst"); ok {
		t.Fatal("empty BITOP result should delete dest")
	}
}

func TestBitmapRenameDel(t *testing.T) {
	st := newTestStore(t)
	str := st.Redka(0).Str()
	str.SetBit("a", 9000, true)
	if err := st.Redka(0).Key().Rename("a", "b"); err != nil {
		t.Fatal(err)
	}
	if on, _ := str.GetBit("b", 9000); !on {
		t.Fatal("renamed bit lost")
	}
	if ok, _ := st.Redka(0).Key().Exists("a"); ok {
		t.Fatal("old key still exists")
	}
	st.Redka(0).Key().Delete("b")
	if ok, _ := st.Redka(0).Key().Exists("b"); ok {
		t.Fatal("deleted bitmap still exists")
	}
	// Re-creating must not resurrect old chunks.
	str.SetBit("b", 1, true)
	if n, _ := str.BitCount("b", restypes.BitRange{}); n != 1 {
		t.Fatalf("recreated BITCOUNT = %d want 1", n)
	}
}

// TestBitmapConvergence: bit flips in different chunks on partitioned
// replicas merge after heal.
func TestBitmapConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	stores[0].Redka(0).Str().SetBit("bm", 0, true)
	eventually(t, "r1 sees bm", func() bool {
		on, _ := stores[1].Redka(0).Str().GetBit("bm", 0)
		return on
	})
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	stores[0].Redka(0).Str().SetBit("bm", 100, true)   // chunk 0
	stores[1].Redka(0).Str().SetBit("bm", 50000, true) // chunk 12
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i := 0; i < 2; i++ {
		i := i
		eventually(t, fmt.Sprintf("r%d sees both bits", i), func() bool {
			a, _ := stores[i].Redka(0).Str().GetBit("bm", 100)
			b, _ := stores[i].Redka(0).Str().GetBit("bm", 50000)
			return a && b
		})
	}
}
