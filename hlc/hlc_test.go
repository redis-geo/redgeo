package hlc

import "testing"

func TestStampOrderAndCodec(t *testing.T) {
	a := Stamp{WallMS: 100, Logical: 0}
	b := Stamp{WallMS: 100, Logical: 1}
	c := Stamp{WallMS: 101, Logical: 0}
	if !a.Less(b) || !b.Less(c) || a.Compare(a) != 0 {
		t.Fatalf("ordering broken: a=%v b=%v c=%v", a, b, c)
	}
	for _, s := range []Stamp{a, b, c, {WallMS: 1 << 40, Logical: 1 << 20}} {
		got, ok := Decode(s.Encode())
		if !ok || got != s {
			t.Errorf("roundtrip %v -> %v ok=%v", s, got, ok)
		}
	}
}

func TestClockMonotonicAndObserve(t *testing.T) {
	// Frozen wall clock: logical counter must carry the ordering.
	ms := int64(1000)
	c := NewWithClock(func() int64 { return ms })
	prev := c.Now()
	for i := 0; i < 5; i++ {
		next := c.Now()
		if !prev.Less(next) {
			t.Fatalf("clock not monotonic: %v !< %v", prev, next)
		}
		prev = next
	}
	// Observing a far-future remote stamp must push local stamps past it.
	remote := Stamp{WallMS: 5000, Logical: 9}
	c.Observe(remote)
	if got := c.Now(); !remote.Less(got) {
		t.Fatalf("after Observe, Now()=%v not > remote=%v", got, remote)
	}
}
