package crdtstore

import (
	"testing"

	"github.com/redis-geo/redgeo/hlc"
)

func TestSlotRoundtripAndWinner(t *testing.T) {
	s := slot{stamp: hlc.Stamp{WallMS: 42, Logical: 7}, tag: tagPresent, value: []byte("hi")}
	got, err := decodeSlot(encodeSlot(s))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.stamp != s.stamp || got.tag != s.tag || string(got.value) != "hi" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	// winner picks max HLC; deleted winner => not live.
	cands := map[string]slot{
		"ra": {stamp: hlc.Stamp{WallMS: 10}, tag: tagPresent, value: []byte("old")},
		"rb": {stamp: hlc.Stamp{WallMS: 20}, tag: tagPresent, value: []byte("new")},
	}
	if v, ok := liveValue(cands); !ok || string(v) != "new" {
		t.Fatalf("liveValue = %q,%v want new,true", v, ok)
	}
	cands["rc"] = slot{stamp: hlc.Stamp{WallMS: 30}, tag: tagDeleted}
	if _, ok := liveValue(cands); ok {
		t.Fatalf("deleted slot with max HLC should make register absent")
	}
	// HLC tie broken by replicaID (higher wins).
	tie := map[string]slot{
		"ra": {stamp: hlc.Stamp{WallMS: 5}, tag: tagPresent, value: []byte("a")},
		"rz": {stamp: hlc.Stamp{WallMS: 5}, tag: tagPresent, value: []byte("z")},
	}
	if v, _ := liveValue(tie); string(v) != "z" {
		t.Fatalf("tie-break = %q want z", v)
	}
}

func TestKeyCodec(t *testing.T) {
	// Same logical key always lands in the same bucket; sub-keys share it.
	if bucket(0, "foo") != bucket(0, "foo") {
		t.Fatal("bucket not deterministic")
	}
	if part(0, "foo") != part(0, "foo") {
		t.Fatal("part not deterministic")
	}
	// Buckets are within range.
	for _, k := range []string{"", "a", "foo/bar", "x\x00y"} {
		if b := bucket(1, k); b < 0 || b >= NumBuckets {
			t.Fatalf("bucket(%q)=%d out of range", k, b)
		}
	}
	// Segment escaping is reversible and path-safe (no '/').
	for _, s := range []string{"", "plain", "a/b/c", "weird\x00\xff", "emoji😀"} {
		enc := encSeg(s)
		for i := 0; i < len(enc); i++ {
			if enc[i] == '/' {
				t.Fatalf("encSeg(%q)=%q contains '/'", s, enc)
			}
		}
		dec, err := decSeg(enc)
		if err != nil || dec != s {
			t.Fatalf("decSeg(encSeg(%q))=%q,%v", s, dec, err)
		}
	}
	// A string slot key is under its key's strBase prefix.
	sk := strSlot(2, "mykey", "replicaA").String()
	if !hasPrefix(sk, strBase(2, "mykey")) {
		t.Fatalf("strSlot %q not under strBase %q", sk, strBase(2, "mykey"))
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
