package crdtstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

func listNames(es []restypes.TopKEntry) string {
	var out []string
	for _, e := range es {
		out = append(out, fmt.Sprintf("%s:%d", e.Item, e.Count))
	}
	return strings.Join(out, " ")
}

func TestSpaceSaving(t *testing.T) {
	s := summary{}
	for _, it := range []string{"a", "a", "b", "c"} {
		s.add(it, 1, 3)
	}
	if s["a"].count != 2 || len(s) != 3 {
		t.Fatalf("summary = %v", s)
	}
	// A new item over capacity evicts the minimum (ties: smallest name) and
	// inherits its count as error.
	s.add("d", 1, 3)
	if _, ok := s["b"]; ok || s["d"].count != 2 || s["d"].err != 1 || len(s) != 3 {
		t.Fatalf("after eviction: b=%v d=%+v len=%d", s["b"], s["d"], len(s))
	}
	got, ok := decodeSummary(encodeSummary(s))
	if !ok || len(got) != 3 || got["a"].count != 2 || got["d"].err != 1 {
		t.Fatalf("round trip = %v,%v", got, ok)
	}
	if _, ok := decodeSummary([]byte{0xff}); ok {
		t.Fatal("garbage decoded")
	}
	if _, ok := decodeSummary([]byte{2, 1, 'x'}); ok {
		t.Fatal("truncated summary decoded")
	}
}

func TestTopKBasics(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	if err := sk.TopKReserve("t", 3, 8, 7, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := sk.TopKReserve("t", 3, 8, 7, 0.9); err != redis.ErrSketchExists {
		t.Fatalf("second reserve = %v", err)
	}
	for _, bad := range [][4]float64{{0, 8, 7, 0.9}, {3, 0, 7, 0.9}, {3, 8, 0, 0.9}, {3, 8, 7, 0}, {3, 8, 7, 1.5}} {
		if sk.TopKReserve("bad", int(bad[0]), int(bad[1]), int(bad[2]), bad[3]) == nil {
			t.Errorf("reserve %v accepted", bad)
		}
	}
	add := func(item string, n int64) []byte {
		out, err := sk.TopKAdd("t", one(item), []int64{n})
		if err != nil {
			t.Fatal(err)
		}
		return out[0]
	}
	add("a", 50)
	add("b", 30)
	add("c", 20)
	if e := add("d", 5); e != nil { // top-3 is still a b c
		t.Fatalf("expelled %q too early", e)
	}
	list, _ := sk.TopKList("t")
	if listNames(list) != "a:50 b:30 c:20" {
		t.Fatalf("list = %s", listNames(list))
	}
	// d overtakes c: that add expels c.
	if e := add("d", 20); string(e) != "c" {
		t.Fatalf("expelled %q, want c", e)
	}
	list, _ = sk.TopKList("t")
	if listNames(list) != "a:50 b:30 d:25" {
		t.Fatalf("list = %s", listNames(list))
	}
	in, _ := sk.TopKQuery("t", [][]byte{[]byte("a"), []byte("c"), []byte("zzz")})
	if !in[0] || in[1] || in[2] {
		t.Fatalf("query = %v", in)
	}
	counts, _ := sk.TopKCount("t", [][]byte{[]byte("a"), []byte("c"), []byte("zzz")})
	if counts[0] != 50 || counts[1] != 20 || counts[2] != 0 {
		t.Fatalf("count = %v", counts)
	}
	info, err := sk.TopKInfo("t")
	if err != nil || info.K != 3 || info.Width != 8 || info.Depth != 7 || info.Decay != 0.9 {
		t.Fatalf("info = %+v,%v", info, err)
	}
	// Batch add reports per item.
	out, _ := sk.TopKAdd("t", [][]byte{[]byte("e"), []byte("e")}, []int64{1000, 1000})
	if out[0] == nil || out[1] != nil { // the first push expels one item, the second expels none
		t.Fatalf("batch expelled = %q", out)
	}
	if _, err := sk.TopKAdd("t", one("x"), []int64{0}); err == nil {
		t.Fatal("zero increment accepted")
	}
	if _, err := sk.TopKList("none"); err != core.ErrNotFound {
		t.Fatalf("missing = %v", err)
	}
	st.Redka(0).Str().Set("s", "v")
	if _, err := sk.TopKList("s"); err != core.ErrKeyType {
		t.Fatalf("on a string = %v", err)
	}
	if ty, _ := st.Redka(0).Key().Get("t"); ty.TypeName() != "TopK-TYPE" {
		t.Fatalf("type = %s", ty.TypeName())
	}
}

// Items whose frequency exceeds N/slots are always tracked, however much noise
// surrounds them.
func TestTopKFindsHeavyHitters(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.TopKReserve("t", 5, 8, 7, 0.9)
	var batch [][]byte
	var incs []int64
	for i := 0; i < 3000; i++ {
		batch = append(batch, []byte(fmt.Sprintf("noise-%d", i)))
		incs = append(incs, 1)
		if i%100 == 0 { // interleave the heavy items through the stream
			for h := 0; h < 5; h++ {
				batch = append(batch, []byte(fmt.Sprintf("heavy-%d", h)))
				incs = append(incs, int64(40+h))
			}
		}
	}
	if _, err := sk.TopKAdd("t", batch, incs); err != nil {
		t.Fatal(err)
	}
	list, _ := sk.TopKList("t")
	names := listNames(list)
	for h := 0; h < 5; h++ {
		if !strings.Contains(names, fmt.Sprintf("heavy-%d:", h)) {
			t.Fatalf("heavy-%d missing from %s", h, names)
		}
	}
	// Heaviest first.
	if string(list[0].Item) != "heavy-4" {
		t.Fatalf("order = %s", names)
	}
}

func TestTopKRenameDeleteRecreate(t *testing.T) {
	st := newTestStore(t)
	sk := st.Redka(0).Sketch()
	sk.TopKReserve("a", 2, 8, 7, 0.9)
	sk.TopKAdd("a", [][]byte{[]byte("x"), []byte("y")}, []int64{9, 4})
	if err := st.Redka(0).Key().Rename("a", "b"); err != nil {
		t.Fatal(err)
	}
	list, err := sk.TopKList("b")
	if err != nil || listNames(list) != "x:9 y:4" {
		t.Fatalf("renamed = %s,%v", listNames(list), err)
	}
	st.Redka(0).Key().Delete("b")
	if _, err := sk.TopKList("b"); err != core.ErrNotFound {
		t.Fatalf("deleted = %v", err)
	}
	sk.TopKReserve("b", 2, 8, 7, 0.9)
	if list, _ := sk.TopKList("b"); len(list) != 0 {
		t.Fatalf("recreated list = %s", listNames(list))
	}
}

// Each replica summarises its own stream; the merged list adds them up.
func TestTopKConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0].Redka(0).Sketch(), stores[1].Redka(0).Sketch()
	a.TopKReserve("t", 3, 8, 7, 0.9)
	eventually(t, "r1 has it", func() bool { _, err := b.TopKInfo("t"); return err == nil })

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	a.TopKAdd("t", [][]byte{[]byte("x"), []byte("y"), []byte("only-a")}, []int64{10, 3, 7})
	b.TopKAdd("t", [][]byte{[]byte("x"), []byte("y"), []byte("only-b")}, []int64{5, 20, 1})
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i, s := range []interface {
		TopKList(string) ([]restypes.TopKEntry, error)
	}{a, b} {
		i, s := i, s
		eventually(t, fmt.Sprintf("r%d merged list", i), func() bool {
			l, _ := s.TopKList("t")
			return listNames(l) == "y:23 x:15 only-a:7"
		})
	}
}
