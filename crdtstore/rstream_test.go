package crdtstore

import (
	"fmt"
	"testing"

	"github.com/redis-geo/redgeo/restypes"
)

func fv(s ...string) [][]byte {
	var out [][]byte
	for _, x := range s {
		out = append(out, []byte(x))
	}
	return out
}

func TestNextStreamID(t *testing.T) {
	auto := restypes.StreamAddOpts{Mode: restypes.IDAuto}
	last := restypes.StreamID{Ms: 100, Seq: 5 << 16}
	// Clock ahead of the stream: fresh ms.
	if id, _ := nextStreamID(200, last, auto, 7); id != (restypes.StreamID{Ms: 200, Seq: 7}) {
		t.Fatalf("fresh ms id = %v", id)
	}
	// Same/behind clock: stay on last.Ms, bump the counter, keep discriminator.
	for _, now := range []uint64{100, 50} {
		id, _ := nextStreamID(now, last, auto, 9)
		if id != (restypes.StreamID{Ms: 100, Seq: 6<<16 | 9}) || id.Cmp(last) <= 0 {
			t.Fatalf("now=%d id = %v", now, id)
		}
	}
	// ms-* and explicit.
	seq := restypes.StreamAddOpts{Mode: restypes.IDAutoSeq, ID: restypes.StreamID{Ms: 100}}
	if id, _ := nextStreamID(0, last, seq, 3); id != (restypes.StreamID{Ms: 100, Seq: 6<<16 | 3}) {
		t.Fatalf("ms-* id = %v", id)
	}
	seq.ID.Ms = 99
	if _, err := nextStreamID(0, last, seq, 3); err != errXaddSmall {
		t.Fatalf("ms-* below top: %v", err)
	}
	if _, err := nextStreamID(0, restypes.StreamID{}, restypes.StreamAddOpts{}, 0); err != errXaddZero {
		t.Fatalf("0-0: %v", err)
	}
	if _, err := nextStreamID(0, last, restypes.StreamAddOpts{ID: last}, 0); err != errXaddSmall {
		t.Fatalf("equal to top: %v", err)
	}
	if id, err := nextStreamID(0, last, restypes.StreamAddOpts{ID: restypes.StreamID{Ms: 100, Seq: 1 << 20}}, 0); err != nil || id.Seq != 1<<20 {
		t.Fatalf("explicit: %v %v", id, err)
	}
}

func TestStreamBasics(t *testing.T) {
	st := newTestStore(t)
	rs := st.Redka(0).Stream()
	var ids []restypes.StreamID
	for i := 0; i < 5; i++ {
		id, ok, err := rs.Add("s", restypes.StreamAddOpts{Mode: restypes.IDAuto}, fv("n", fmt.Sprint(i)))
		if err != nil || !ok {
			t.Fatalf("XADD: %v %v", ok, err)
		}
		if len(ids) > 0 && id.Cmp(ids[len(ids)-1]) <= 0 {
			t.Fatalf("ids not increasing: %v then %v", ids[len(ids)-1], id)
		}
		ids = append(ids, id)
	}
	if n, _ := rs.Len("s"); n != 5 {
		t.Fatalf("len = %d", n)
	}
	all := restypes.StreamBound{ID: maxID}
	es, _ := rs.Range("s", restypes.StreamBound{}, all, 0, false)
	if len(es) != 5 || string(es[3].Fields[1]) != "3" {
		t.Fatalf("range = %+v", es)
	}
	rev, _ := rs.Range("s", restypes.StreamBound{}, all, 2, true)
	if len(rev) != 2 || rev[0].ID != ids[4] || rev[1].ID != ids[3] {
		t.Fatalf("rev range = %+v", rev)
	}
	ex, _ := rs.Range("s", restypes.StreamBound{ID: ids[1], Excl: true}, restypes.StreamBound{ID: ids[3], Excl: true}, 0, false)
	if len(ex) != 1 || ex[0].ID != ids[2] {
		t.Fatalf("exclusive range = %+v", ex)
	}

	if n, _ := rs.Del("s", ids[0], ids[0], restypes.StreamID{Ms: 1}); n != 1 {
		t.Fatalf("XDEL = %d", n)
	}
	if n, _ := rs.Trim("s", restypes.StreamTrim{ByMaxLen: true, MaxLen: 2}); n != 2 {
		t.Fatalf("XTRIM = %d", n)
	}
	es, _ = rs.Range("s", restypes.StreamBound{}, all, 0, false)
	if len(es) != 2 || es[0].ID != ids[3] {
		t.Fatalf("after trim = %+v", es)
	}
	if n, _ := rs.Trim("s", restypes.StreamTrim{ByMinID: true, MinID: ids[4]}); n != 1 {
		t.Fatalf("XTRIM MINID = %d", n)
	}

	// Deleting every entry keeps the stream (Redis); IDs never go backwards.
	rs.Del("s", ids[4])
	if ok, _ := st.Redka(0).Key().Exists("s"); !ok {
		t.Fatal("empty stream should still exist")
	}
	id, _, _ := rs.Add("s", restypes.StreamAddOpts{Mode: restypes.IDAuto}, fv("a", "b"))
	if id.Cmp(ids[4]) <= 0 {
		t.Fatalf("id went backwards after deleting everything: %v <= %v", id, ids[4])
	}

	// NOMKSTREAM, wrong type, rename, delete.
	if _, ok, _ := rs.Add("nope", restypes.StreamAddOpts{NoMkStream: true, Mode: restypes.IDAuto}, fv("a", "b")); ok {
		t.Fatal("NOMKSTREAM created a stream")
	}
	st.Redka(0).Str().Set("str", "x")
	if _, _, err := rs.Add("str", restypes.StreamAddOpts{Mode: restypes.IDAuto}, fv("a", "b")); err == nil {
		t.Fatal("XADD on a string should fail")
	}
	if err := st.Redka(0).Key().Rename("s", "s2"); err != nil {
		t.Fatal(err)
	}
	if n, _ := rs.Len("s2"); n != 1 {
		t.Fatalf("renamed len = %d", n)
	}
	if n, _ := st.Redka(0).Key().Delete("s2"); n != 1 {
		t.Fatalf("DEL = %d", n)
	}
	if ok, _ := st.Redka(0).Key().Exists("s2"); ok {
		t.Fatal("deleted stream exists")
	}
}

var maxID = restypes.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}

// TestStreamConvergence: concurrent XADDs on partitioned replicas, in the same
// millisecond, all survive with distinct IDs and the same order everywhere.
func TestStreamConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)
	frozen := nowMS
	nowMS = func() int64 { return 1_700_000_000_000 } // force same-ms collisions
	defer func() { nowMS = frozen }()

	stores[0].Redka(0).Stream().Add("s", restypes.StreamAddOpts{Mode: restypes.IDAuto}, fv("seed", "0"))
	eventually(t, "r1 sees seed", func() bool {
		n, _ := stores[1].Redka(0).Stream().Len("s")
		return n == 1
	})
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	for i := 0; i < 5; i++ {
		for r := 0; r < 2; r++ {
			if _, _, err := stores[r].Redka(0).Stream().Add("s", restypes.StreamAddOpts{Mode: restypes.IDAuto}, fv("r", fmt.Sprint(r), "i", fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)

	var views [2][]restypes.StreamEntry
	for r := 0; r < 2; r++ {
		r := r
		eventually(t, fmt.Sprintf("r%d sees 11 entries", r), func() bool {
			views[r], _ = stores[r].Redka(0).Stream().Range("s", restypes.StreamBound{}, restypes.StreamBound{ID: maxID}, 0, false)
			return len(views[r]) == 11
		})
	}
	for i := range views[0] {
		if views[0][i].ID != views[1][i].ID {
			t.Fatalf("replicas disagree at %d: %v vs %v", i, views[0][i].ID, views[1][i].ID)
		}
		if i > 0 && views[0][i].ID.Cmp(views[0][i-1].ID) <= 0 {
			t.Fatalf("order not strictly increasing at %d", i)
		}
	}
}
