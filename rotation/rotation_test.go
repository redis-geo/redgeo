package rotation_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
	"github.com/redis-geo/redgeo/rotation"
)

type replica struct {
	id   string
	eng  *engine.Engine
	st   *crdtstore.Store
	node *rotation.Node
}

// newCluster builds n in-process replicas with `parts` partition DAGs over the
// in-memory network, each with a rotation node on the control channel.
func newCluster(t *testing.T, n, parts int, start bool) ([]*replica, *engine.MemNetwork) {
	t.Helper()
	net, factories, dag := engine.NewMemNetworkP(n, parts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rs := make([]*replica, n)
	for i := range rs {
		id := fmt.Sprintf("r%d", i)
		cfg := engine.Config{
			ReplicaID: id, DAGService: dag, NumPartitions: parts,
			RebroadcastInterval: 150 * time.Millisecond,
		}
		if parts > 1 {
			cfg.BroadcasterFactory = factories[i]
		} else {
			cfg.Broadcaster = factories[i](0)
		}
		eng, err := engine.New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { eng.Close() })
		st := crdtstore.NewStore(eng)
		if err := st.RegisterSelf(ctx); err != nil {
			t.Fatal(err)
		}
		node := rotation.New(rotation.Config{
			Engine: eng, ID: id, Transport: net.Ctl(i), Observe: st.Replicas, Unregister: st.UnregisterReplica,
			PrepareTimeout: 4 * time.Second, SwitchTimeout: 8 * time.Second,
			HelloEvery: 100 * time.Millisecond, PollEvery: 40 * time.Millisecond,
		})
		if start {
			node.Start(ctx)
		}
		rs[i] = &replica{id: id, eng: eng, st: st, node: node}
	}
	return rs, net
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func members(r *replica, key string) map[string]bool {
	ms, _ := r.st.Redka(0).Set().Items(key)
	out := map[string]bool{}
	for _, m := range ms {
		out[m.String()] = true
	}
	return out
}

func sameSet(got map[string]bool, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !got[w] {
			return false
		}
	}
	return true
}

func gens(rs []*replica, part int) []int {
	out := make([]int, len(rs))
	for i, r := range rs {
		out[i] = r.eng.Generation(part)
	}
	return out
}

func waitMembers(t *testing.T, rs []*replica) {
	t.Helper()
	eventually(t, "members discovered", func() bool {
		for _, r := range rs {
			if len(r.node.Members()) != len(rs)-1 {
				return false
			}
		}
		return true
	})
}

func TestClusterCompaction(t *testing.T) {
	rs, _ := newCluster(t, 3, 1, true)
	waitMembers(t, rs)
	ctx := context.Background()

	rs[0].st.Redka(0).Str().Set("a", "1")
	rs[0].st.Redka(0).Set().Add("s", "x", "y", "z")
	eventually(t, "r1 sees s", func() bool { return sameSet(members(rs[1], "s"), "x", "y", "z") })
	rs[1].st.Redka(0).Set().Delete("s", "y")
	rs[1].st.Redka(0).Set().Add("s", "w")
	for i := 0; i < 20; i++ { // churn: build DAG height and tombstones
		r := rs[i%3] // add and delete on one replica so the delete observes the add
		r.st.Redka(0).Set().Add("churn", fmt.Sprint(i))
		r.st.Redka(0).Set().Delete("churn", fmt.Sprint(i))
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" converged", func() bool {
			return sameSet(members(r, "s"), "x", "z", "w") && len(members(r, "churn")) == 0
		})
	}
	before := rs[0].eng.Stats(ctx).MaxHeight

	if err := rs[1].node.Compact(ctx, 0); err != nil {
		t.Fatalf("compact: %v", err)
	}
	for i, g := range gens(rs, 0) {
		if g != 1 {
			t.Fatalf("r%d generation = %d, want 1", i, g)
		}
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" has post-compaction data", func() bool {
			v, err := r.st.Redka(0).Str().Get("a")
			return err == nil && v.String() == "1" && sameSet(members(r, "s"), "x", "z", "w")
		})
		if h := r.eng.Stats(ctx).MaxHeight; h >= before {
			t.Fatalf("%s DAG height %d not reduced from %d", r.id, h, before)
		}
		if r.eng.InDoubt(0) {
			t.Fatalf("%s still in doubt", r.id)
		}
	}

	// The cluster keeps working in both directions on the new generation.
	rs[2].st.Redka(0).Set().Add("s", "new2")
	rs[0].st.Redka(0).Str().Set("b", "2")
	for _, r := range rs {
		r := r
		eventually(t, r.id+" sees new writes", func() bool {
			v, err := r.st.Redka(0).Str().Get("b")
			return err == nil && v.String() == "2" && sameSet(members(r, "s"), "x", "z", "w", "new2")
		})
	}
	// Several anti-entropy rounds later nothing deleted has come back.
	time.Sleep(700 * time.Millisecond)
	for _, r := range rs {
		if m := members(r, "s"); !sameSet(m, "x", "z", "w", "new2") {
			t.Fatalf("%s resurrected state: %v", r.id, m)
		}
	}
}

// A replica's un-synced writes made while partitioned survive a compaction
// started right after it heals: the protocol waits for convergence first.
func TestCompactionWaitsForConvergence(t *testing.T) {
	rs, net := newCluster(t, 3, 1, true)
	waitMembers(t, rs)
	rs[0].st.Redka(0).Set().Add("s", "base")
	eventually(t, "synced", func() bool { return sameSet(members(rs[2], "s"), "base") })

	net.SetPartitioned(2, true)
	rs[2].st.Redka(0).Set().Add("s", "lonely")
	rs[2].st.Redka(0).Set().Delete("s", "base")
	net.SetPartitioned(2, false)

	if err := rs[0].node.Compact(context.Background(), 0); err != nil {
		t.Fatalf("compact: %v", err)
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" has r2's writes", func() bool { return sameSet(members(r, "s"), "lonely") })
	}
	time.Sleep(500 * time.Millisecond)
	for _, r := range rs {
		if !sameSet(members(r, "s"), "lonely") {
			t.Fatalf("%s: %v", r.id, members(r, "s"))
		}
	}
}

func TestCompactionRefusedWhenMemberUnreachable(t *testing.T) {
	rs, net := newCluster(t, 3, 1, true)
	waitMembers(t, rs)
	rs[0].st.Redka(0).Set().Add("s", "a")
	eventually(t, "synced", func() bool { return sameSet(members(rs[2], "s"), "a") })

	net.SetPartitioned(2, true)
	err := rs[0].node.Compact(context.Background(), 0)
	if !errors.Is(err, rotation.ErrMemberUnavailable) {
		t.Fatalf("compact with a member down = %v, want ErrMemberUnavailable", err)
	}
	for i, g := range gens(rs, 0) {
		if g != 0 {
			t.Fatalf("r%d rotated despite refusal (gen %d)", i, g)
		}
	}
	// Nothing stays frozen: writes work on the surviving side.
	done := make(chan error, 1)
	go func() { _, err := rs[0].st.Redka(0).Set().Add("s", "b"); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writes still blocked after a refused compaction")
	}

	// After healing it succeeds; a retired member can be forgotten instead.
	net.SetPartitioned(2, false)
	if err := rs[0].node.Compact(context.Background(), 0); err != nil {
		t.Fatalf("compact after heal: %v", err)
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" has data", func() bool { return sameSet(members(r, "s"), "a", "b") })
	}
}

func TestForgetRetiredMember(t *testing.T) {
	rs, net := newCluster(t, 3, 1, true)
	waitMembers(t, rs)
	net.SetPartitioned(2, true)
	rs[0].node.Forget("r2")
	rs[1].node.Forget("r2")
	rs[0].st.Redka(0).Str().Set("k", "v")
	eventually(t, "r1 sees k", func() bool { v, err := rs[1].st.Redka(0).Str().Get("k"); return err == nil && v.String() == "v" })
	// r2 keeps announcing itself... but it is partitioned, so it stays forgotten.
	if err := rs[0].node.Compact(context.Background(), 0); err != nil {
		t.Fatalf("compact without the forgotten member: %v", err)
	}
}

func TestCompactionRefusedWhenMemberBusy(t *testing.T) {
	rs, _ := newCluster(t, 3, 1, true)
	waitMembers(t, rs)
	ctx := context.Background()
	// r2 is mid-way through some other rotation.
	if err := rs[2].eng.Freeze(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rs[2].eng.Stage(ctx, 0, false); err != nil {
		t.Fatal(err)
	}
	err := rs[0].node.Compact(ctx, 0)
	if !errors.Is(err, rotation.ErrBusy) {
		t.Fatalf("compact with a busy member = %v, want ErrBusy", err)
	}
	if rs[0].eng.Generation(0) != 0 || rs[1].eng.Generation(0) != 0 {
		t.Fatal("rotated despite a busy member")
	}
	if err := rs[0].st.Redka(0).Str().Set("k", "v"); err != nil {
		t.Fatalf("write after abort: %v", err)
	}
}

// No acknowledged write is lost to a compaction running underneath it.
func TestWritesDuringCompaction(t *testing.T) {
	rs, _ := newCluster(t, 3, 1, true)
	waitMembers(t, rs)

	var wg sync.WaitGroup
	var acked sync.Map
	var stop atomic.Bool
	var failures atomic.Int32
	for w := 0; w < 3; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				m := fmt.Sprintf("w%d-%d", w, i)
				if _, err := rs[w].st.Redka(0).Set().Add("s", m); err != nil {
					failures.Add(1)
					continue
				}
				acked.Store(m, true)
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	for round := 0; round < 2; round++ {
		if err := rs[round].node.Compact(context.Background(), 0); err != nil {
			t.Fatalf("compact %d: %v", round, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d writes failed during compaction", failures.Load())
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" has every acknowledged write", func() bool {
			got := members(r, "s")
			ok := true
			acked.Range(func(k, _ any) bool {
				if !got[k.(string)] {
					ok = false
				}
				return ok
			})
			return ok
		})
	}
	for i, g := range gens(rs, 0) {
		if g != 2 {
			t.Fatalf("r%d generation = %d, want 2", i, g)
		}
	}
}

func TestRollingCompactionAcrossPartitions(t *testing.T) {
	rs, _ := newCluster(t, 3, 4, true)
	waitMembers(t, rs)
	for i := 0; i < 40; i++ {
		rs[i%3].st.Redka(0).Str().Set(fmt.Sprintf("key%d", i), fmt.Sprint(i))
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" converged", func() bool { n, _ := r.st.Redka(0).Key().Len(); return n == 40 })
	}
	if err := rs[2].node.CompactAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	for p := 0; p < 4; p++ {
		for i, g := range gens(rs, p) {
			if g != 1 {
				t.Fatalf("partition %d r%d generation = %d", p, i, g)
			}
		}
	}
	for _, r := range rs {
		r := r
		eventually(t, r.id+" has all keys", func() bool {
			for i := 0; i < 40; i++ {
				v, err := r.st.Redka(0).Str().Get(fmt.Sprintf("key%d", i))
				if err != nil || v.String() != fmt.Sprint(i) {
					return false
				}
			}
			return true
		})
	}
}

func TestSingleNodeCompaction(t *testing.T) {
	rs, _ := newCluster(t, 1, 1, true)
	rs[0].st.Redka(0).Str().Set("k", "v")
	if err := rs[0].node.Compact(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if v, err := rs[0].st.Redka(0).Str().Get("k"); err != nil || v.String() != "v" || rs[0].eng.Generation(0) != 1 {
		t.Fatalf("after compaction: %v %v gen=%d", v, err, rs[0].eng.Generation(0))
	}
}

// A replica that activated a new generation but missed the outcome (crash)
// resolves it from its peers on restart.
func TestInDoubtResolution(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		peerRotated  bool // the peer finished the rotation (commit) or never started it (rollback)
		wantGen      int
		wantStillSet bool
	}{
		{"peer committed", true, 1, true},
		{"peer never rotated", false, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs, _ := newCluster(t, 2, 1, false)
			rs[0].st.Redka(0).Set().Add("s", "x")
			eventually(t, "synced", func() bool { return sameSet(members(rs[1], "s"), "x") })

			// r1 crashed after Activate: frozen, new generation active, old kept.
			if err := rs[1].eng.Freeze(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if _, _, err := rs[1].eng.Stage(ctx, 0, true); err != nil {
				t.Fatal(err)
			}
			if err := rs[1].eng.Activate(ctx, 0, false); err != nil {
				t.Fatal(err)
			}
			if tc.peerRotated {
				if _, err := rs[0].eng.RotatePartition(ctx, 0); err != nil {
					t.Fatal(err)
				}
			}
			// Both nodes (re)start their protocol loops.
			nctx, cancel := context.WithCancel(ctx)
			defer cancel()
			rs[0].node.Start(nctx)
			rs[1].node.Start(nctx)

			eventually(t, "r1 settled", func() bool { return !rs[1].eng.InDoubt(0) && rs[1].eng.Generation(0) == tc.wantGen })
			if _, err := rs[1].st.Redka(0).Set().Add("s", "after"); err != nil {
				t.Fatalf("write after resolution: %v", err)
			}
			eventually(t, "r1 data", func() bool { return members(rs[1], "s")["x"] && members(rs[1], "s")["after"] })
		})
	}
}
