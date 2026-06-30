package crdtstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	redis "github.com/redis-geo/redgeo/redisapi"

	"github.com/redis-geo/redgeo/engine"
)

// newCluster builds n in-process replicas wired over a memory gossip network
// with a shared DAG service (DESIGN §9 phase 8). Returns the stores and the
// network control handle (for partition/heal).
func newCluster(t *testing.T, n int) ([]*Store, *engine.MemNetwork) {
	t.Helper()
	net, bcasts, dag := engine.NewMemNetwork(n)
	stores := make([]*Store, n)
	for i := 0; i < n; i++ {
		eng, err := engine.New(context.Background(), engine.Config{
			ReplicaID:   fmt.Sprintf("r%d", i),
			Broadcaster: bcasts[i],
			DAGService:  dag,
			// Fast anti-entropy so healed partitions converge quickly in tests.
			RebroadcastInterval: 200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("replica %d: %v", i, err)
		}
		t.Cleanup(func() { eng.Close() })
		stores[i] = NewStore(eng)
	}
	return stores, net
}

// eventually polls cond until it holds or the deadline passes.
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

func get(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, err := s.Redka(0).Str().Get(key)
	if err != nil {
		return "", false
	}
	return v.String(), true
}

// TestReplicationBasic: a write on one replica becomes visible on the others.
func TestReplicationBasic(t *testing.T) {
	stores, _ := newCluster(t, 3)
	if err := stores[0].Redka(0).Str().Set("k", "hello"); err != nil {
		t.Fatalf("set: %v", err)
	}
	for i := 1; i < 3; i++ {
		i := i
		eventually(t, fmt.Sprintf("r%d sees k", i), func() bool {
			v, ok := get(t, stores[i], "k")
			return ok && v == "hello"
		})
	}
}

// TestCounterConvergence: concurrent increments on partitioned replicas sum
// correctly after heal — the §6.4 PN-counter showcase.
func TestCounterConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)

	// Establish the counter and let it converge.
	if _, err := stores[0].Redka(0).Str().Incr("c", 1); err != nil {
		t.Fatalf("incr: %v", err)
	}
	eventually(t, "r1 sees c=1", func() bool { v, ok := get(t, stores[1], "c"); return ok && v == "1" })

	// Partition, increment independently on each side.
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	if _, err := stores[0].Redka(0).Str().Incr("c", 5); err != nil { // r0: 1 -> 6
		t.Fatalf("incr r0: %v", err)
	}
	if _, err := stores[1].Redka(0).Str().Incr("c", 3); err != nil { // r1: +3
		t.Fatalf("incr r1: %v", err)
	}

	// Heal: both must converge to the SUM (1 + 5 + 3 = 9).
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i := 0; i < 2; i++ {
		i := i
		eventually(t, fmt.Sprintf("r%d sees c=9", i), func() bool {
			v, ok := get(t, stores[i], "c")
			return ok && v == "9"
		})
	}
}

// TestSetAddWins: a concurrent SADD and SREM of the same member resolves to the
// member present (§6.3 Add-Wins OR-Set).
func TestSetAddWins(t *testing.T) {
	stores, net := newCluster(t, 2)
	r0 := stores[0].Redka(0).Set()

	// Both replicas know member x.
	if _, err := r0.Add("s", "x"); err != nil {
		t.Fatalf("sadd: %v", err)
	}
	eventually(t, "r1 sees x", func() bool {
		ok, _ := stores[1].Redka(0).Set().Exists("s", "x")
		return ok
	})

	// Partition; concurrently remove on r0 and (re-)add on r1.
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	if _, err := r0.Delete("s", "x"); err != nil {
		t.Fatalf("srem r0: %v", err)
	}
	if _, err := stores[1].Redka(0).Set().Add("s", "x"); err != nil {
		t.Fatalf("sadd r1: %v", err)
	}

	// Heal: add wins -> x present on BOTH replicas.
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i := 0; i < 2; i++ {
		i := i
		eventually(t, fmt.Sprintf("r%d: add wins, x present", i), func() bool {
			ok, _ := stores[i].Redka(0).Set().Exists("s", "x")
			return ok
		})
	}
}

// TestRegisterLWWConverges: concurrent SETs to the same key converge to a
// single value on all replicas (§6.7 HLC-LWW slots).
func TestRegisterLWWConverges(t *testing.T) {
	stores, net := newCluster(t, 2)

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	_ = stores[0].Redka(0).Str().Set("k", "from-r0")
	time.Sleep(5 * time.Millisecond) // ensure distinct HLC wall component
	_ = stores[1].Redka(0).Str().Set("k", "from-r1")

	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)

	// Both replicas must agree on the SAME winning value.
	var converged string
	eventually(t, "replicas agree on k", func() bool {
		v0, ok0 := get(t, stores[0], "k")
		v1, ok1 := get(t, stores[1], "k")
		if ok0 && ok1 && v0 == v1 {
			converged = v0
			return true
		}
		return false
	})
	if converged != "from-r0" && converged != "from-r1" {
		t.Fatalf("converged to unexpected value %q", converged)
	}
}

var _ = redis.Redka{}

// TestHashCounterConvergence: concurrent HINCRBY on the same field across
// partitioned replicas sums correctly after heal (§6.4 PN-counter for hash
// fields).
func TestHashCounterConvergence(t *testing.T) {
	stores, net := newCluster(t, 2)

	if _, err := stores[0].Redka(0).Hash().Incr("h", "clicks", 1); err != nil {
		t.Fatalf("hincrby: %v", err)
	}
	eventually(t, "r1 sees clicks=1", func() bool {
		v, err := stores[1].Redka(0).Hash().Get("h", "clicks")
		return err == nil && v.String() == "1"
	})

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	if _, err := stores[0].Redka(0).Hash().Incr("h", "clicks", 5); err != nil {
		t.Fatalf("hincrby r0: %v", err)
	}
	if _, err := stores[1].Redka(0).Hash().Incr("h", "clicks", 4); err != nil {
		t.Fatalf("hincrby r1: %v", err)
	}

	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	for i := 0; i < 2; i++ {
		i := i
		eventually(t, "hash counter sums to 10", func() bool {
			v, err := stores[i].Redka(0).Hash().Get("h", "clicks")
			return err == nil && v.String() == "10"
		})
	}
}

// TestHashCounterFlavorRejection: HSET and HINCRBY don't mix on a field.
func TestHashCounterFlavorRejection(t *testing.T) {
	st := newTestStore(t)
	h := st.Redka(0).Hash()

	if _, err := h.Incr("h", "n", 1); err != nil {
		t.Fatalf("hincrby new: %v", err)
	}
	if _, err := h.Set("h", "n", "x"); err == nil {
		t.Fatal("HSET on a counter field should be rejected")
	}
	if _, err := h.Set("h", "name", "alice"); err != nil {
		t.Fatalf("hset lww: %v", err)
	}
	if _, err := h.Incr("h", "name", 1); err == nil {
		t.Fatal("HINCRBY on an LWW field should be rejected")
	}
}
