package crdtstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/engine"
)

// TestPartitionRoutingAndRotation runs a single node with multiple partition
// DAGs: keys spread across partitions by their bucket, reads route correctly,
// and rotating one partition leaves the rest intact (DESIGN §5.5).
func TestPartitionRoutingAndRotation(t *testing.T) {
	eng, err := engine.New(context.Background(), engine.Config{ReplicaID: "r1", NumPartitions: 8})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	st := NewStore(eng)
	red := st.Redka(0)
	ctx := context.Background()

	// Write keys that hash into a spread of partitions.
	keys := make([]string, 40)
	for i := range keys {
		keys[i] = fmt.Sprintf("item:%d", i)
		if err := red.Str().Set(keys[i], fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("set %s: %v", keys[i], err)
		}
	}
	// Confirm the keys actually spread across more than one partition DAG.
	seenParts := map[int]bool{}
	for _, k := range keys {
		seenParts[bucket(0, k)%8] = true
	}
	if len(seenParts) < 2 {
		t.Fatalf("keys did not spread across partitions: %v", seenParts)
	}
	// All keys read back correctly (routing works).
	for i, k := range keys {
		if v, err := red.Str().Get(k); err != nil || v.String() != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %s = %q,%v", k, v, err)
		}
	}

	// Rotate just one partition; every key must still be present.
	victim := bucket(0, "item:0") % 8
	if _, err := eng.RotatePartition(ctx, victim); err != nil {
		t.Fatalf("rotate partition %d: %v", victim, err)
	}
	for i, k := range keys {
		if v, err := red.Str().Get(k); err != nil || v.String() != fmt.Sprintf("v%d", i) {
			t.Fatalf("after rotation, get %s = %q,%v", k, v, err)
		}
	}
}

// newPartitionedCluster builds n in-process replicas each with `parts` partition
// DAGs, wired over a partition-aware memory network sharing a DAG service.
func newPartitionedCluster(t *testing.T, n, parts int) ([]*Store, *engine.MemNetwork) {
	t.Helper()
	net, factories, dag := engine.NewMemNetworkP(n, parts)
	stores := make([]*Store, n)
	for i := 0; i < n; i++ {
		eng, err := engine.New(context.Background(), engine.Config{
			ReplicaID:           fmt.Sprintf("r%d", i),
			BroadcasterFactory:  factories[i],
			DAGService:          dag,
			NumPartitions:       parts,
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

// TestPartitionedClusterConvergence verifies that writes across many partitions
// replicate between nodes — each partition DAG gossips on its own channel.
func TestPartitionedClusterConvergence(t *testing.T) {
	stores, _ := newPartitionedCluster(t, 2, 8)

	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("k:%d", i)
		if err := stores[0].Redka(0).Str().Set(keys[i], fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	for i, k := range keys {
		i, k := i, k
		eventually(t, "replica 1 sees "+k, func() bool {
			v, err := stores[1].Redka(0).Str().Get(k)
			return err == nil && v.String() == fmt.Sprintf("v%d", i)
		})
	}
}
