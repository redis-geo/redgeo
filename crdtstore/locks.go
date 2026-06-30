package crdtstore

import (
	"hash/fnv"
	"sync"
)

// lockManager is a sharded per-key mutex pool. redcon runs one goroutine per
// connection with no serialization, and go-ds-crdt processes local writes
// synchronously, so read-modify-write sequences (counters, NX checks, list
// end-pushes, type checks) must be made atomic within a node (DESIGN §4).
// Cross-node correctness comes from the CRDT encoding, not these locks.
type lockManager struct {
	shards []sync.Mutex
	mask   uint32
}

// newLockManager builds a lock manager with nShards mutexes (rounded up to a
// power of two so masking is cheap).
func newLockManager(nShards int) *lockManager {
	n := 1
	for n < nShards {
		n <<= 1
	}
	return &lockManager{shards: make([]sync.Mutex, n), mask: uint32(n - 1)}
}

func (m *lockManager) shardFor(logicalKey string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(logicalKey))
	return &m.shards[h.Sum32()&m.mask]
}

// Lock acquires the shard guarding logicalKey and returns the unlock func.
// Different logical keys may share a shard (false contention) but the same key
// always maps to the same shard, which is what correctness requires.
func (m *lockManager) Lock(logicalKey string) func() {
	mu := m.shardFor(logicalKey)
	mu.Lock()
	return mu.Unlock
}
