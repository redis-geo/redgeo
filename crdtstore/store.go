// Package crdtstore implements the six redgeo storage interfaces (RStr, RKey,
// RHash, RSet, RZSet, RList) against go-ds-crdt, using the flat-key CRDT
// encodings of DESIGN §5–§6: per-replica HLC-LWW slots for registers, native
// OR-Set for sets, summed per-replica components for counters.
package crdtstore

import (
	"context"

	ds "github.com/ipfs/go-datastore"

	"github.com/redis-geo/redgeo/engine"
	"github.com/redis-geo/redgeo/hlc"
)

// lockShards is the per-key lock pool size (DESIGN §4). Generous to keep false
// contention low across many connections.
const lockShards = 1024

// Store is the CRDT-backed implementation of the six storage interfaces. It is
// constructed once per node; per-connection database selection is bound by
// Redka(db), which returns repos scoped to a logical DB (DESIGN §6.11).
type Store struct {
	eng   *engine.Engine
	locks *lockManager
}

// NewStore builds a Store over an engine.
func NewStore(eng *engine.Engine) *Store {
	return &Store{eng: eng, locks: newLockManager(lockShards)}
}

// replica is this node's slot-owner identity.
func (s *Store) replica() string { return s.eng.Replica() }

// stamp issues the next local HLC stamp.
func (s *Store) stamp() hlc.Stamp { return s.eng.Clock().Now() }

// ---- slot helpers (DESIGN §6.7) ----

// readSlots prefix-scans base and decodes every per-replica slot envelope into
// a map keyed by replicaID (the trailing path segment). Malformed slots are
// skipped rather than failing the whole read.
func (s *Store) readSlots(ctx context.Context, base string) (map[string]slot, error) {
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return nil, err
	}
	out := make(map[string]slot, len(entries))
	for _, e := range entries {
		replicaEnc := lastSegment(e.Key)
		replica, derr := decSeg(replicaEnc)
		if derr != nil {
			continue
		}
		sl, derr := decodeSlot(e.Value)
		if derr != nil {
			continue
		}
		out[replica] = sl
	}
	return out, nil
}

// writeSlot writes this replica's slot at slotKey with a fresh HLC stamp.
func (s *Store) writeSlot(ctx context.Context, slotKey ds.Key, tag byte, value []byte) error {
	env := encodeSlot(slot{stamp: s.stamp(), tag: tag, value: value})
	return s.eng.Put(ctx, slotKey, env)
}

// lastSegment returns the final '/'-delimited component of a ds.Key string.
func lastSegment(key string) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[i+1:]
		}
	}
	return key
}

// bg is the default background context for store operations until request
// contexts are threaded through the command layer.
func bg() context.Context { return context.Background() }
