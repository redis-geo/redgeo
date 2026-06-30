package crdtstore

import (
	"context"
	"sort"
	"sync"

	"github.com/redis-geo/redgeo/hlc"
)

// Watermark tracks each replica's observed HLC progress and derives a
// causal-stability cut: a stamp every known replica has provably advanced past
// (DESIGN §5.5). Compaction MUST gate tombstone/DAG purging on this cut —
// purging a tombstone while any replica still holds the matching add un-synced
// would let anti-entropy resurrect a deleted key.
//
// This node learns a replica R's progress from the HLC stamps in R's slots that
// it has received (Observe). The safe cut is the MIN of all known replicas'
// high-water stamps: below it, this node has seen every known replica advance,
// so their already-synced state can't reference an older cut.
//
// Limits (documented, §5.5/§8): a fully correct watermark needs a per-pair sync
// matrix (what every replica knows about every other). This min-of-high-water
// approximation is safe only when the expected replica set is fully known and
// each replica's slots have actually propagated here; it refuses to produce a
// cut until every expected replica has been observed. That conservative refusal
// is the safety property — never purge on incomplete information.
type Watermark struct {
	mu       sync.Mutex
	expected map[string]struct{}  // replica IDs that must be confirmed
	high     map[string]hlc.Stamp // highest stamp observed per replica
}

// NewWatermark creates a watermark expecting the given replica set. An empty
// set means "unknown membership" → SafeCut always refuses (returns ok=false).
func NewWatermark(expectedReplicas []string) *Watermark {
	w := &Watermark{
		expected: make(map[string]struct{}, len(expectedReplicas)),
		high:     make(map[string]hlc.Stamp),
	}
	for _, r := range expectedReplicas {
		w.expected[r] = struct{}{}
	}
	return w
}

// Observe records that replica r has been seen at stamp s (monotonic high-water).
func (w *Watermark) Observe(r string, s hlc.Stamp) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cur, ok := w.high[r]; !ok || cur.Less(s) {
		w.high[r] = s
	}
}

// SafeCut returns the stamp every expected replica has advanced past, and true,
// once every expected replica has been observed. Otherwise it returns the zero
// stamp and false — the caller must NOT purge anything (DESIGN §5.5).
func (w *Watermark) SafeCut() (hlc.Stamp, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.expected) == 0 {
		return hlc.Stamp{}, false
	}
	var cut hlc.Stamp
	first := true
	for r := range w.expected {
		s, ok := w.high[r]
		if !ok {
			return hlc.Stamp{}, false // a replica's progress is unknown
		}
		if first || s.Less(cut) {
			cut, first = s, false
		}
	}
	return cut, true
}

// Confirmed reports which expected replicas have been observed (for INFO/debug).
func (w *Watermark) Confirmed() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.high))
	for r := range w.high {
		if _, ok := w.expected[r]; ok {
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out
}

// ObserveFromSlots scans the whole keyspace's slot stamps and feeds each
// replica's high-water stamp into the watermark. This is how a node refreshes
// its view of peer progress before considering a compaction.
func (s *Store) ObserveFromSlots(ctx context.Context, w *Watermark) error {
	// Walk all meta slots (every live key has one) to learn per-replica HLC
	// high-water marks. Meta is the most complete per-key, per-replica index.
	for db := 0; db < numDBs; db++ {
		for _, prefix := range dbMetaPrefixes(db) {
			entries, err := s.eng.QueryPrefix(ctx, prefix, false)
			if err != nil {
				return err
			}
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
				w.Observe(replica, sl.stamp)
			}
		}
	}
	return nil
}
