package crdtstore

import (
	"context"
	"errors"

	"github.com/redis-geo/redgeo/hlc"
)

// ErrNotSafeToCompact is returned when the causal-stability watermark cannot
// confirm a safe cut, so compaction must not proceed (DESIGN §5.5).
var ErrNotSafeToCompact = errors.New("crdtstore: no causal-stability cut; refusing to compact")

// CompactReport summarizes a (would-be) global purge compaction.
type CompactReport struct {
	SafeCut   hlc.Stamp
	LiveKeys  int // distinct live logical keys snapshotted across all DBs
	Performed bool
}

// Compactor performs global-purge compaction (DESIGN §5.5 v1 strategy):
// confirm the global stability watermark, snapshot live state, rotate all DAGs,
// purge. It is GATED on the watermark — it refuses unless every expected
// replica has confirmed a cut.
//
// NOTE: the physical DAG rotation/purge depends on a PurgeDAG primitive that is
// not yet in the go-ds-crdt fork. Until it lands, Compact verifies the safety
// gate and snapshots the live state (the correctness-critical half), and
// reports Performed=false. This keeps the load-bearing watermark logic built
// and tested early, per §5.5, with the purge wired in when PurgeDAG exists.
type Compactor struct {
	store     *Store
	watermark *Watermark
}

// NewCompactor builds a compactor over a store and a watermark seeded with the
// expected replica set.
func NewCompactor(store *Store, watermark *Watermark) *Compactor {
	return &Compactor{store: store, watermark: watermark}
}

// Compact refreshes the watermark, confirms a safe cut, snapshots live state,
// and (once PurgeDAG exists) rotates. Returns ErrNotSafeToCompact if the cut
// can't be established.
func (c *Compactor) Compact(ctx context.Context) (CompactReport, error) {
	if err := c.store.ObserveFromSlots(ctx, c.watermark); err != nil {
		return CompactReport{}, err
	}
	cut, ok := c.watermark.SafeCut()
	if !ok {
		return CompactReport{}, ErrNotSafeToCompact
	}

	// Snapshot live state: the set that would seed the fresh genesis DAG.
	live := 0
	for db := 0; db < numDBs; db++ {
		keys, err := (keyRepo{s: c.store, db: db}).listKeys(ctx)
		if err != nil {
			return CompactReport{}, err
		}
		live += len(keys)
	}

	// PurgeDAG rotation goes here once the fork exposes it.
	return CompactReport{SafeCut: cut, LiveKeys: live, Performed: false}, nil
}
