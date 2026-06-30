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
// confirm the global stability watermark, then rotate the DAG — snapshot live
// state into a fresh genesis datastore and drop the old one (with its
// accumulated tombstones). It is GATED on the watermark and refuses unless
// every expected replica has confirmed a cut.
//
// Rotation is single-node correct via engine.Rotate. In a cluster the gate
// must hold for ALL replicas and they rotate together in a maintenance window,
// else an un-rotated replica would merge the fresh genesis with its old DAG and
// resurrect tombstoned keys (§5.5).
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

	// Count live logical keys (for the report) before rotating.
	live := 0
	for db := 0; db < numDBs; db++ {
		keys, err := (keyRepo{s: c.store, db: db}).listKeys(ctx)
		if err != nil {
			return CompactReport{}, err
		}
		live += len(keys)
	}

	// Rotate the DAG: snapshot live state into a fresh genesis, drop the old
	// DAG and its tombstones.
	if _, err := c.store.eng.Rotate(ctx); err != nil {
		return CompactReport{}, err
	}
	return CompactReport{SafeCut: cut, LiveKeys: live, Performed: true}, nil
}
