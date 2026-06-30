package crdtstore

import (
	"context"
	"fmt"

	"github.com/redis-geo/redgeo/core"
)

// deleteKey removes a whole logical key of the given type. Slot-based
// registers (string value, hash fields, zset scores, meta) are deleted by
// writing a deleted-tagged slot at this replica with a fresh HLC (so it wins
// the max-HLC read, DESIGN §6.7) — NOT by ds.Delete, which would only tombstone
// one replica's slot and let another replica's present slot win. Presence-only
// members (set, list) are deleted with ds.Delete (OR-Set remove, §6.3).
func (s *Store) deleteKey(ctx context.Context, db int, key string, typ core.TypeID) error {
	switch typ {
	case core.TypeString:
		// Tombstone this replica's value (and counter) slot, then meta.
		if err := s.writeSlot(ctx, strSlot(db, key, s.replica()), tagDeleted, nil); err != nil {
			return err
		}
		return s.deleteMeta(ctx, db, key)
	default:
		// Types whose deletion lands in a later phase. Until then no data of
		// these types exists; tombstone meta so the key reads as absent.
		return s.deleteMeta(ctx, db, key)
	}
}

// copyKey copies a logical key's live data to newKey (used by RENAME). Per-type
// like deleteKey; strings now, other types in their phases.
func (s *Store) copyKey(ctx context.Context, db int, key, newKey string, k core.Key) error {
	switch k.Type {
	case core.TypeString:
		cands, err := s.readSlots(ctx, strBase(db, key))
		if err != nil {
			return err
		}
		v, live := liveValue(cands)
		if !live {
			return nil
		}
		m, _, _, err := s.readMeta(ctx, db, key)
		if err != nil {
			return err
		}
		if err := s.writeSlot(ctx, strSlot(db, newKey, s.replica()), tagPresent, v); err != nil {
			return err
		}
		return s.writeMeta(ctx, db, newKey, metaEnvelope{
			KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: m.ETimeMS},
			Flavor:  flavorString,
		})
	default:
		return fmt.Errorf("crdtstore: RENAME of type %s not yet supported", k.TypeName())
	}
}
