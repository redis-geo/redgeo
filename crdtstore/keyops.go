package crdtstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis-geo/redgeo/core"
)

func trimPrefix(s, p string) string { return strings.TrimPrefix(s, p) }

// deleteKey removes a whole logical key of the given type. Slot-based
// registers (string value, hash fields, zset scores, meta) are deleted by
// writing a deleted-tagged slot at this replica with a fresh HLC (so it wins
// the max-HLC read, DESIGN §6.7) — NOT by ds.Delete, which would only tombstone
// one replica's slot and let another replica's present slot win. Presence-only
// members (set, list) are deleted with ds.Delete (OR-Set remove, §6.3).
func (s *Store) deleteKey(ctx context.Context, db int, key string, typ core.TypeID) error {
	switch typ {
	case core.TypeString:
		// A string key is either a plain register or a PN-counter (§6.4).
		if m, _, ok, err := s.readMeta(ctx, db, key); err == nil && ok && m.Flavor.isCounter() {
			return s.deleteCounter(ctx, db, key)
		}
		// Tombstone this replica's value slot, then meta.
		if err := s.writeSlot(ctx, strSlot(db, key, s.replica()), tagDeleted, nil); err != nil {
			return err
		}
		return s.deleteMeta(ctx, db, key)
	case core.TypeHash:
		fields, err := s.hashLiveFields(ctx, db, key)
		if err != nil {
			return err
		}
		for field := range fields {
			if err := s.writeSlot(ctx, hashSlot(db, key, field, s.replica()), tagDeleted, nil); err != nil {
				return err
			}
		}
		return s.deleteMeta(ctx, db, key)
	case core.TypeSet:
		entries, err := s.eng.QueryPrefix(ctx, setBase(db, key), true)
		if err != nil {
			return err
		}
		base := setBase(db, key)
		for _, e := range entries {
			m, derr := decSeg(trimPrefix(e.Key, base))
			if derr != nil {
				continue
			}
			if err := s.eng.Delete(ctx, setMember(db, key, m)); err != nil {
				return err
			}
		}
		return s.deleteMeta(ctx, db, key)
	case core.TypeZSet:
		scores, err := s.zsetLiveScores(ctx, db, key)
		if err != nil {
			return err
		}
		for member := range scores {
			if err := s.writeSlot(ctx, zsetSlot(db, key, member, s.replica()), tagDeleted, nil); err != nil {
				return err
			}
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
		m, _, _, err := s.readMeta(ctx, db, key)
		if err != nil {
			return err
		}
		if m.Flavor.isCounter() {
			// Collapse the counter to a single component on this replica under
			// the new key, preserving the total.
			var comp []byte
			if m.Flavor == flavorCounter {
				sum, err := s.counterSumInt(ctx, db, key)
				if err != nil {
					return err
				}
				comp = []byte(fmt.Sprintf("%d", sum))
			} else {
				sum, err := s.counterSumFloat(ctx, db, key)
				if err != nil {
					return err
				}
				comp = ftoa(sum)
			}
			if err := s.eng.Put(ctx, counterSlot(db, newKey, s.replica()), comp); err != nil {
				return err
			}
			return s.writeMeta(ctx, db, newKey, metaEnvelope{
				KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: m.ETimeMS},
				Flavor:  m.Flavor,
			})
		}
		cands, err := s.readSlots(ctx, strBase(db, key))
		if err != nil {
			return err
		}
		v, live := liveValue(cands)
		if !live {
			return nil
		}
		if err := s.writeSlot(ctx, strSlot(db, newKey, s.replica()), tagPresent, v); err != nil {
			return err
		}
		return s.writeMeta(ctx, db, newKey, metaEnvelope{
			KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: m.ETimeMS},
			Flavor:  flavorString,
		})
	case core.TypeHash:
		fields, err := s.hashLiveFields(ctx, db, key)
		if err != nil {
			return err
		}
		for field, v := range fields {
			if err := s.writeSlot(ctx, hashSlot(db, newKey, field, s.replica()), tagPresent, v.Bytes()); err != nil {
				return err
			}
		}
		return s.writeMeta(ctx, db, newKey, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeHash}})
	case core.TypeSet:
		entries, err := s.eng.QueryPrefix(ctx, setBase(db, key), true)
		if err != nil {
			return err
		}
		base := setBase(db, key)
		for _, e := range entries {
			m, derr := decSeg(trimPrefix(e.Key, base))
			if derr != nil {
				continue
			}
			if err := s.eng.Put(ctx, setMember(db, newKey, m), presence); err != nil {
				return err
			}
		}
		return s.writeMeta(ctx, db, newKey, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeSet}})
	case core.TypeZSet:
		scores, err := s.zsetLiveScores(ctx, db, key)
		if err != nil {
			return err
		}
		for member, sc := range scores {
			if err := s.writeSlot(ctx, zsetSlot(db, newKey, member, s.replica()), tagPresent, encodeScore(sc)); err != nil {
				return err
			}
		}
		return s.writeMeta(ctx, db, newKey, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeZSet}})
	default:
		return fmt.Errorf("crdtstore: RENAME of type %s not yet supported", k.TypeName())
	}
}
