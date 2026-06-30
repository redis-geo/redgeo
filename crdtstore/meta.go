package crdtstore

import (
	"context"
	"encoding/binary"

	"github.com/redis-geo/redgeo/core"
)

// KeyMeta carries a logical key's type, expiry, and bookkeeping (DESIGN §5.3).
// It is NOT the source of truth for collection existence (that is derived from
// live members, §5.2) — it records type + TTL + epoch only.
type KeyMeta struct {
	Type    core.TypeID // 1 str, 2 list, 3 set, 4 hash, 5 zset
	ETimeMS int64       // absolute expiry, unix ms; 0 = no expiry
	Epoch   uint32      // bumped on full DEL+recreate to fence stale members
}

// strFlavor distinguishes a plain string from a counter under TypeString
// (DESIGN §6.4: counters and plain strings are distinct flavors and don't mix).
// Stored in the high bytes of the meta envelope so a plain SET and an INCR on
// the same key can be rejected against each other.
type strFlavor uint8

const (
	flavorNone    strFlavor = 0 // non-string types
	flavorString  strFlavor = 1 // plain string register
	flavorCounter strFlavor = 2 // PN-counter (§6.4)
)

// metaEnvelope is KeyMeta plus the string flavor, encoded into a slot value.
type metaEnvelope struct {
	KeyMeta
	Flavor strFlavor
}

// encodeMeta packs a metaEnvelope: type(1) flavor(1) etime(8) epoch(4) = 14 B.
func encodeMeta(m metaEnvelope) []byte {
	b := make([]byte, 14)
	b[0] = byte(m.Type)
	b[1] = byte(m.Flavor)
	binary.BigEndian.PutUint64(b[2:10], uint64(m.ETimeMS))
	binary.BigEndian.PutUint32(b[10:14], m.Epoch)
	return b
}

func decodeMeta(b []byte) (metaEnvelope, bool) {
	if len(b) != 14 {
		return metaEnvelope{}, false
	}
	return metaEnvelope{
		KeyMeta: KeyMeta{
			Type:    core.TypeID(b[0]),
			ETimeMS: int64(binary.BigEndian.Uint64(b[2:10])),
			Epoch:   binary.BigEndian.Uint32(b[10:14]),
		},
		Flavor: strFlavor(b[1]),
	}, true
}

// readMeta returns the winning (max-HLC) meta envelope for a key and whether a
// live (present) meta record exists, plus the winning slot's wall-ms (MTime).
func (s *Store) readMeta(ctx context.Context, db int, key string) (metaEnvelope, int64, bool, error) {
	cands, err := s.readSlots(ctx, metaBase(db, key))
	if err != nil {
		return metaEnvelope{}, 0, false, err
	}
	best, _, found := winner(cands)
	if !found || best.tag == tagDeleted {
		return metaEnvelope{}, 0, false, nil
	}
	m, ok := decodeMeta(best.value)
	if !ok {
		return metaEnvelope{}, 0, false, nil
	}
	return m, best.stamp.WallMS, true, nil
}

// writeMeta writes this replica's present meta slot for a key.
func (s *Store) writeMeta(ctx context.Context, db int, key string, m metaEnvelope) error {
	return s.writeSlot(ctx, metaSlot(db, key, s.replica()), tagPresent, encodeMeta(m))
}

// deleteMeta writes this replica's deleted meta slot for a key.
func (s *Store) deleteMeta(ctx context.Context, db int, key string) error {
	return s.writeSlot(ctx, metaSlot(db, key, s.replica()), tagDeleted, nil)
}
