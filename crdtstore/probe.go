package crdtstore

import (
	"context"
	"time"

	"github.com/redis-geo/redgeo/core"
)

// nowMS returns the current wall clock in unix ms. Indirected for tests.
var nowMS = func() int64 { return time.Now().UnixMilli() }

// probe resolves a logical key to its core.Key and liveness, applying lazy TTL
// expiry (DESIGN §6.8: a key past its ETime is invisible immediately). It is
// the generic basis for EXISTS/TYPE/DEL/SCAN and for type-mismatch checks.
//
// Existence is derived from live data, not from meta alone (§5.2): meta gives
// the declared type/TTL, then a per-type liveness check confirms ≥1 live
// member (or, for strings, a live value/counter component).
func (s *Store) probe(ctx context.Context, db int, key string) (core.Key, bool, error) {
	m, mtime, ok, err := s.readMeta(ctx, db, key)
	if err != nil || !ok {
		return core.Key{}, false, err
	}
	if m.ETimeMS > 0 && m.ETimeMS <= nowMS() {
		return core.Key{}, false, nil // lazily expired
	}
	live, err := s.isLive(ctx, db, key, m)
	if err != nil || !live {
		return core.Key{}, false, err
	}
	k := core.Key{Key: key, Type: m.Type, MTime: mtime}
	if m.ETimeMS > 0 {
		et := m.ETimeMS
		k.ETime = &et
	}
	return k, true, nil
}

// isLive reports whether the key has ≥1 live member for its declared type.
func (s *Store) isLive(ctx context.Context, db int, key string, m metaEnvelope) (bool, error) {
	switch m.Type {
	case core.TypeString:
		if m.Flavor.isCounter() {
			return s.counterExists(ctx, db, key)
		}
		cands, err := s.readSlots(ctx, strBase(db, key))
		if err != nil {
			return false, err
		}
		_, live := liveValue(cands)
		return live, nil
	case core.TypeSet:
		return s.anyPrefix(ctx, setBase(db, key))
	case core.TypeHash:
		return s.hashAnyLive(ctx, db, key)
	case core.TypeZSet:
		return s.zsetAnyLive(ctx, db, key)
	case core.TypeList:
		return s.anyPrefix(ctx, listBase(db, key))
	}
	return false, nil
}

// anyPrefix reports whether any key exists under prefix (used for presence-only
// member spaces such as sets and lists).
func (s *Store) anyPrefix(ctx context.Context, prefix string) (bool, error) {
	entries, err := s.eng.QueryPrefix(ctx, prefix, true)
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}

// ---- per-type liveness stubs filled in by later phases ----
// Until a type's repo lands, no data of that type exists, so these report
// "not live"; each is replaced with a real check in its phase.

func (s *Store) hashAnyLive(ctx context.Context, db int, key string) (bool, error) {
	items, err := s.hashLiveFields(ctx, db, key)
	return len(items) > 0, err
}
func (s *Store) zsetAnyLive(ctx context.Context, db int, key string) (bool, error) {
	scores, err := s.zsetLiveScores(ctx, db, key)
	return len(scores) > 0, err
}
