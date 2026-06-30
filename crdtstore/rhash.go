package crdtstore

import (
	"context"
	"sort"
	"strings"

	"github.com/tidwall/match"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// hashRepo implements redisapi.RHash as a per-field LWW-Map (DESIGN §6.2):
// each field is a per-replica HLC-LWW slot, so concurrent writes to different
// fields always merge and same-field writes are last-writer-wins.
type hashRepo struct {
	s  *Store
	db int
}

// hashLiveFields returns the winning value of every live (present) field of a
// hash. Shared by hashRepo and the generic key probe.
func (s *Store) hashLiveFields(ctx context.Context, db int, key string) (map[string]core.Value, error) {
	base := hashBase(db, key)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return nil, err
	}
	// Group slot envelopes by field, keyed by replica.
	byField := make(map[string]map[string]slot)
	for _, e := range entries {
		rest := strings.TrimPrefix(e.Key, base) // {encField}/{encReplica}
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			continue
		}
		field, derr := decSeg(rest[:i])
		if derr != nil {
			continue
		}
		replica, derr := decSeg(rest[i+1:])
		if derr != nil {
			continue
		}
		sl, derr := decodeSlot(e.Value)
		if derr != nil {
			continue
		}
		if byField[field] == nil {
			byField[field] = make(map[string]slot)
		}
		byField[field][replica] = sl
	}
	out := make(map[string]core.Value, len(byField))
	for field, cands := range byField {
		if v, live := liveValue(cands); live {
			out[field] = core.Value(v)
		}
	}
	return out, nil
}

// ensureHashType verifies the key is absent or a hash, returning ErrKeyType
// otherwise. Caller holds the key lock.
func (r hashRepo) ensureHashType(ctx context.Context, key string) error {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return err
	}
	if ok && k.Type != core.TypeHash {
		return core.ErrKeyType
	}
	return nil
}

func (r hashRepo) fieldSlots(ctx context.Context, key, field string) (map[string]slot, error) {
	return r.s.readSlots(ctx, hashFieldBase(r.db, key, field))
}

func (r hashRepo) Get(key, field string) (core.Value, error) {
	ctx := bg()
	if err := r.ensureHashType(ctx, key); err != nil {
		return nil, err
	}
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return nil, err
	}
	v, live := liveValue(cands)
	if !live {
		return nil, core.ErrNotFound
	}
	return core.Value(v), nil
}

func (r hashRepo) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	ctx := bg()
	if err := r.ensureHashType(ctx, key); err != nil {
		return nil, err
	}
	out := make(map[string]core.Value, len(fields))
	for _, f := range fields {
		cands, err := r.fieldSlots(ctx, key, f)
		if err != nil {
			return nil, err
		}
		if v, live := liveValue(cands); live {
			out[f] = core.Value(v)
		}
	}
	return out, nil
}

func (r hashRepo) Exists(key, field string) (bool, error) {
	_, err := r.Get(key, field)
	switch err {
	case nil:
		return true, nil
	case core.ErrNotFound:
		return false, nil
	default:
		return false, err
	}
}

func (r hashRepo) Items(key string) (map[string]core.Value, error) {
	ctx := bg()
	if err := r.ensureHashType(ctx, key); err != nil {
		return nil, err
	}
	return r.s.hashLiveFields(ctx, r.db, key)
}

func (r hashRepo) Fields(key string) ([]string, error) {
	items, err := r.Items(key)
	if err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(items))
	for f := range items {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields, nil
}

func (r hashRepo) Values(key string) ([]core.Value, error) {
	fields, err := r.Fields(key)
	if err != nil {
		return nil, err
	}
	items, _ := r.Items(key)
	vals := make([]core.Value, len(fields))
	for i, f := range fields {
		vals[i] = items[f]
	}
	return vals, nil
}

func (r hashRepo) Len(key string) (int, error) {
	items, err := r.Items(key)
	return len(items), err
}

// setField writes one field's value slot. Caller holds the key lock and has
// verified the type. Returns whether the field was newly created.
func (r hashRepo) setField(ctx context.Context, key, field string, b []byte, created *bool) error {
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return err
	}
	_, live := liveValue(cands)
	if created != nil {
		*created = !live
	}
	if err := r.s.writeSlot(ctx, hashSlot(r.db, key, field, r.s.replica()), tagPresent, b); err != nil {
		return err
	}
	return r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeHash}})
}

func (r hashRepo) Set(key, field string, value any) (bool, error) {
	b, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return false, err
	}
	var created bool
	if err := r.setField(ctx, key, field, b, &created); err != nil {
		return false, err
	}
	return created, nil
}

func (r hashRepo) SetMany(key string, items map[string]any) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	created := 0
	for field, value := range items {
		b, err := core.ToBytes(value)
		if err != nil {
			return created, err
		}
		var c bool
		if err := r.setField(ctx, key, field, b, &c); err != nil {
			return created, err
		}
		if c {
			created++
		}
	}
	return created, nil
}

func (r hashRepo) SetNotExists(key, field string, value any) (bool, error) {
	b, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return false, err
	}
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return false, err
	}
	if _, live := liveValue(cands); live {
		return false, nil // field exists: HSETNX is a no-op
	}
	if err := r.setField(ctx, key, field, b, nil); err != nil {
		return false, err
	}
	return true, nil
}

func (r hashRepo) Delete(key string, fields ...string) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	n := 0
	for _, field := range fields {
		cands, err := r.fieldSlots(ctx, key, field)
		if err != nil {
			return n, err
		}
		if _, live := liveValue(cands); !live {
			continue
		}
		// LWW delete: write a deleted-tagged slot (wins max-HLC read).
		if err := r.s.writeSlot(ctx, hashSlot(r.db, key, field, r.s.replica()), tagDeleted, nil); err != nil {
			return n, err
		}
		n++
	}
	// If the hash is now empty, tombstone meta so the key reads as absent.
	if items, err := r.s.hashLiveFields(ctx, r.db, key); err == nil && len(items) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return n, nil
}

// Incr/IncrFloat: single-node placeholder on the field register (Phase 2).
// Phase 4 swaps in the per-replica PN-counter component codec (§6.4).
func (r hashRepo) Incr(key, field string, delta int) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	cur := 0
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return 0, err
	}
	if v, live := liveValue(cands); live {
		n, perr := core.Value(v).Int()
		if perr != nil {
			return 0, core.ErrValueType
		}
		cur = n
	}
	next := cur + delta
	if err := r.setField(ctx, key, field, []byte(itoa(next)), nil); err != nil {
		return 0, err
	}
	return next, nil
}

func (r hashRepo) IncrFloat(key, field string, delta float64) (float64, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	cur := 0.0
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return 0, err
	}
	if v, live := liveValue(cands); live {
		f, perr := core.Value(v).Float()
		if perr != nil {
			return 0, core.ErrValueType
		}
		cur = f
	}
	next := cur + delta
	if err := r.setField(ctx, key, field, ftoa(next), nil); err != nil {
		return 0, err
	}
	return next, nil
}

func (r hashRepo) Scan(key string, cursor int, pattern string, count int) (restypes.HashScanResult, error) {
	items, err := r.Items(key)
	if err != nil {
		return restypes.HashScanResult{}, err
	}
	fields := make([]string, 0, len(items))
	for f := range items {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	var out []restypes.HashItem
	for _, f := range fields {
		if pattern != "" && pattern != "*" && !match.Match(f, pattern) {
			continue
		}
		out = append(out, restypes.HashItem{Field: f, Value: items[f]})
	}
	return restypes.HashScanResult{Cursor: 0, Items: out}, nil
}
