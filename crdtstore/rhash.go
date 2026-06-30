package crdtstore

import (
	"context"
	"sort"
	"strconv"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/tidwall/match"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// fmtNum formats a counter total like Redis: an integer-valued float without a
// decimal point, otherwise the minimal float representation.
func fmtNum(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// hashRepo implements redisapi.RHash as a per-field LWW-Map (DESIGN §6.2):
// each field is a per-replica HLC-LWW slot, so concurrent writes to different
// fields always merge and same-field writes are last-writer-wins.
type hashRepo struct {
	s  *Store
	db int
}

// hashLiveFields returns the winning value of every live field of a hash,
// handling both LWW fields (HSET) and PN-counter fields (HINCRBY, §6.4) in one
// scan. A field is one flavor or the other (no mixing); counter fields report
// the sum of their per-replica components. Shared by hashRepo and the probe.
func (s *Store) hashLiveFields(ctx context.Context, db int, key string) (map[string]core.Value, error) {
	base := hashBase(db, key)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return nil, err
	}
	lww := make(map[string]map[string]slot)        // field -> replica -> slot
	counter := make(map[string]map[string]float64) // field -> replica -> component
	for _, e := range entries {
		// rest is either "{encField}/{encReplica}" (LWW) or
		// "{encField}/c/{encReplica}" (counter component).
		rest := strings.TrimPrefix(e.Key, base)
		parts := strings.Split(rest, "/")
		switch {
		case len(parts) == 2:
			field, derr := decSeg(parts[0])
			if derr != nil {
				continue
			}
			replica, derr := decSeg(parts[1])
			if derr != nil {
				continue
			}
			sl, derr := decodeSlot(e.Value)
			if derr != nil {
				continue
			}
			if lww[field] == nil {
				lww[field] = make(map[string]slot)
			}
			lww[field][replica] = sl
		case len(parts) == 3 && parts[1] == "c":
			field, derr := decSeg(parts[0])
			if derr != nil {
				continue
			}
			n, perr := strconv.ParseFloat(string(e.Value), 64)
			if perr != nil {
				continue
			}
			if counter[field] == nil {
				counter[field] = make(map[string]float64)
			}
			counter[field][parts[2]] = n
		}
	}
	out := make(map[string]core.Value, len(lww)+len(counter))
	for field, comps := range counter {
		var sum float64
		for _, n := range comps {
			sum += n
		}
		out[field] = core.Value(fmtNum(sum))
	}
	for field, cands := range lww {
		if _, isCounter := counter[field]; isCounter {
			continue
		}
		if v, live := liveValue(cands); live {
			out[field] = core.Value(v)
		}
	}
	return out, nil
}

// deleteHashData removes all of a hash's fields: LWW fields via deleted slots,
// counter fields via ds.Delete of their components. Caller holds the key lock.
func (s *Store) deleteHashData(ctx context.Context, db int, key string) error {
	base := hashBase(db, key)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return err
	}
	deletedField := make(map[string]bool)
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, base), "/")
		switch {
		case len(parts) == 2: // LWW slot: tombstone the field once on our replica
			field, derr := decSeg(parts[0])
			if derr != nil || deletedField[field] {
				continue
			}
			deletedField[field] = true
			if err := s.writeSlot(ctx, hashSlot(db, key, field, s.replica()), tagDeleted, nil); err != nil {
				return err
			}
		case len(parts) == 3 && parts[1] == "c": // counter component
			if err := s.eng.Delete(ctx, ds.NewKey(e.Key)); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyHashData copies a hash's live data (LWW fields + counter components) to
// newKey, preserving flavor. Caller holds both key locks.
func (s *Store) copyHashData(ctx context.Context, db int, key, newKey string) error {
	base := hashBase(db, key)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return err
	}
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, base), "/")
		switch {
		case len(parts) == 2:
			field, derr := decSeg(parts[0])
			if derr != nil {
				continue
			}
			sl, derr := decodeSlot(e.Value)
			if derr != nil || sl.tag == tagDeleted {
				continue
			}
			if err := s.writeSlot(ctx, hashSlot(db, newKey, field, s.replica()), tagPresent, sl.value); err != nil {
				return err
			}
		case len(parts) == 3 && parts[1] == "c":
			field, derr := decSeg(parts[0])
			if derr != nil {
				continue
			}
			replica, derr := decSeg(parts[2])
			if derr != nil {
				continue
			}
			if err := s.eng.Put(ctx, hashFieldCounterSlot(db, newKey, field, replica), e.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

// hashFieldIsCounter reports whether a field is stored as a PN-counter.
func (s *Store) hashFieldIsCounter(ctx context.Context, db int, key, field string) (bool, error) {
	return s.anyPrefix(ctx, hashFieldCounterBase(db, key, field))
}

// hashCounterSum sums a field's PN-counter components.
func (s *Store) hashCounterSum(ctx context.Context, db int, key, field string) (float64, error) {
	base := hashFieldCounterBase(db, key, field)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return 0, err
	}
	var sum float64
	for _, e := range entries {
		if n, perr := strconv.ParseFloat(string(e.Value), 64); perr == nil {
			sum += n
		}
	}
	return sum, nil
}

// hashFieldIncr applies a delta to a hash field's PN-counter component on this
// replica, returning the new global total. Caller holds the key lock.
func (s *Store) hashFieldIncr(ctx context.Context, db int, key, field string, delta float64) (float64, error) {
	base := hashFieldCounterBase(db, key, field)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return 0, err
	}
	var mine, others float64
	for _, e := range entries {
		n, perr := strconv.ParseFloat(string(e.Value), 64)
		if perr != nil {
			continue
		}
		if lastSegment(e.Key) == encSeg(s.replica()) {
			mine = n
		} else {
			others += n
		}
	}
	mine += delta
	if err := s.eng.Put(ctx, hashFieldCounterSlot(db, key, field, s.replica()), []byte(fmtNum(mine))); err != nil {
		return 0, err
	}
	if err := s.writeMeta(ctx, db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeHash}}); err != nil {
		return 0, err
	}
	return others + mine, nil
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
	// A counter field reports the sum of its components (§6.4).
	if isCounter, err := r.s.hashFieldIsCounter(ctx, r.db, key, field); err != nil {
		return nil, err
	} else if isCounter {
		sum, err := r.s.hashCounterSum(ctx, r.db, key, field)
		if err != nil {
			return nil, err
		}
		return core.Value(fmtNum(sum)), nil
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

// setField writes one field's LWW value slot. Caller holds the key lock and has
// verified the type. Returns whether the field was newly created. Rejects
// writing an LWW value over a counter field (no mixing, §6.4).
func (r hashRepo) setField(ctx context.Context, key, field string, b []byte, created *bool) error {
	if isCounter, err := r.s.hashFieldIsCounter(ctx, r.db, key, field); err != nil {
		return err
	} else if isCounter {
		return core.ErrKeyType
	}
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

// rejectLWWField returns ErrValueType if the field exists as a plain LWW value,
// enforcing the no-mixing rule for HINCRBY/HINCRBYFLOAT (§6.4). Caller holds the
// key lock.
func (r hashRepo) rejectLWWField(ctx context.Context, key, field string) error {
	cands, err := r.fieldSlots(ctx, key, field)
	if err != nil {
		return err
	}
	if _, live := liveValue(cands); live {
		return core.ErrValueType
	}
	return nil
}

// Incr applies an integer delta to a hash-field PN-counter (§6.4), returning
// the new total. The field must be absent or already a counter.
func (r hashRepo) Incr(key, field string, delta int) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	if err := r.rejectLWWField(ctx, key, field); err != nil {
		return 0, err
	}
	total, err := r.s.hashFieldIncr(ctx, r.db, key, field, float64(delta))
	if err != nil {
		return 0, err
	}
	return int(total), nil
}

// IncrFloat applies a float delta to a hash-field PN-counter.
func (r hashRepo) IncrFloat(key, field string, delta float64) (float64, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureHashType(ctx, key); err != nil {
		return 0, err
	}
	if err := r.rejectLWWField(ctx, key, field); err != nil {
		return 0, err
	}
	return r.s.hashFieldIncr(ctx, r.db, key, field, delta)
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
