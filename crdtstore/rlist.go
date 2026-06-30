package crdtstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/redis-geo/redgeo/core"
)

// listRepo implements redisapi.RList as a fractional-index sequence (DESIGN
// §6.5). Each element key is /l/{posKey}, where posKey is an order-preserving
// encoding of a fractional position plus a {replica}-{seq} tiebreaker so
// concurrent pushes at the same logical position both survive in deterministic
// order. RPUSH uses max+1, LPUSH uses min-1, LINSERT uses a midpoint — these
// commute across replicas. Index-based ops (LSET i, LTRIM) race before
// convergence (documented; lists are the weakest type).
type listRepo struct {
	s  *Store
	db int
}

// listEntry is one decoded list element.
type listEntry struct {
	posKey string  // full trailing path segment, sorts in position order
	pos    float64 // fractional position
	value  []byte
}

// encodePos encodes a float64 position order-preservingly into 16 hex chars:
// flip the sign bit for non-negatives, invert all bits for negatives, so the
// big-endian byte order matches numeric order, then hex (itself order-
// preserving). This makes a prefix scan of /l/ yield elements in position order.
func encodePos(f float64) string {
	bits := math.Float64bits(f)
	if f >= 0 {
		bits |= 1 << 63
	} else {
		bits = ^bits
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return fmt.Sprintf("%016x", b)
}

func decodePos(hex16 string) (float64, bool) {
	if len(hex16) < 16 {
		return 0, false
	}
	u, err := strconv.ParseUint(hex16[:16], 16, 64)
	if err != nil {
		return 0, false
	}
	if u&(1<<63) != 0 {
		u &^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u), true
}

// makePosKey builds the trailing segment for a position: ordered hex + a
// per-node-unique tiebreaker.
func (r listRepo) makePosKey(pos float64) string {
	seq := atomic.AddUint64(&r.s.listSeq, 1)
	return fmt.Sprintf("%s-%s-%016x", encodePos(pos), encSeg(r.s.replica()), seq)
}

func (r listRepo) ensureListType(ctx context.Context, key string) error {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return err
	}
	if ok && k.Type != core.TypeList {
		return core.ErrKeyType
	}
	return nil
}

// entries returns the list's elements in position order.
func (r listRepo) entries(ctx context.Context, key string) ([]listEntry, error) {
	base := listBase(r.db, key)
	raw, err := r.s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return nil, err
	}
	out := make([]listEntry, 0, len(raw))
	for _, e := range raw {
		posKey := strings.TrimPrefix(e.Key, base)
		pos, ok := decodePos(posKey)
		if !ok {
			continue
		}
		out = append(out, listEntry{posKey: posKey, pos: pos, value: e.Value})
	}
	// Query results are not guaranteed sorted; posKey is order-preserving, so
	// sorting by it yields position order.
	sort.Slice(out, func(i, j int) bool { return out[i].posKey < out[j].posKey })
	return out, nil
}

func (r listRepo) push(key string, elem any, front bool) (int, error) {
	b, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return 0, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return 0, err
	}
	var pos float64
	switch {
	case len(es) == 0:
		pos = 0
	case front:
		pos = es[0].pos - 1
	default:
		pos = es[len(es)-1].pos + 1
	}
	if err := r.s.eng.Put(ctx, listElem(r.db, key, r.makePosKey(pos)), b); err != nil {
		return 0, err
	}
	if err := r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeList}}); err != nil {
		return 0, err
	}
	return len(es) + 1, nil
}

func (r listRepo) PushBack(key string, elem any) (int, error)  { return r.push(key, elem, false) }
func (r listRepo) PushFront(key string, elem any) (int, error) { return r.push(key, elem, true) }

func (r listRepo) pop(key string, front bool) (core.Value, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return nil, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(es) == 0 {
		return nil, core.ErrNotFound
	}
	var victim listEntry
	if front {
		victim = es[0]
	} else {
		victim = es[len(es)-1]
	}
	if err := r.s.eng.Delete(ctx, listElem(r.db, key, victim.posKey)); err != nil {
		return nil, err
	}
	if len(es) == 1 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return core.Value(victim.value), nil
}

func (r listRepo) PopFront(key string) (core.Value, error) { return r.pop(key, true) }
func (r listRepo) PopBack(key string) (core.Value, error)  { return r.pop(key, false) }

func (r listRepo) Len(key string) (int, error) {
	ctx := bg()
	if err := r.ensureListType(ctx, key); err != nil {
		return 0, err
	}
	es, err := r.entries(ctx, key)
	return len(es), err
}

func (r listRepo) Get(key string, idx int) (core.Value, error) {
	ctx := bg()
	if err := r.ensureListType(ctx, key); err != nil {
		return nil, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return nil, err
	}
	i := absIndex(idx, len(es))
	if i < 0 || i >= len(es) {
		return nil, core.ErrNotFound
	}
	return core.Value(es[i].value), nil
}

func (r listRepo) Set(key string, idx int, elem any) error {
	b, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return err
	}
	i := absIndex(idx, len(es))
	if i < 0 || i >= len(es) {
		return core.ErrNotFound
	}
	// Overwrite the value at the same position (register; LSET races, §6.5).
	return r.s.eng.Put(ctx, listElem(r.db, key, es[i].posKey), b)
}

func (r listRepo) Range(key string, start, stop int) ([]core.Value, error) {
	ctx := bg()
	if err := r.ensureListType(ctx, key); err != nil {
		return nil, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return nil, err
	}
	lo, hi := rangeBounds(start, stop, len(es))
	if lo > hi {
		return nil, nil
	}
	out := make([]core.Value, 0, hi-lo+1)
	for _, e := range es[lo : hi+1] {
		out = append(out, core.Value(e.value))
	}
	return out, nil
}

func (r listRepo) insert(key string, pivot, elem any, before bool) (int, error) {
	pb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	eb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return 0, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return 0, err
	}
	pivotIdx := -1
	for i, e := range es {
		if string(e.value) == string(pb) {
			pivotIdx = i
			break
		}
	}
	if pivotIdx < 0 {
		return -1, nil // Redis: pivot not found -> -1
	}
	var pos float64
	if before {
		if pivotIdx == 0 {
			pos = es[0].pos - 1
		} else {
			pos = (es[pivotIdx-1].pos + es[pivotIdx].pos) / 2
		}
	} else {
		if pivotIdx == len(es)-1 {
			pos = es[pivotIdx].pos + 1
		} else {
			pos = (es[pivotIdx].pos + es[pivotIdx+1].pos) / 2
		}
	}
	if err := r.s.eng.Put(ctx, listElem(r.db, key, r.makePosKey(pos)), eb); err != nil {
		return 0, err
	}
	return len(es) + 1, nil
}

func (r listRepo) InsertBefore(key string, pivot, elem any) (int, error) {
	return r.insert(key, pivot, elem, true)
}
func (r listRepo) InsertAfter(key string, pivot, elem any) (int, error) {
	return r.insert(key, pivot, elem, false)
}

// deleteMatching removes up to count elements equal to elem. count==0 removes
// all; count>0 from the front; count<0 from the back (LREM semantics).
func (r listRepo) deleteMatching(key string, elem any, count int) (int, error) {
	tb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return 0, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return 0, err
	}
	order := make([]int, 0, len(es))
	if count < 0 {
		for i := len(es) - 1; i >= 0; i-- {
			order = append(order, i)
		}
	} else {
		for i := range es {
			order = append(order, i)
		}
	}
	limit := count
	if limit < 0 {
		limit = -limit
	}
	removed := 0
	for _, i := range order {
		if string(es[i].value) != string(tb) {
			continue
		}
		if err := r.s.eng.Delete(ctx, listElem(r.db, key, es[i].posKey)); err != nil {
			return removed, err
		}
		removed++
		if limit > 0 && removed >= limit {
			break
		}
	}
	if removed == len(es) {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return removed, nil
}

func (r listRepo) Delete(key string, elem any) (int, error) {
	return r.deleteMatching(key, elem, 0)
}
func (r listRepo) DeleteFront(key string, elem any, count int) (int, error) {
	return r.deleteMatching(key, elem, count)
}
func (r listRepo) DeleteBack(key string, elem any, count int) (int, error) {
	return r.deleteMatching(key, elem, -count)
}

func (r listRepo) Trim(key string, start, stop int) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureListType(ctx, key); err != nil {
		return 0, err
	}
	es, err := r.entries(ctx, key)
	if err != nil {
		return 0, err
	}
	lo, hi := rangeBounds(start, stop, len(es))
	removed := 0
	for i, e := range es {
		if i >= lo && i <= hi {
			continue
		}
		if err := r.s.eng.Delete(ctx, listElem(r.db, key, e.posKey)); err != nil {
			return removed, err
		}
		removed++
	}
	if removed == len(es) {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return removed, nil
}

func (r listRepo) PopBackPushFront(src, dest string) (core.Value, error) {
	v, err := r.PopBack(src)
	if err != nil {
		return nil, err
	}
	if _, err := r.PushFront(dest, v.Bytes()); err != nil {
		return nil, err
	}
	return v, nil
}

// ---- index helpers ----

func absIndex(idx, n int) int {
	if idx < 0 {
		return n + idx
	}
	return idx
}

// rangeBounds normalizes a Redis [start,stop] range (supporting negatives) to
// absolute inclusive [lo,hi] clamped to [0,n-1]. Returns lo>hi for empty.
func rangeBounds(start, stop, n int) (int, int) {
	if n == 0 {
		return 0, -1
	}
	lo := absIndex(start, n)
	hi := absIndex(stop, n)
	if lo < 0 {
		lo = 0
	}
	if hi >= n {
		hi = n - 1
	}
	return lo, hi
}
