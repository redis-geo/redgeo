package crdtstore

import (
	"context"
	"encoding/binary"
	"math"
	"sort"
	"strings"

	"github.com/tidwall/match"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// zsetRepo implements redisapi.RZSet as a per-member score LWW-Map (DESIGN
// §6.6): each member's score is a per-replica HLC-LWW slot, so concurrent ZADDs
// of different members merge and same-member writes are last-writer-wins.
// Ranges are computed by prefix-scan + in-memory sort (v1).
type zsetRepo struct {
	s  *Store
	db int
}

func encodeScore(f float64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	return b
}

func decodeScore(b []byte) (float64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), true
}

func (r zsetRepo) ensureZSetType(ctx context.Context, key string) error {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return err
	}
	if ok && k.Type != core.TypeZSet {
		return core.ErrKeyType
	}
	return nil
}

// liveScores returns the winning score of every live member of a zset.
func (s *Store) zsetLiveScores(ctx context.Context, db int, key string) (map[string]float64, error) {
	base := zsetBase(db, key)
	entries, err := s.eng.QueryPrefix(ctx, base, false)
	if err != nil {
		return nil, err
	}
	byMember := make(map[string]map[string]slot)
	for _, e := range entries {
		rest := strings.TrimPrefix(e.Key, base) // {encMember}/{encReplica}
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			continue
		}
		member, derr := decSeg(rest[:i])
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
		if byMember[member] == nil {
			byMember[member] = make(map[string]slot)
		}
		byMember[member][replica] = sl
	}
	out := make(map[string]float64, len(byMember))
	for member, cands := range byMember {
		best, _, found := winner(cands)
		if !found || best.tag == tagDeleted {
			continue
		}
		if score, ok := decodeScore(best.value); ok {
			out[member] = score
		}
	}
	return out, nil
}

// sortedItems returns the zset's members sorted by (score, member) ascending.
func (r zsetRepo) sortedItems(ctx context.Context, key string) ([]restypes.ZSetItem, error) {
	scores, err := r.s.zsetLiveScores(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	items := make([]restypes.ZSetItem, 0, len(scores))
	for m, sc := range scores {
		items = append(items, restypes.ZSetItem{Elem: core.Value(m), Score: sc})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score < items[j].Score
		}
		return string(items[i].Elem) < string(items[j].Elem)
	})
	return items, nil
}

// writeScore writes one member's score slot; caller holds the key lock and has
// verified the type. Returns whether the member was newly created.
func (r zsetRepo) writeScore(ctx context.Context, key, member string, score float64) (bool, error) {
	cands, err := r.s.readSlots(ctx, zsetMemberBase(r.db, key, member))
	if err != nil {
		return false, err
	}
	_, live := liveValue(cands)
	if err := r.s.writeSlot(ctx, zsetSlot(r.db, key, member, r.s.replica()), tagPresent, encodeScore(score)); err != nil {
		return false, err
	}
	if err := r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeZSet}}); err != nil {
		return false, err
	}
	return !live, nil
}

func (r zsetRepo) Add(key string, elem any, score float64) (bool, error) {
	m, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return false, err
	}
	return r.writeScore(ctx, key, string(m), score)
}

func (r zsetRepo) AddMany(key string, items map[any]float64) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	created := 0
	for elem, score := range items {
		m, err := core.ToBytes(elem)
		if err != nil {
			return created, err
		}
		isNew, err := r.writeScore(ctx, key, string(m), score)
		if err != nil {
			return created, err
		}
		if isNew {
			created++
		}
	}
	return created, nil
}

func (r zsetRepo) GetScore(key string, elem any) (float64, error) {
	m, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	cands, err := r.s.readSlots(ctx, zsetMemberBase(r.db, key, string(m)))
	if err != nil {
		return 0, err
	}
	best, _, found := winner(cands)
	if !found || best.tag == tagDeleted {
		return 0, core.ErrNotFound
	}
	score, _ := decodeScore(best.value)
	return score, nil
}

func (r zsetRepo) rank(key string, elem any, rev bool) (int, float64, error) {
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, 0, err
	}
	m, err := core.ToBytes(elem)
	if err != nil {
		return 0, 0, err
	}
	items, err := r.sortedItems(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	if rev {
		reverse(items)
	}
	for i, it := range items {
		if string(it.Elem) == string(m) {
			return i, it.Score, nil
		}
	}
	return 0, 0, core.ErrNotFound
}

func (r zsetRepo) GetRank(key string, elem any) (int, float64, error) {
	return r.rank(key, elem, false)
}

func (r zsetRepo) GetRankRev(key string, elem any) (int, float64, error) {
	return r.rank(key, elem, true)
}

func (r zsetRepo) Len(key string) (int, error) {
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	scores, err := r.s.zsetLiveScores(ctx, r.db, key)
	return len(scores), err
}

func (r zsetRepo) Count(key string, min, max float64) (int, error) {
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	scores, err := r.s.zsetLiveScores(ctx, r.db, key)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, sc := range scores {
		if sc >= min && sc <= max {
			n++
		}
	}
	return n, nil
}

func (r zsetRepo) Delete(key string, elems ...any) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	n := 0
	for _, elem := range elems {
		m, err := core.ToBytes(elem)
		if err != nil {
			return n, err
		}
		cands, err := r.s.readSlots(ctx, zsetMemberBase(r.db, key, string(m)))
		if err != nil {
			return n, err
		}
		best, _, found := winner(cands)
		if !found || best.tag == tagDeleted {
			continue
		}
		if err := r.s.writeSlot(ctx, zsetSlot(r.db, key, string(m), r.s.replica()), tagDeleted, nil); err != nil {
			return n, err
		}
		n++
	}
	if scores, err := r.s.zsetLiveScores(ctx, r.db, key); err == nil && len(scores) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return n, nil
}

// Range implements the ZRANGE family via in-memory sort (§6.6).
func (r zsetRepo) Range(key string, opts restypes.RangeOpts) ([]restypes.ZSetItem, error) {
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return nil, err
	}
	items, err := r.sortedItems(ctx, key)
	if err != nil {
		return nil, err
	}
	return applyRange(items, opts), nil
}

// DeleteRange removes the members selected by a rank/score range
// (ZREMRANGEBYRANK / ZREMRANGEBYSCORE).
func (r zsetRepo) DeleteRange(key string, opts restypes.RangeOpts) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	items, err := r.sortedItems(ctx, key)
	if err != nil {
		return 0, err
	}
	victims := applyRange(items, opts)
	for _, it := range victims {
		if err := r.s.writeSlot(ctx, zsetSlot(r.db, key, string(it.Elem), r.s.replica()), tagDeleted, nil); err != nil {
			return 0, err
		}
	}
	if scores, err := r.s.zsetLiveScores(ctx, r.db, key); err == nil && len(scores) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return len(victims), nil
}

func (r zsetRepo) Incr(key string, elem any, delta float64) (float64, error) {
	m, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return 0, err
	}
	cur := 0.0
	cands, err := r.s.readSlots(ctx, zsetMemberBase(r.db, key, string(m)))
	if err != nil {
		return 0, err
	}
	if best, _, found := winner(cands); found && best.tag == tagPresent {
		cur, _ = decodeScore(best.value)
	}
	next := cur + delta
	if _, err := r.writeScore(ctx, key, string(m), next); err != nil {
		return 0, err
	}
	return next, nil
}

func (r zsetRepo) Scan(key string, cursor int, pattern string, count int) (restypes.ZScanResult, error) {
	ctx := bg()
	if err := r.ensureZSetType(ctx, key); err != nil {
		return restypes.ZScanResult{}, err
	}
	items, err := r.sortedItems(ctx, key)
	if err != nil {
		return restypes.ZScanResult{}, err
	}
	var out []restypes.ZSetItem
	for _, it := range items {
		if pattern != "" && pattern != "*" && !match.Match(string(it.Elem), pattern) {
			continue
		}
		out = append(out, it)
	}
	return restypes.ZScanResult{Cursor: 0, Items: out}, nil
}

// ---- set algebra over zsets (aggregate sum/min/max) ----

func (r zsetRepo) gather(ctx context.Context, keys []string) ([]map[string]float64, error) {
	out := make([]map[string]float64, 0, len(keys))
	for _, k := range keys {
		if err := r.ensureZSetType(ctx, k); err != nil {
			return nil, err
		}
		sc, err := r.s.zsetLiveScores(ctx, r.db, k)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, nil
}

func aggregate(agg string, a, b float64) float64 {
	switch agg {
	case "min":
		return math.Min(a, b)
	case "max":
		return math.Max(a, b)
	default: // sum
		return a + b
	}
}

func (r zsetRepo) Union(keys ...string) ([]restypes.ZSetItem, error) {
	return r.unionAgg("sum", keys)
}

func (r zsetRepo) unionAgg(agg string, keys []string) ([]restypes.ZSetItem, error) {
	ctx := bg()
	maps, err := r.gather(ctx, keys)
	if err != nil {
		return nil, err
	}
	acc := make(map[string]float64)
	seen := make(map[string]bool)
	for _, m := range maps {
		for member, sc := range m {
			if seen[member] {
				acc[member] = aggregate(agg, acc[member], sc)
			} else {
				acc[member] = sc
				seen[member] = true
			}
		}
	}
	return mapToSortedItems(acc), nil
}

func (r zsetRepo) Inter(keys ...string) ([]restypes.ZSetItem, error) {
	return r.interAgg("sum", keys)
}

func (r zsetRepo) interAgg(agg string, keys []string) ([]restypes.ZSetItem, error) {
	ctx := bg()
	maps, err := r.gather(ctx, keys)
	if err != nil || len(maps) == 0 {
		return nil, err
	}
	acc := make(map[string]float64)
	for member, sc := range maps[0] {
		acc[member] = sc
	}
	for _, m := range maps[1:] {
		for member := range acc {
			sc, ok := m[member]
			if !ok {
				delete(acc, member)
				continue
			}
			acc[member] = aggregate(agg, acc[member], sc)
		}
	}
	return mapToSortedItems(acc), nil
}

func (r zsetRepo) storeItems(dest string, items []restypes.ZSetItem) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, dest))
	defer unlock()
	// Blind overwrite: tombstone existing dest members, then write results.
	if existing, err := r.s.zsetLiveScores(ctx, r.db, dest); err == nil {
		for member := range existing {
			_ = r.s.writeSlot(ctx, zsetSlot(r.db, dest, member, r.s.replica()), tagDeleted, nil)
		}
	}
	for _, it := range items {
		if _, err := r.writeScore(ctx, dest, string(it.Elem), it.Score); err != nil {
			return 0, err
		}
	}
	if len(items) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, dest)
	}
	return len(items), nil
}

func (r zsetRepo) UnionStore(dest, agg string, keys ...string) (int, error) {
	items, err := r.unionAgg(normAgg(agg), keys)
	if err != nil {
		return 0, err
	}
	return r.storeItems(dest, items)
}

func (r zsetRepo) InterStore(dest, agg string, keys ...string) (int, error) {
	items, err := r.interAgg(normAgg(agg), keys)
	if err != nil {
		return 0, err
	}
	return r.storeItems(dest, items)
}

// ---- helpers ----

func normAgg(a string) string {
	switch strings.ToLower(a) {
	case "min":
		return "min"
	case "max":
		return "max"
	default:
		return "sum"
	}
}

func reverse(items []restypes.ZSetItem) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func mapToSortedItems(m map[string]float64) []restypes.ZSetItem {
	items := make([]restypes.ZSetItem, 0, len(m))
	for member, sc := range m {
		items = append(items, restypes.ZSetItem{Elem: core.Value(member), Score: sc})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score < items[j].Score
		}
		return string(items[i].Elem) < string(items[j].Elem)
	})
	return items
}

// applyRange selects items by rank or score, applying desc, offset, and count.
// Input items must be sorted ascending by (score, member).
func applyRange(items []restypes.ZSetItem, opts restypes.RangeOpts) []restypes.ZSetItem {
	var sel []restypes.ZSetItem
	switch {
	case opts.ByScore:
		for _, it := range items {
			if it.Score >= opts.Min && it.Score <= opts.Max {
				sel = append(sel, it)
			}
		}
		if opts.Desc {
			reverse(sel)
		}
	case opts.ByRank:
		n := len(items)
		start, stop := normIndex(opts.Start, n), normIndex(opts.Stop, n)
		if start < 0 {
			start = 0
		}
		if stop >= n {
			stop = n - 1
		}
		if start <= stop && start < n {
			sel = append(sel, items[start:stop+1]...)
		}
		if opts.Desc {
			reverse(sel)
		}
	default:
		sel = append(sel, items...)
		if opts.Desc {
			reverse(sel)
		}
	}
	// Offset/Count (used by ZRANGEBYSCORE LIMIT).
	if opts.Offset > 0 {
		if opts.Offset >= len(sel) {
			return nil
		}
		sel = sel[opts.Offset:]
	}
	if opts.Count > 0 && opts.Count < len(sel) {
		sel = sel[:opts.Count]
	}
	return sel
}

// normIndex converts a possibly-negative Redis index to an absolute one.
func normIndex(i, n int) int {
	if i < 0 {
		i += n
	}
	return i
}
