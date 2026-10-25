package crdtstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	ds "github.com/ipfs/go-datastore"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

// Top-k (the RedisBloom TOPK.* model). RedisBloom's HeavyKeeper relies on
// probabilistic decay, which is neither commutative nor mergeable, so redgeo
// keeps a *Space-Saving* summary of each replica's own stream instead:
//
//	k/{replica}   this replica's summary: up to m items with (count, error)
//
// A replica only writes its own summary; a read sums the counts of every
// replica's summary and ranks the result, which is deterministic and does not
// depend on the order updates arrive (a state-based merge of per-replica
// states, like the counters in §6.4). Space-Saving keeps the m = max(4k, 32)
// heaviest items per replica; counts are over-estimates by at most the item's
// recorded error, and any item whose true frequency exceeds N/m is guaranteed
// to be tracked. The width/depth/decay arguments of TOPK.RESERVE are stored and
// reported but do not size anything.

const (
	topkMaxK     = 1 << 20
	topkMinSlots = 32
)

type topkParams struct {
	k, width, depth int
	decay           float64
}

func (p topkParams) slots() int { return max(4*p.k, topkMinSlots) }

func (p topkParams) encode() []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint32(b, uint32(p.k))
	binary.BigEndian.PutUint32(b[4:], uint32(p.width))
	binary.BigEndian.PutUint32(b[8:], uint32(p.depth))
	binary.BigEndian.PutUint64(b[12:], math.Float64bits(p.decay))
	return b
}

func decodeTopKParams(b []byte) (topkParams, bool) {
	if len(b) != 20 {
		return topkParams{}, false
	}
	return topkParams{
		k: int(binary.BigEndian.Uint32(b)), width: int(binary.BigEndian.Uint32(b[4:])),
		depth: int(binary.BigEndian.Uint32(b[8:])), decay: math.Float64frombits(binary.BigEndian.Uint64(b[12:])),
	}, true
}

func (p topkParams) validate() error {
	switch {
	case p.k < 1 || p.k > topkMaxK:
		return fmt.Errorf("TOPK: invalid k")
	case p.width < 1 || p.width > 1<<24:
		return fmt.Errorf("TOPK: invalid width")
	case p.depth < 1 || p.depth > 1<<10:
		return fmt.Errorf("TOPK: invalid depth")
	case !(p.decay > 0 && p.decay <= 1):
		return fmt.Errorf("TOPK: invalid decay value. must be between 0 and 1")
	}
	return nil
}

type ssEntry struct{ count, err int64 }

// summary is one replica's Space-Saving state.
type summary map[string]*ssEntry

func encodeSummary(s summary) []byte {
	names := make([]string, 0, len(s))
	for n := range s {
		names = append(names, n)
	}
	sort.Strings(names) // stable bytes for identical state
	out := binary.AppendUvarint(nil, uint64(len(names)))
	for _, n := range names {
		e := s[n]
		out = binary.AppendUvarint(out, uint64(len(n)))
		out = append(out, n...)
		out = binary.AppendUvarint(out, uint64(e.count))
		out = binary.AppendUvarint(out, uint64(e.err))
	}
	return out
}

func decodeSummary(b []byte) (summary, bool) {
	n, w := binary.Uvarint(b)
	if w <= 0 || n > uint64(len(b)) {
		return nil, false
	}
	b = b[w:]
	s := make(summary, n)
	next := func() (uint64, bool) {
		v, w := binary.Uvarint(b)
		if w <= 0 {
			return 0, false
		}
		b = b[w:]
		return v, true
	}
	for i := uint64(0); i < n; i++ {
		l, ok := next()
		if !ok || uint64(len(b)) < l {
			return nil, false
		}
		name := string(b[:l])
		b = b[l:]
		c, ok1 := next()
		e, ok2 := next()
		if !ok1 || !ok2 {
			return nil, false
		}
		s[name] = &ssEntry{int64(c), int64(e)}
	}
	return s, true
}

// add applies one occurrence-weighted update (Space-Saving): a tracked item
// grows; a new item takes a free slot, or evicts the current minimum and
// inherits its count (recorded as error).
func (s summary) add(item string, inc int64, slots int) {
	if e, ok := s[item]; ok {
		e.count += inc
		return
	}
	if len(s) < slots {
		s[item] = &ssEntry{inc, 0}
		return
	}
	var minName string
	var min *ssEntry
	for n, e := range s {
		if min == nil || e.count < min.count || (e.count == min.count && n < minName) {
			minName, min = n, e
		}
	}
	delete(s, minName)
	s[item] = &ssEntry{min.count + inc, min.count}
}

type topkView struct {
	s      *Store
	sp     sketchParams
	p      topkParams
	me     string
	others map[string]int64 // items' counts summed over the other replicas
	own    summary
}

func (r sketchRepo) topkOpen(ctx context.Context, key string) (*topkView, int64, bool, error) {
	sp, ok, etime, err := r.s.sketchOpen(ctx, r.db, key, skTopK)
	if err != nil || !ok {
		return nil, etime, false, err
	}
	p, pok := decodeTopKParams(sp.raw)
	if !pok {
		return nil, etime, false, fmt.Errorf("TOPK: corrupt sketch parameters")
	}
	v := &topkView{s: r.s, sp: sp, p: p, me: r.s.replica(), others: map[string]int64{}, own: summary{}}
	entries, err := r.s.query(ctx, sp.data+"k/", false)
	if err != nil {
		return nil, etime, false, err
	}
	for _, e := range entries {
		sum, ok := decodeSummary(e.Value)
		if !ok {
			continue
		}
		if rep, rok := replicaOf(e.Key); rok && rep == v.me {
			v.own = sum
			continue
		}
		for n, en := range sum {
			v.others[n] += en.count
		}
	}
	return v, etime, true, nil
}

func (v *topkView) count(item string) int64 {
	c := v.others[item]
	if e, ok := v.own[item]; ok {
		c += e.count
	}
	return c
}

// ranked returns the merged items, heaviest first (ties by name).
func (v *topkView) ranked() []restypes.TopKEntry {
	seen := make(map[string]bool, len(v.others)+len(v.own))
	var out []restypes.TopKEntry
	add := func(n string) {
		if !seen[n] {
			seen[n] = true
			out = append(out, restypes.TopKEntry{Item: []byte(n), Count: v.count(n)})
		}
	}
	for n := range v.others {
		add(n)
	}
	for n := range v.own {
		add(n)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return string(out[i].Item) < string(out[j].Item)
	})
	return out
}

func (v *topkView) top() []restypes.TopKEntry {
	r := v.ranked()
	if len(r) > v.p.k {
		r = r[:v.p.k]
	}
	return r
}

func nameSet(es []restypes.TopKEntry) map[string]bool {
	m := make(map[string]bool, len(es))
	for _, e := range es {
		m[string(e.Item)] = true
	}
	return m
}

func (v *topkView) save(ctx context.Context) error {
	return v.s.put(ctx, ds.NewKey(v.sp.data+"k/"+encSeg(v.me)), encodeSummary(v.own))
}

// ---- repo ----

func (r sketchRepo) TopKReserve(key string, k, width, depth int, decay float64) error {
	p := topkParams{k, width, depth, decay}
	if err := p.validate(); err != nil {
		return err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	_, exists, _, err := r.s.sketchOpen(ctx, r.db, key, skTopK)
	if err != nil {
		return err
	}
	if exists {
		return redis.ErrSketchExists
	}
	_, err = r.s.writeSketchParams(ctx, r.db, key, skTopK, p.encode(), 0)
	return err
}

// TopKAdd adds incs[i] occurrences of items[i]. For each it returns the item
// that fell out of the top-k as a result (nil if none).
func (r sketchRepo) TopKAdd(key string, items [][]byte, incs []int64) ([][]byte, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	v, _, ok, err := r.topkOpen(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	expelled := make([][]byte, len(items))
	before := nameSet(v.top())
	for i, it := range items {
		if incs[i] <= 0 {
			return nil, fmt.Errorf("TOPK: Increment must be a positive integer")
		}
		v.own.add(string(it), incs[i], v.p.slots())
		after := v.top()
		afterSet := nameSet(after)
		var gone []string
		for n := range before {
			if !afterSet[n] {
				gone = append(gone, n)
			}
		}
		if len(gone) > 0 {
			sort.Strings(gone)
			expelled[i] = []byte(gone[0])
		}
		before = afterSet
	}
	return expelled, v.save(ctx)
}

func (r sketchRepo) TopKQuery(key string, items [][]byte) ([]bool, error) {
	v, _, ok, err := r.topkOpen(bg(), key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	in := nameSet(v.top())
	out := make([]bool, len(items))
	for i, it := range items {
		out[i] = in[string(it)]
	}
	return out, nil
}

func (r sketchRepo) TopKCount(key string, items [][]byte) ([]int64, error) {
	v, _, ok, err := r.topkOpen(bg(), key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	out := make([]int64, len(items))
	for i, it := range items {
		out[i] = v.count(string(it))
	}
	return out, nil
}

func (r sketchRepo) TopKList(key string) ([]restypes.TopKEntry, error) {
	v, _, ok, err := r.topkOpen(bg(), key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	return v.top(), nil
}

func (r sketchRepo) TopKInfo(key string) (restypes.TopKInfo, error) {
	v, _, ok, err := r.topkOpen(bg(), key)
	if err != nil {
		return restypes.TopKInfo{}, err
	}
	if !ok {
		return restypes.TopKInfo{}, core.ErrNotFound
	}
	return restypes.TopKInfo{K: v.p.k, Width: v.p.width, Depth: v.p.depth, Decay: v.p.decay}, nil
}

// copyTopK stores the merged summary (heaviest m items) as this replica's.
func (s *Store) copyTopK(ctx context.Context, from, to sketchParams) error {
	p, ok := decodeTopKParams(from.raw)
	if !ok {
		return fmt.Errorf("TOPK: corrupt sketch parameters")
	}
	src := &topkView{s: s, sp: from, p: p, me: s.replica(), others: map[string]int64{}, own: summary{}}
	entries, err := s.query(ctx, from.data+"k/", false)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if sum, ok := decodeSummary(e.Value); ok {
			for n, en := range sum {
				src.others[n] += en.count
			}
		}
	}
	dst := &topkView{s: s, sp: to, p: p, me: s.replica(), own: summary{}}
	ranked := src.ranked()
	if len(ranked) > p.slots() {
		ranked = ranked[:p.slots()]
	}
	for _, e := range ranked {
		dst.own[string(e.Item)] = &ssEntry{count: e.Count}
	}
	return dst.save(ctx)
}
