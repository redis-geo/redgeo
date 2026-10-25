package crdtstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	ds "github.com/ipfs/go-datastore"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// Scalable Bloom filter (the RedisBloom BF.* model). A filter is a series of
// sub-filters ("layers"); layer i has capacity C·E^i and false-positive rate
// P·0.5^i, so its size and hash count follow from the parameters alone and every
// replica agrees on the geometry. An insert goes to the first layer that is not
// full. Because bits only ever turn on, replicas merge by OR:
//
//	b/{layer:%02x}/{chunk:%08x}/{replica}   this replica's bits of one 4 KiB chunk
//	n/{layer:%02x}/{replica}                this replica's insert count for the layer
//
// Reads OR the replicas' chunks and sum their counts. A count only includes
// inserts that set a new bit locally, so an item added concurrently on two
// replicas is counted twice: the layer reads as fuller than it is and the filter
// grows a little early (never the unsafe direction).

const (
	bloomChunkBytes = 4096
	bloomChunkBits  = bloomChunkBytes * 8
	bloomMaxLayers  = 32
	bloomMaxBits    = 1 << 32

	bloomDefaultError    = 0.01
	bloomDefaultCapacity = 100
	bloomDefaultExpand   = 2
)

type bloomParams struct {
	errRate   float64
	capacity  int64
	expansion int // 0 = non-scaling
}

func (p bloomParams) encode() []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint64(b, math.Float64bits(p.errRate))
	binary.BigEndian.PutUint64(b[8:], uint64(p.capacity))
	binary.BigEndian.PutUint32(b[16:], uint32(p.expansion))
	return b
}

func decodeBloomParams(b []byte) (bloomParams, bool) {
	if len(b) != 20 {
		return bloomParams{}, false
	}
	return bloomParams{
		errRate:   math.Float64frombits(binary.BigEndian.Uint64(b)),
		capacity:  int64(binary.BigEndian.Uint64(b[8:])),
		expansion: int(binary.BigEndian.Uint32(b[16:])),
	}, true
}

func (p bloomParams) validate() error {
	switch {
	case !(p.errRate > 0 && p.errRate < 1):
		return fmt.Errorf("ERR (0 < error rate range < 1)")
	case p.capacity < 1:
		return fmt.Errorf("ERR (capacity should be larger than 0)")
	case p.expansion < 0 || p.expansion > 32768:
		return fmt.Errorf("ERR bad expansion")
	}
	if _, bits, _, err := p.layer(0); err != nil || bits == 0 {
		return fmt.Errorf("ERR bad capacity")
	}
	return nil
}

// layer returns layer i's capacity, bit count and hash-function count.
func (p bloomParams) layer(i int) (capacity int64, bits uint64, hashes int, err error) {
	if i >= bloomMaxLayers || (p.expansion == 0 && i > 0) {
		return 0, 0, 0, fmt.Errorf("ERR non scaling filter is full")
	}
	c := float64(p.capacity) * math.Pow(float64(p.expansion), float64(i))
	if p.expansion == 0 {
		c = float64(p.capacity)
	}
	e := p.errRate * math.Pow(0.5, float64(i))
	m := math.Ceil(c * -math.Log(e) / (math.Ln2 * math.Ln2))
	if c > 1<<53 || m > bloomMaxBits || m < 1 {
		return 0, 0, 0, fmt.Errorf("ERR bad capacity")
	}
	return int64(c), uint64(m), int(math.Ceil(-math.Log(e) / math.Ln2)), nil
}

// positions are the bit indexes an item occupies in a layer (double hashing).
func bloomPositions(item []byte, bits uint64, hashes int) []uint64 {
	h1 := murmur64a(item, 0x9747b28c)
	h2 := murmur64a(item, 0xc2b2ae35) | 1
	out := make([]uint64, hashes)
	for j := range out {
		out[j] = (h1 + uint64(j)*h2) % bits
	}
	return out
}

type chunkID struct{ layer, idx int }

// bloomState is one command's cached view of a filter.
type bloomState struct {
	s      *Store
	ctx    context.Context
	sp     sketchParams
	p      bloomParams
	me     string
	merged map[chunkID][]byte
	own    map[chunkID][]byte
	dirty  map[chunkID]bool
	counts map[int]int64 // merged insert counts
	mine   map[int]int64 // this replica's insert counts
	ndirty map[int]bool
	layers int // layers that may hold data
}

func (s *Store) newBloomState(ctx context.Context, sp sketchParams) (*bloomState, error) {
	p, ok := decodeBloomParams(sp.raw)
	if !ok {
		return nil, fmt.Errorf("ERR corrupt filter parameters")
	}
	st := &bloomState{
		s: s, ctx: ctx, sp: sp, p: p, me: s.replica(),
		merged: map[chunkID][]byte{}, own: map[chunkID][]byte{}, dirty: map[chunkID]bool{},
		counts: map[int]int64{}, mine: map[int]int64{}, ndirty: map[int]bool{}, layers: 1,
	}
	// The highest layer with a count says how many layers exist.
	entries, err := s.query(ctx, sp.data+"n/", true)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, sp.data+"n/"), "/")
		if l, err := strconv.ParseUint(parts[0], 16, 8); err == nil && int(l)+1 > st.layers {
			st.layers = int(l) + 1
		}
	}
	return st, nil
}

func (st *bloomState) chunk(c chunkID) ([]byte, error) {
	if b, ok := st.merged[c]; ok {
		return b, nil
	}
	prefix := fmt.Sprintf("%sb/%02x/%08x/", st.sp.data, c.layer, c.idx)
	entries, err := st.s.query(st.ctx, prefix, false)
	if err != nil {
		return nil, err
	}
	merged, own := make([]byte, bloomChunkBytes), make([]byte, bloomChunkBytes)
	for _, e := range entries {
		if len(e.Value) != bloomChunkBytes {
			continue
		}
		for i, v := range e.Value {
			merged[i] |= v
		}
		if r, ok := replicaOf(e.Key); ok && r == st.me {
			copy(own, e.Value)
		}
	}
	st.merged[c], st.own[c] = merged, own
	return merged, nil
}

func (st *bloomState) count(layer int) (int64, error) {
	if n, ok := st.counts[layer]; ok {
		return n, nil
	}
	prefix := fmt.Sprintf("%sn/%02x/", st.sp.data, layer)
	entries, err := st.s.query(st.ctx, prefix, false)
	if err != nil {
		return 0, err
	}
	var sum int64
	for _, e := range entries {
		if len(e.Value) != 8 {
			continue
		}
		n := int64(binary.BigEndian.Uint64(e.Value))
		sum += n
		if r, ok := replicaOf(e.Key); ok && r == st.me {
			st.mine[layer] = n
		}
	}
	st.counts[layer] = sum
	return sum, nil
}

// has reports whether item is (probably) in any layer.
func (st *bloomState) has(item []byte) (bool, error) {
	for l := 0; l < st.layers; l++ {
		_, bits, hashes, err := st.p.layer(l)
		if err != nil {
			break
		}
		all := true
		for _, pos := range bloomPositions(item, bits, hashes) {
			b, err := st.chunk(chunkID{l, int(pos / bloomChunkBits)})
			if err != nil {
				return false, err
			}
			if b[(pos%bloomChunkBits)/8]&(1<<(pos%8)) == 0 {
				all = false
				break
			}
		}
		if all {
			return true, nil
		}
	}
	return false, nil
}

// add inserts item into the first non-full layer.
func (st *bloomState) add(item []byte) error {
	layer := 0
	for {
		capacity, bits, hashes, err := st.p.layer(layer)
		if err != nil {
			return err
		}
		n, err := st.count(layer)
		if err != nil {
			return err
		}
		if n >= capacity {
			layer++
			continue
		}
		for _, pos := range bloomPositions(item, bits, hashes) {
			c := chunkID{layer, int(pos / bloomChunkBits)}
			m, err := st.chunk(c)
			if err != nil {
				return err
			}
			bit := byte(1) << (pos % 8)
			m[(pos%bloomChunkBits)/8] |= bit
			st.own[c][(pos%bloomChunkBits)/8] |= bit
			st.dirty[c] = true
		}
		st.counts[layer]++
		st.mine[layer]++
		st.ndirty[layer] = true
		if layer+1 > st.layers {
			st.layers = layer + 1
		}
		return nil
	}
}

func (st *bloomState) flush() error {
	for c := range st.dirty {
		key := ds.NewKey(fmt.Sprintf("%sb/%02x/%08x/%s", st.sp.data, c.layer, c.idx, encSeg(st.me)))
		if err := st.s.put(st.ctx, key, st.own[c]); err != nil {
			return err
		}
	}
	for l := range st.ndirty {
		key := ds.NewKey(fmt.Sprintf("%sn/%02x/%s", st.sp.data, l, encSeg(st.me)))
		if err := st.s.put(st.ctx, key, binary.BigEndian.AppendUint64(nil, uint64(st.mine[l]))); err != nil {
			return err
		}
	}
	st.dirty, st.ndirty = map[chunkID]bool{}, map[int]bool{}
	return nil
}

// ---- repo ----

type sketchRepo struct {
	s  *Store
	db int
}

func (r sketchRepo) bloomOpen(ctx context.Context, key string) (*bloomState, int64, bool, error) {
	sp, ok, etime, err := r.s.sketchOpen(ctx, r.db, key, skBloom)
	if err != nil || !ok {
		return nil, etime, false, err
	}
	st, err := r.s.newBloomState(ctx, sp)
	return st, etime, true, err
}

func bloomParamsFrom(o restypes.BloomOpts) (bloomParams, error) {
	p := bloomParams{errRate: o.ErrorRate, capacity: o.Capacity, expansion: o.Expansion}
	return p, p.validate()
}

func (r sketchRepo) BFReserve(key string, o restypes.BloomOpts) error {
	p, err := bloomParamsFrom(o)
	if err != nil {
		return err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	_, exists, _, err := r.s.sketchOpen(ctx, r.db, key, skBloom)
	if err != nil {
		return err
	}
	if exists {
		return ErrSketchExists
	}
	_, err = r.s.writeSketchParams(ctx, r.db, key, skBloom, p.encode(), 0)
	return err
}

func (r sketchRepo) BFAdd(key string, items [][]byte, create *restypes.BloomOpts) ([]bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	st, etime, exists, err := r.bloomOpen(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		if create == nil {
			return nil, core.ErrNotFound
		}
		p, err := bloomParamsFrom(*create)
		if err != nil {
			return nil, err
		}
		sp, err := r.s.writeSketchParams(ctx, r.db, key, skBloom, p.encode(), etime)
		if err != nil {
			return nil, err
		}
		if st, err = r.s.newBloomState(ctx, sp); err != nil {
			return nil, err
		}
	}
	added := make([]bool, len(items))
	for i, it := range items {
		has, err := st.has(it)
		if err != nil {
			return nil, err
		}
		if has {
			continue
		}
		if err := st.add(it); err != nil {
			_ = st.flush() // keep what was inserted before the failure
			return nil, err
		}
		added[i] = true
	}
	return added, st.flush()
}

func (r sketchRepo) BFExists(key string, items [][]byte) ([]bool, error) {
	ctx := bg()
	st, _, exists, err := r.bloomOpen(ctx, key)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(items))
	if !exists {
		return out, nil
	}
	for i, it := range items {
		if out[i], err = st.has(it); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r sketchRepo) BFInfo(key string) (restypes.BloomInfo, error) {
	ctx := bg()
	st, _, exists, err := r.bloomOpen(ctx, key)
	if err != nil {
		return restypes.BloomInfo{}, err
	}
	if !exists {
		return restypes.BloomInfo{}, core.ErrNotFound
	}
	info := restypes.BloomInfo{Filters: st.layers, Expansion: st.p.expansion}
	for l := 0; l < st.layers; l++ {
		capacity, bits, _, err := st.p.layer(l)
		if err != nil {
			break
		}
		n, err := st.count(l)
		if err != nil {
			return info, err
		}
		info.Capacity += capacity
		info.SizeBytes += int64((bits + 7) / 8)
		info.Items += n
	}
	return info, nil
}

// copyBloom writes the OR of all replicas' components under the new key as this
// replica's components.
func (s *Store) copyBloom(ctx context.Context, from, to sketchParams) error {
	src, err := s.newBloomState(ctx, from)
	if err != nil {
		return err
	}
	dst, err := s.newBloomState(ctx, to)
	if err != nil {
		return err
	}
	entries, err := s.query(ctx, from.data+"b/", true)
	if err != nil {
		return err
	}
	seen := map[chunkID]bool{}
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, from.data+"b/"), "/")
		if len(parts) != 3 {
			continue
		}
		l, e1 := strconv.ParseUint(parts[0], 16, 8)
		i, e2 := strconv.ParseUint(parts[1], 16, 32)
		if e1 != nil || e2 != nil {
			continue
		}
		c := chunkID{int(l), int(i)}
		if seen[c] {
			continue
		}
		seen[c] = true
		m, err := src.chunk(c)
		if err != nil {
			return err
		}
		dst.own[c], dst.dirty[c] = append([]byte(nil), m...), true
	}
	for l := 0; l < src.layers; l++ {
		n, err := src.count(l)
		if err != nil {
			return err
		}
		dst.mine[l], dst.ndirty[l] = n, true
	}
	return dst.flush()
}
