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
	redis "github.com/redis-geo/redgeo/redisapi"
)

// Count-min sketch (the RedisBloom CMS.* model). A sketch is a depth×width
// matrix of counters; an item maps to one cell per row and its estimate is the
// smallest of those cells. Each cell is a PN-counter component per replica:
//
//	c/{cell:%010x}/{replica}   this replica's contribution to one cell (int64)
//	t/{replica}                this replica's contribution to the total count
//
// A cell's value is the sum over replicas, so concurrent increments on
// different replicas add up exactly as if applied to one sketch. Only non-zero
// cells exist (the matrix is sparse). Components are signed so CMS.MERGE can
// *replace* a destination by writing own = target − (everyone else's).

const (
	cmsMaxDim   = 1 << 31
	cmsMaxCells = 1 << 36
)

type cmsParams struct{ width, depth int }

func (p cmsParams) encode() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b, uint32(p.width))
	binary.BigEndian.PutUint32(b[4:], uint32(p.depth))
	return b
}

func decodeCMSParams(b []byte) (cmsParams, bool) {
	if len(b) != 8 {
		return cmsParams{}, false
	}
	return cmsParams{int(binary.BigEndian.Uint32(b)), int(binary.BigEndian.Uint32(b[4:]))}, true
}

func (p cmsParams) validate() error {
	if p.width < 1 || p.depth < 1 || p.width > cmsMaxDim || p.depth > 1<<16 || int64(p.width)*int64(p.depth) > cmsMaxCells {
		return fmt.Errorf("CMS: invalid width or depth")
	}
	return nil
}

// cells returns the matrix cell hit in each row for item.
func (p cmsParams) cells(item []byte) []uint64 {
	h1 := murmur64a(item, 0x5bd1e995)
	h2 := murmur64a(item, 0xdeadbeef) | 1
	out := make([]uint64, p.depth)
	for r := range out {
		out[r] = uint64(r)*uint64(p.width) + (h1+uint64(r)*h2)%uint64(p.width)
	}
	return out
}

type cmsView struct {
	s  *Store
	sp sketchParams
	p  cmsParams
}

func (r sketchRepo) cmsOpen(ctx context.Context, key string) (*cmsView, int64, bool, error) {
	sp, ok, etime, err := r.s.sketchOpen(ctx, r.db, key, skCMS)
	if err != nil || !ok {
		return nil, etime, false, err
	}
	p, pok := decodeCMSParams(sp.raw)
	if !pok {
		return nil, etime, false, fmt.Errorf("CMS: corrupt sketch parameters")
	}
	return &cmsView{r.s, sp, p}, etime, true, nil
}

func cmsCellKey(v *cmsView, cell uint64, replica string) ds.Key {
	return ds.NewKey(fmt.Sprintf("%sc/%010x/%s", v.sp.data, cell, encSeg(replica)))
}

func i64(b []byte) (int64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(b)), true
}

func putI64(n int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(n)) }

// cell returns a cell's merged value and this replica's component.
func (v *cmsView) cell(ctx context.Context, cell uint64) (sum, own int64, err error) {
	entries, err := v.s.query(ctx, fmt.Sprintf("%sc/%010x/", v.sp.data, cell), false)
	if err != nil {
		return 0, 0, err
	}
	me := v.s.replica()
	for _, e := range entries {
		n, ok := i64(e.Value)
		if !ok {
			continue
		}
		sum += n
		if rep, ok := replicaOf(e.Key); ok && rep == me {
			own = n
		}
	}
	return sum, own, nil
}

// total returns the merged item count and this replica's component.
func (v *cmsView) total(ctx context.Context) (sum, own int64, err error) {
	entries, err := v.s.query(ctx, v.sp.data+"t/", false)
	if err != nil {
		return 0, 0, err
	}
	me := v.s.replica()
	for _, e := range entries {
		n, ok := i64(e.Value)
		if !ok {
			continue
		}
		sum += n
		if rep, ok := replicaOf(e.Key); ok && rep == me {
			own = n
		}
	}
	return sum, own, nil
}

func (v *cmsView) setTotalOwn(ctx context.Context, n int64) error {
	return v.s.put(ctx, ds.NewKey(v.sp.data+"t/"+encSeg(v.s.replica())), putI64(n))
}

// allCells returns every non-zero cell's merged value and this replica's
// component (the whole sparse matrix).
func (v *cmsView) allCells(ctx context.Context) (sums, own map[uint64]int64, err error) {
	entries, err := v.s.query(ctx, v.sp.data+"c/", false)
	if err != nil {
		return nil, nil, err
	}
	sums, own = map[uint64]int64{}, map[uint64]int64{}
	me := v.s.replica()
	prefix := v.sp.data + "c/"
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, prefix), "/")
		if len(parts) != 2 {
			continue
		}
		cell, perr := strconv.ParseUint(parts[0], 16, 64)
		n, ok := i64(e.Value)
		if perr != nil || !ok {
			continue
		}
		sums[cell] += n
		if rep, rok := replicaOf(e.Key); rok && rep == me {
			own[cell] = n
		}
	}
	return sums, own, nil
}

// ---- repo ----

func (r sketchRepo) CMSInit(key string, width, depth int) error {
	p := cmsParams{width, depth}
	if err := p.validate(); err != nil {
		return err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	_, exists, _, err := r.s.sketchOpen(ctx, r.db, key, skCMS)
	if err != nil {
		return err
	}
	if exists {
		return redis.ErrSketchExists
	}
	_, err = r.s.writeSketchParams(ctx, r.db, key, skCMS, p.encode(), 0)
	return err
}

func (r sketchRepo) CMSIncr(key string, items [][]byte, incs []int64) ([]int64, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	v, _, ok, err := r.cmsOpen(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	out := make([]int64, len(items))
	_, totalOwn, err := v.total(ctx)
	if err != nil {
		return nil, err
	}
	for i, it := range items {
		if incs[i] <= 0 {
			return nil, fmt.Errorf("CMS: Increment must be a positive integer")
		}
		est := int64(math.MaxInt64)
		for _, cell := range v.p.cells(it) {
			sum, own, err := v.cell(ctx, cell)
			if err != nil {
				return nil, err
			}
			if sum > math.MaxInt64-incs[i] {
				return nil, fmt.Errorf("CMS: INCRBY overflow")
			}
			if err := v.s.put(ctx, cmsCellKey(v, cell, v.s.replica()), putI64(own+incs[i])); err != nil {
				return nil, err
			}
			est = min(est, sum+incs[i])
		}
		out[i] = est
		totalOwn += incs[i]
	}
	return out, v.setTotalOwn(ctx, totalOwn)
}

func (r sketchRepo) CMSQuery(key string, items [][]byte) ([]int64, error) {
	ctx := bg()
	v, _, ok, err := r.cmsOpen(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	out := make([]int64, len(items))
	for i, it := range items {
		est := int64(math.MaxInt64)
		for _, cell := range v.p.cells(it) {
			sum, _, err := v.cell(ctx, cell)
			if err != nil {
				return nil, err
			}
			est = min(est, sum)
		}
		out[i] = max(est, 0)
	}
	return out, nil
}

func (r sketchRepo) CMSInfo(key string) (width, depth int, count int64, err error) {
	ctx := bg()
	v, _, ok, err := r.cmsOpen(ctx, key)
	if err != nil {
		return 0, 0, 0, err
	}
	if !ok {
		return 0, 0, 0, core.ErrNotFound
	}
	count, _, err = v.total(ctx)
	return v.p.width, v.p.depth, count, err
}

// CMSMerge makes dest the weighted sum of the sources (replacing its contents).
func (r sketchRepo) CMSMerge(dest string, srcs []string, weights []int64) error {
	ctx := bg()
	type snap struct {
		cells map[uint64]int64
		total int64
	}
	var views []*cmsView
	var snaps []snap
	for _, src := range srcs {
		v, _, ok, err := r.cmsOpen(ctx, src)
		if err != nil {
			return err
		}
		if !ok {
			return core.ErrNotFound
		}
		sums, _, err := v.allCells(ctx)
		if err != nil {
			return err
		}
		total, _, err := v.total(ctx)
		if err != nil {
			return err
		}
		views, snaps = append(views, v), append(snaps, snap{sums, total})
	}

	unlock := r.s.locks.Lock(lockKey(r.db, dest))
	defer unlock()
	d, _, ok, err := r.cmsOpen(ctx, dest)
	if err != nil {
		return err
	}
	if !ok {
		return core.ErrNotFound
	}
	target, targetTotal := map[uint64]int64{}, int64(0)
	for i, v := range views {
		if v.p != d.p {
			return redis.ErrCMSDims
		}
		for cell, n := range snaps[i].cells {
			target[cell] += n * weights[i]
		}
		targetTotal += snaps[i].total * weights[i]
	}
	sums, own, err := d.allCells(ctx)
	if err != nil {
		return err
	}
	touch := map[uint64]bool{}
	for c := range target {
		touch[c] = true
	}
	for c := range sums {
		touch[c] = true
	}
	for cell := range touch {
		others := sums[cell] - own[cell]
		want := target[cell] - others
		if want == own[cell] {
			continue
		}
		key := cmsCellKey(d, cell, d.s.replica())
		if want == 0 {
			err = d.s.del(ctx, key)
		} else {
			err = d.s.put(ctx, key, putI64(want))
		}
		if err != nil {
			return err
		}
	}
	tsum, town, err := d.total(ctx)
	if err != nil {
		return err
	}
	return d.setTotalOwn(ctx, targetTotal-(tsum-town))
}

// copyCMS writes the merged matrix under the new key as this replica's
// components.
func (s *Store) copyCMS(ctx context.Context, from, to sketchParams) error {
	src := &cmsView{s: s, sp: from}
	p, ok := decodeCMSParams(from.raw)
	if !ok {
		return fmt.Errorf("CMS: corrupt sketch parameters")
	}
	src.p = p
	dst := &cmsView{s: s, sp: to, p: p}
	sums, _, err := src.allCells(ctx)
	if err != nil {
		return err
	}
	for cell, n := range sums {
		if n == 0 {
			continue
		}
		if err := s.put(ctx, cmsCellKey(dst, cell, s.replica()), putI64(n)); err != nil {
			return err
		}
	}
	total, _, err := src.total(ctx)
	if err != nil {
		return err
	}
	return dst.setTotalOwn(ctx, total)
}
