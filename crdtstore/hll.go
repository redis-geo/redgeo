package crdtstore

import (
	"context"
	"encoding/binary"
	"math"
	"math/bits"
	"strings"

	"github.com/redis-geo/redgeo/core"
)

// HyperLogLog as a CRDT. A sketch is an array of 2^14 registers merged by
// register-wise max, which is commutative/associative/idempotent. Like the
// PN-counter (§6.4), each replica writes ONLY its own sketch at
// /d/{db}/{key}/p/{replicaID}; a read merges every replica's sketch, so
// concurrent PFADDs on different replicas converge with no coordination.
// The key is TypeString/flavorHLL. Like Redis, GET returns the sketch as a
// "HYLL" string and SET of a valid one stores an HLL (hllfmt.go).

const (
	hllP    = 14
	hllM    = 1 << hllP // registers
	hllQ    = 64 - hllP // bits left for the rank (rank in 1..hllQ+1)
	hllFmtS = 0         // sparse: (uint16 idx, uint8 val)*
	hllFmtD = 1         // dense: hllM bytes
)

type hllSketch [hllM]uint8

// hllHash is Redis' MurmurHash64A with seed 0xadc83b19, so an element lands in
// the same register here as in Redis and sketches interoperate (hllfmt.go).
func hllHash(b []byte) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	h := uint64(0xadc83b19) ^ (uint64(len(b)) * m)
	for ; len(b) >= 8; b = b[8:] {
		k := binary.LittleEndian.Uint64(b)
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}
	if n := len(b); n > 0 {
		for i := n - 1; i >= 0; i-- {
			h ^= uint64(b[i]) << (8 * uint(i))
		}
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// add folds one element in and reports whether a register increased.
func (h *hllSketch) add(elem []byte) bool {
	x := hllHash(elem)
	idx := x & (hllM - 1)
	rest := (x >> hllP) | (1 << hllQ) // sentinel bounds the rank at hllQ+1
	rank := uint8(bits.TrailingZeros64(rest)) + 1
	if rank > h[idx] {
		h[idx] = rank
		return true
	}
	return false
}

// merge takes the register-wise max of o into h and reports any change.
func (h *hllSketch) merge(o *hllSketch) bool {
	changed := false
	for i, v := range o {
		if v > h[i] {
			h[i] = v
			changed = true
		}
	}
	return changed
}

// count is Ertl's improved estimator ("New cardinality estimation algorithms
// for HyperLogLog sketches", 2017), as used by Redis.
func (h *hllSketch) count() int64 {
	var c [hllQ + 2]float64
	for _, v := range h {
		c[v]++
	}
	m := float64(hllM)
	z := m * hllTau(1-c[hllQ+1]/m)
	for k := hllQ; k >= 1; k-- {
		z = 0.5 * (z + c[k])
	}
	z += m * hllSigma(c[0]/m)
	if math.IsInf(z, 1) || z == 0 {
		return 0
	}
	return int64(math.Round(m * m / (2 * math.Ln2) / z))
}

func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		old := z
		z += x * y
		y += y
		if z == old {
			return z
		}
	}
}

func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		old := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if z == old {
			return z / 3
		}
	}
}

func (h *hllSketch) encode() []byte {
	nz := 0
	for _, v := range h {
		if v != 0 {
			nz++
		}
	}
	if nz*3 < hllM {
		out := make([]byte, 1, 1+nz*3)
		out[0] = hllFmtS
		for i, v := range h {
			if v != 0 {
				out = append(out, byte(i>>8), byte(i), v)
			}
		}
		return out
	}
	out := make([]byte, 1+hllM)
	out[0] = hllFmtD
	copy(out[1:], h[:])
	return out
}

func decodeHLL(b []byte) (*hllSketch, bool) {
	h := new(hllSketch)
	if len(b) == 0 {
		return nil, false
	}
	switch b[0] {
	case hllFmtS:
		b = b[1:]
		if len(b)%3 != 0 {
			return nil, false
		}
		for ; len(b) > 0; b = b[3:] {
			idx := int(b[0])<<8 | int(b[1])
			if idx >= hllM || b[2] > hllQ+1 {
				return nil, false
			}
			h[idx] = b[2]
		}
	case hllFmtD:
		if len(b) != 1+hllM {
			return nil, false
		}
		copy(h[:], b[1:])
	default:
		return nil, false
	}
	return h, true
}

// hllLoad merges every replica's sketch for a key into one view. own is this
// replica's component (zero sketch if none); exists reports any component.
func (s *Store) hllLoad(ctx context.Context, db int, key string) (merged, own *hllSketch, exists bool, err error) {
	base := hllBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return nil, nil, false, err
	}
	merged, own = new(hllSketch), new(hllSketch)
	mine := s.replica()
	for _, e := range entries {
		sk, ok := decodeHLL(e.Value)
		if !ok {
			continue
		}
		exists = true
		merged.merge(sk)
		if r, derr := decSeg(strings.TrimPrefix(e.Key, base)); derr == nil && r == mine {
			own.merge(sk)
		}
	}
	return merged, own, exists, nil
}

// hllCheck verifies key is absent or an HLL; returns its current ETime.
func (s *Store) hllCheck(ctx context.Context, db int, key string) (etimeMS int64, err error) {
	k, ok, err := s.probe(ctx, db, key)
	if err != nil || !ok {
		return 0, err
	}
	m, _, _, err := s.readMeta(ctx, db, key)
	if err != nil {
		return 0, err
	}
	if k.Type != core.TypeString || m.Flavor != flavorHLL {
		return 0, core.ErrKeyType
	}
	return m.ETimeMS, nil
}

// hllStore writes sk as this replica's component and the key's meta.
func (s *Store) hllStore(ctx context.Context, db int, key string, sk *hllSketch, etimeMS int64) error {
	if err := s.put(ctx, hllSlot(db, key, s.replica()), sk.encode()); err != nil {
		return err
	}
	return s.writeMeta(ctx, db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etimeMS},
		Flavor:  flavorHLL,
	})
}

// PFAdd folds elems into the key's sketch, creating it if needed. It reports
// whether the estimate may have changed or the key was created (Redis PFADD).
func (r strRepo) PFAdd(key string, elems ...[]byte) (bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	etime, err := r.s.hllCheck(ctx, r.db, key)
	if err != nil {
		return false, err
	}
	merged, own, exists, err := r.s.hllLoad(ctx, r.db, key)
	if err != nil {
		return false, err
	}
	changed := !exists
	for _, e := range elems {
		if merged.add(e) {
			own.add(e)
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	return true, r.s.hllStore(ctx, r.db, key, own, etime)
}

// PFCount returns the cardinality of the union of the given keys' sketches.
// Missing keys count as empty.
func (r strRepo) PFCount(keys ...string) (int64, error) {
	ctx := bg()
	union := new(hllSketch)
	for _, key := range keys {
		if _, err := r.s.hllCheck(ctx, r.db, key); err != nil {
			return 0, err
		}
		merged, _, _, err := r.s.hllLoad(ctx, r.db, key)
		if err != nil {
			return 0, err
		}
		union.merge(merged)
	}
	return union.count(), nil
}

// PFMerge stores the union of dest and srcs into dest's sketch.
func (r strRepo) PFMerge(dest string, srcs ...string) error {
	ctx := bg()
	for _, src := range srcs {
		if _, err := r.s.hllCheck(ctx, r.db, src); err != nil {
			return err
		}
	}
	unlock := r.s.locks.Lock(lockKey(r.db, dest))
	defer unlock()
	etime, err := r.s.hllCheck(ctx, r.db, dest)
	if err != nil {
		return err
	}
	merged, own, _, err := r.s.hllLoad(ctx, r.db, dest)
	if err != nil {
		return err
	}
	own.merge(merged)
	for _, src := range srcs {
		sm, _, _, err := r.s.hllLoad(ctx, r.db, src)
		if err != nil {
			return err
		}
		own.merge(sm)
	}
	return r.s.hllStore(ctx, r.db, dest, own, etime)
}

// deleteHLL removes every replica component plus meta (OR-Set remove, as for
// counters).
func (s *Store) deleteHLL(ctx context.Context, db int, key string) error {
	base := hllBase(db, key)
	entries, err := s.query(ctx, base, true)
	if err != nil {
		return err
	}
	for _, e := range entries {
		r, derr := decSeg(strings.TrimPrefix(e.Key, base))
		if derr != nil {
			continue
		}
		if err := s.del(ctx, hllSlot(db, key, r)); err != nil {
			return err
		}
	}
	return s.deleteMeta(ctx, db, key)
}

// copyHLL collapses the merged sketch into this replica's component of newKey.
func (s *Store) copyHLL(ctx context.Context, db int, key, newKey string, etimeMS int64) error {
	merged, _, _, err := s.hllLoad(ctx, db, key)
	if err != nil {
		return err
	}
	return s.hllStore(ctx, db, newKey, merged, etimeMS)
}
