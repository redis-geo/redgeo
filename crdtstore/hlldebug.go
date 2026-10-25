package crdtstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"

	"github.com/redis-geo/redgeo/core"
)

// PFDEBUG and PFSELFTEST. The encoding a sketch reports is the one GET would
// return: "sparse" while it qualifies (all registers <= 32 and the sparse form
// fits hll-sparse-max-bytes) and the key has not been pinned, otherwise "dense".
// Unlike Redis, where encoding is stored state, "sparse" here is a function of
// the register contents, plus the pin that TODENSE sets.

var (
	errNoHLLKey  = errors.New("ERR The specified key does not exist")
	errNotSparse = errors.New("ERR HLL encoding is not sparse")
)

// hllInspect resolves a key to its merged sketch and flavor under the key lock.
func (r strRepo) hllInspect(ctx context.Context, key string) (*hllSketch, strFlavor, int64, error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return nil, 0, 0, err
	}
	if !ok {
		return nil, 0, 0, errNoHLLKey
	}
	m, _, _, err := r.s.readMeta(ctx, r.db, key)
	if err != nil {
		return nil, 0, 0, err
	}
	if k.Type != core.TypeString || !m.Flavor.isHLL() {
		return nil, 0, 0, core.ErrKeyType
	}
	merged, _, _, err := r.s.hllLoad(ctx, r.db, key)
	return merged, m.Flavor, m.ETimeMS, err
}

func (h *hllSketch) isSparse(flavor strFlavor) bool {
	if flavor == flavorHLLDense {
		return false
	}
	_, ok := h.encodeSparse()
	return ok
}

// PFEncoding reports "sparse" or "dense".
func (r strRepo) PFEncoding(key string) (string, error) {
	h, fl, _, err := r.hllInspect(bg(), key)
	if err != nil {
		return "", err
	}
	if h.isSparse(fl) {
		return "sparse", nil
	}
	return "dense", nil
}

// PFToDense pins the key to the dense encoding; it reports whether that changed
// anything (false if it was already dense).
func (r strRepo) PFToDense(key string) (bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	h, fl, etime, err := r.hllInspect(ctx, key)
	if err != nil {
		return false, err
	}
	if !h.isSparse(fl) {
		return false, nil
	}
	return true, r.s.writeMeta(ctx, r.db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etime},
		Flavor:  flavorHLLDense,
	})
}

// PFRegisters returns all 16384 register values. As in Redis, reading them
// converts a sparse sketch to dense.
func (r strRepo) PFRegisters(key string) ([]uint8, error) {
	if _, err := r.PFToDense(key); err != nil {
		return nil, err
	}
	h, _, _, err := r.hllInspect(bg(), key)
	if err != nil {
		return nil, err
	}
	return append([]uint8(nil), h[:]...), nil
}

// PFDecode renders the sparse opcodes (z:N Z:N v:value,run); an error if the
// sketch is not sparse.
func (r strRepo) PFDecode(key string) (string, error) {
	h, fl, _, err := r.hllInspect(bg(), key)
	if err != nil {
		return "", err
	}
	enc, ok := h.encodeSparse()
	if !ok || fl == flavorHLLDense {
		return "", errNotSparse
	}
	return decodeSparseOps(enc[hyllHeader:]), nil
}

// PFSelfTest checks the HyperLogLog implementation end to end on synthetic data,
// returning nil if everything holds. It touches no stored keys.
func (r strRepo) PFSelfTest() error { return hllSelfTest() }

func hllSelfTest() error {
	fail := func(f string, a ...any) error {
		return fmt.Errorf("ERR HyperLogLog self test failed: "+f, a...)
	}
	rng := rand.New(rand.NewSource(1)) // fixed seed: a failure reproduces
	elem := func(i int, salt uint64) []byte { return []byte(fmt.Sprintf("selftest-%d-%d", salt, i)) }

	// 1. Cardinality stays within the algorithm's error bound at several scales.
	var sizes = []int{1, 10, 100, 1000, 10000, 100000}
	var full hllSketch
	added := 0
	for _, n := range sizes {
		for ; added < n; added++ {
			full.add(elem(added, 7))
		}
		got := float64(full.count())
		tol := math.Max(2, 0.05*float64(n)) // ~6 standard errors (0.81%) at scale
		if n <= 100 {
			tol = math.Max(2, 0.04*float64(n))
		}
		if math.Abs(got-float64(n)) > tol {
			return fail("cardinality of %d distinct elements estimated as %d", n, int64(got))
		}
		// 2. Both encodings round-trip the registers exactly.
		if d, ok := decodeRedis(full.encodeRedis()); !ok || *d != full {
			return fail("dense encoding round trip at n=%d", n)
		}
		if sp, ok := full.encodeSparse(); ok {
			if d, ok := decodeRedis(sp); !ok || *d != full {
				return fail("sparse encoding round trip at n=%d", n)
			}
		}
		// 3. The internal per-replica component round-trips as well.
		if d, ok := decodeHLL(full.encode()); !ok || *d != full {
			return fail("component encoding round trip at n=%d", n)
		}
	}

	// 4. Merge is commutative, associative and idempotent, and counts the union.
	var a, b, c hllSketch
	for i := 0; i < 5000; i++ {
		a.add(elem(i, 1))
		b.add(elem(i+2500, 1)) // overlaps a on 2500 elements
		c.add(elem(rng.Intn(1000), 2))
	}
	ab, ba := a, b
	ab.merge(&b)
	ba.merge(&a)
	if ab != ba {
		return fail("merge is not commutative")
	}
	x, y := ab, a
	x.merge(&c)
	y.merge(&b)
	y.merge(&c)
	if x != y {
		return fail("merge is not associative")
	}
	again := ab
	if again.merge(&ab) || again != ab {
		return fail("merge is not idempotent")
	}
	if got := float64(ab.count()); math.Abs(got-7500) > 0.05*7500 {
		return fail("union of overlapping sets estimated as %d, want ~7500", int64(got))
	}

	// 5. Adding a known element never changes a sketch (and a new one may).
	before := ab
	for i := 0; i < 200; i++ {
		if ab.add(elem(i, 1)) {
			return fail("re-adding element %d changed the sketch", i)
		}
	}
	if ab != before {
		return fail("registers changed on re-add")
	}

	// 6. The hash spreads elements over the registers and is stable.
	hit := map[uint64]bool{}
	for i := 0; i < 50000; i++ {
		if hllHash(elem(i, 3)) != hllHash(elem(i, 3)) {
			return fail("hash is not deterministic")
		}
		hit[hllHash(elem(i, 3))&(hllM-1)] = true
	}
	if len(hit) < hllM*9/10 {
		return fail("hash reached only %d of %d registers", len(hit), hllM)
	}
	return nil
}
