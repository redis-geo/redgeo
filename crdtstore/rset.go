package crdtstore

import (
	"context"
	"math/rand"
	"sort"
	"strings"

	"github.com/tidwall/match"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// setRepo implements redisapi.RSet as a native Add-Wins OR-Set (DESIGN §6.3):
// each member is a presence-only key, SADD = Put, SREM = Delete. This maps
// directly onto go-ds-crdt's underlying OR-Set, so concurrent SADD x / SREM x
// resolves to x present (add wins) — correct conflict-free behavior.
type setRepo struct {
	s  *Store
	db int
}

var presence = []byte{1} // non-empty presence marker

func (r setRepo) ensureSetType(ctx context.Context, key string) error {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return err
	}
	if ok && k.Type != core.TypeSet {
		return core.ErrKeyType
	}
	return nil
}

// members returns the live member strings of a set.
func (r setRepo) members(ctx context.Context, key string) ([]string, error) {
	base := setBase(r.db, key)
	entries, err := r.s.query(ctx, base, true)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		enc := strings.TrimPrefix(e.Key, base)
		m, derr := decSeg(enc)
		if derr != nil {
			continue
		}
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

func (r setRepo) Add(key string, elems ...any) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureSetType(ctx, key); err != nil {
		return 0, err
	}
	added := 0
	for _, elem := range elems {
		m, err := core.ToBytes(elem)
		if err != nil {
			return added, err
		}
		mk := setMember(r.db, key, string(m))
		has, err := r.s.has(ctx, mk)
		if err != nil {
			return added, err
		}
		if err := r.s.put(ctx, mk, presence); err != nil {
			return added, err
		}
		if !has {
			added++
		}
	}
	if err := r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeSet}}); err != nil {
		return added, err
	}
	return added, nil
}

func (r setRepo) Delete(key string, elems ...any) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureSetType(ctx, key); err != nil {
		return 0, err
	}
	removed := 0
	for _, elem := range elems {
		m, err := core.ToBytes(elem)
		if err != nil {
			return removed, err
		}
		mk := setMember(r.db, key, string(m))
		has, err := r.s.has(ctx, mk)
		if err != nil {
			return removed, err
		}
		if !has {
			continue
		}
		if err := r.s.del(ctx, mk); err != nil { // OR-Set remove
			return removed, err
		}
		removed++
	}
	if mem, err := r.members(ctx, key); err == nil && len(mem) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return removed, nil
}

func (r setRepo) Exists(key string, elem any) (bool, error) {
	ctx := bg()
	if err := r.ensureSetType(ctx, key); err != nil {
		return false, err
	}
	m, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}
	return r.s.has(ctx, setMember(r.db, key, string(m)))
}

func (r setRepo) Items(key string) ([]core.Value, error) {
	ctx := bg()
	if err := r.ensureSetType(ctx, key); err != nil {
		return nil, err
	}
	mem, err := r.members(ctx, key)
	if err != nil {
		return nil, err
	}
	return toValues(mem), nil
}

func (r setRepo) Len(key string) (int, error) {
	ctx := bg()
	if err := r.ensureSetType(ctx, key); err != nil {
		return 0, err
	}
	mem, err := r.members(ctx, key)
	return len(mem), err
}

func (r setRepo) Random(key string) (core.Value, error) {
	ctx := bg()
	if err := r.ensureSetType(ctx, key); err != nil {
		return nil, err
	}
	mem, err := r.members(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(mem) == 0 {
		return nil, core.ErrNotFound
	}
	return core.Value(mem[rand.Intn(len(mem))]), nil
}

func (r setRepo) Pop(key string) (core.Value, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if err := r.ensureSetType(ctx, key); err != nil {
		return nil, err
	}
	mem, err := r.members(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(mem) == 0 {
		return nil, core.ErrNotFound
	}
	pick := mem[rand.Intn(len(mem))]
	if err := r.s.del(ctx, setMember(r.db, key, pick)); err != nil {
		return nil, err
	}
	if len(mem) == 1 {
		_ = r.s.deleteMeta(ctx, r.db, key)
	}
	return core.Value(pick), nil
}

func (r setRepo) Move(src, dest string, elem any) error {
	ctx := bg()
	m, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	// Lock both keys in a stable order.
	a, b := lockKey(r.db, src), lockKey(r.db, dest)
	if a > b {
		a, b = b, a
	}
	u1 := r.s.locks.Lock(a)
	defer u1()
	u2 := r.s.locks.Lock(b)
	defer u2()

	if err := r.ensureSetType(ctx, src); err != nil {
		return err
	}
	if err := r.ensureSetType(ctx, dest); err != nil {
		return err
	}
	srcMk := setMember(r.db, src, string(m))
	has, err := r.s.has(ctx, srcMk)
	if err != nil {
		return err
	}
	if !has {
		return core.ErrNotFound
	}
	if err := r.s.del(ctx, srcMk); err != nil {
		return err
	}
	if err := r.s.put(ctx, setMember(r.db, dest, string(m)), presence); err != nil {
		return err
	}
	if err := r.s.writeMeta(ctx, r.db, dest, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeSet}}); err != nil {
		return err
	}
	if mem, err := r.members(ctx, src); err == nil && len(mem) == 0 {
		_ = r.s.deleteMeta(ctx, r.db, src)
	}
	return nil
}

// ---- set algebra (computed in app; *STORE is a non-atomic blind write) ----

func (r setRepo) memberSet(ctx context.Context, key string) (map[string]struct{}, error) {
	if err := r.ensureSetType(ctx, key); err != nil {
		return nil, err
	}
	mem, err := r.members(ctx, key)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(mem))
	for _, m := range mem {
		set[m] = struct{}{}
	}
	return set, nil
}

func (r setRepo) Diff(keys ...string) ([]core.Value, error) {
	ctx := bg()
	if len(keys) == 0 {
		return nil, nil
	}
	base, err := r.memberSet(ctx, keys[0])
	if err != nil {
		return nil, err
	}
	for _, k := range keys[1:] {
		other, err := r.memberSet(ctx, k)
		if err != nil {
			return nil, err
		}
		for m := range other {
			delete(base, m)
		}
	}
	return sortedValues(base), nil
}

func (r setRepo) Inter(keys ...string) ([]core.Value, error) {
	ctx := bg()
	if len(keys) == 0 {
		return nil, nil
	}
	base, err := r.memberSet(ctx, keys[0])
	if err != nil {
		return nil, err
	}
	for _, k := range keys[1:] {
		other, err := r.memberSet(ctx, k)
		if err != nil {
			return nil, err
		}
		for m := range base {
			if _, ok := other[m]; !ok {
				delete(base, m)
			}
		}
	}
	return sortedValues(base), nil
}

func (r setRepo) Union(keys ...string) ([]core.Value, error) {
	ctx := bg()
	union := make(map[string]struct{})
	for _, k := range keys {
		other, err := r.memberSet(ctx, k)
		if err != nil {
			return nil, err
		}
		for m := range other {
			union[m] = struct{}{}
		}
	}
	return sortedValues(union), nil
}

func (r setRepo) storeResult(dest string, vals []core.Value) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, dest))
	defer unlock()
	// Blind overwrite: delete existing dest set, then write the result.
	if k, ok, err := r.s.probe(ctx, r.db, dest); err == nil && ok && k.Type == core.TypeSet {
		mem, _ := r.members(ctx, dest)
		for _, m := range mem {
			_ = r.s.del(ctx, setMember(r.db, dest, m))
		}
	}
	for _, v := range vals {
		if err := r.s.put(ctx, setMember(r.db, dest, v.String()), presence); err != nil {
			return 0, err
		}
	}
	if len(vals) > 0 {
		if err := r.s.writeMeta(ctx, r.db, dest, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeSet}}); err != nil {
			return 0, err
		}
	} else {
		_ = r.s.deleteMeta(ctx, r.db, dest)
	}
	return len(vals), nil
}

func (r setRepo) DiffStore(dest string, keys ...string) (int, error) {
	v, err := r.Diff(keys...)
	if err != nil {
		return 0, err
	}
	return r.storeResult(dest, v)
}

func (r setRepo) InterStore(dest string, keys ...string) (int, error) {
	v, err := r.Inter(keys...)
	if err != nil {
		return 0, err
	}
	return r.storeResult(dest, v)
}

func (r setRepo) UnionStore(dest string, keys ...string) (int, error) {
	v, err := r.Union(keys...)
	if err != nil {
		return 0, err
	}
	return r.storeResult(dest, v)
}

func (r setRepo) Scan(key string, cursor int, pattern string, count int) (restypes.SetScanResult, error) {
	ctx := bg()
	if err := r.ensureSetType(ctx, key); err != nil {
		return restypes.SetScanResult{}, err
	}
	mem, err := r.members(ctx, key)
	if err != nil {
		return restypes.SetScanResult{}, err
	}
	var items []core.Value
	for _, m := range mem {
		if pattern != "" && pattern != "*" && !match.Match(m, pattern) {
			continue
		}
		items = append(items, core.Value(m))
	}
	return restypes.SetScanResult{Cursor: 0, Items: items}, nil
}

// helpers

func toValues(ss []string) []core.Value {
	out := make([]core.Value, len(ss))
	for i, s := range ss {
		out[i] = core.Value(s)
	}
	return out
}

func sortedValues(set map[string]struct{}) []core.Value {
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return toValues(out)
}
