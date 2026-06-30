package crdtstore

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/match"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// keyRepo implements redisapi.RKey, bound to one logical DB.
type keyRepo struct {
	s  *Store
	db int
}

// listKeys returns every live logical key name in the DB. It scans the 256
// per-partition meta prefixes (DESIGN §6.9), extracts the encoded key segment,
// dedupes across replica slots, and probes each for liveness. Naive but
// correct; the resumable numeric cursor (§6.9) is a Phase 9 refinement.
func (r keyRepo) listKeys(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	var keys []string
	for _, prefix := range dbMetaPrefixes(r.db) {
		entries, err := r.s.eng.QueryPrefix(ctx, prefix, true)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			// e.Key = <prefix><encKey>/<encReplica>
			rest := strings.TrimPrefix(e.Key, prefix)
			enc := rest
			if i := strings.IndexByte(rest, '/'); i >= 0 {
				enc = rest[:i]
			}
			key, derr := decSeg(enc)
			if derr != nil {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if _, live, perr := r.s.probe(ctx, r.db, key); perr != nil {
				return nil, perr
			} else if live {
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (r keyRepo) Get(key string) (core.Key, error) {
	k, ok, err := r.s.probe(bg(), r.db, key)
	if err != nil {
		return core.Key{}, err
	}
	if !ok {
		return core.Key{}, core.ErrNotFound
	}
	return k, nil
}

func (r keyRepo) Exists(key string) (bool, error) {
	_, ok, err := r.s.probe(bg(), r.db, key)
	return ok, err
}

func (r keyRepo) Count(keys ...string) (int, error) {
	n := 0
	for _, key := range keys {
		ok, err := r.Exists(key)
		if err != nil {
			return 0, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}

func (r keyRepo) Len() (int, error) {
	keys, err := r.listKeys(bg())
	return len(keys), err
}

// Delete removes the given keys (DESIGN §6.9: tombstone meta + every element
// key). Returns the number of keys that existed and were removed.
func (r keyRepo) Delete(keys ...string) (int, error) {
	ctx := bg()
	n := 0
	for _, key := range keys {
		unlock := r.s.locks.Lock(lockKey(r.db, key))
		k, ok, err := r.s.probe(ctx, r.db, key)
		if err != nil {
			unlock()
			return n, err
		}
		if !ok {
			unlock()
			continue
		}
		if err := r.s.deleteKey(ctx, r.db, key, k.Type); err != nil {
			unlock()
			return n, err
		}
		unlock()
		n++
	}
	return n, nil
}

func (r keyRepo) DeleteAll() error {
	ctx := bg()
	keys, err := r.listKeys(ctx)
	if err != nil {
		return err
	}
	_, err = r.Delete(keys...)
	return err
}

func (r keyRepo) Expire(key string, ttl time.Duration) error {
	return r.ExpireAt(key, time.Now().Add(ttl))
}

func (r keyRepo) ExpireAt(key string, at time.Time) error {
	return r.setETime(key, at.UnixMilli())
}

func (r keyRepo) Persist(key string) error {
	return r.setETime(key, 0)
}

// setETime rewrites the key's meta with a new absolute expiry (0 = persist),
// preserving type/flavor/epoch. Returns ErrNotFound if the key is gone.
func (r keyRepo) setETime(key string, etimeMS int64) error {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if _, ok, err := r.s.probe(ctx, r.db, key); err != nil {
		return err
	} else if !ok {
		return core.ErrNotFound
	}
	m, _, ok, err := r.s.readMeta(ctx, r.db, key)
	if err != nil {
		return err
	}
	if !ok {
		return core.ErrNotFound
	}
	m.ETimeMS = etimeMS
	return r.s.writeMeta(ctx, r.db, key, m)
}

func (r keyRepo) Keys(pattern string) ([]core.Key, error) {
	ctx := bg()
	names, err := r.listKeys(ctx)
	if err != nil {
		return nil, err
	}
	var out []core.Key
	for _, name := range names {
		if pattern != "*" && pattern != "" && !match.Match(name, pattern) {
			continue
		}
		k, ok, err := r.s.probe(ctx, r.db, name)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, k)
		}
	}
	return out, nil
}

func (r keyRepo) Random() (core.Key, error) {
	ctx := bg()
	names, err := r.listKeys(ctx)
	if err != nil {
		return core.Key{}, err
	}
	if len(names) == 0 {
		return core.Key{}, core.ErrNotFound
	}
	name := names[rand.Intn(len(names))]
	k, _, err := r.s.probe(ctx, r.db, name)
	return k, err
}

// Scan returns all matching keys in one page (cursor 0). Pagination via the
// resumable numeric cursor (DESIGN §6.9) is a Phase 9 refinement; returning
// every key in one call is a valid SCAN per Redis semantics.
func (r keyRepo) Scan(cursor int, pattern string, ktype core.TypeID, count int) (restypes.KeyScanResult, error) {
	ctx := bg()
	names, err := r.listKeys(ctx)
	if err != nil {
		return restypes.KeyScanResult{}, err
	}
	var keys []core.Key
	for _, name := range names {
		if pattern != "" && pattern != "*" && !match.Match(name, pattern) {
			continue
		}
		k, ok, err := r.s.probe(ctx, r.db, name)
		if err != nil {
			return restypes.KeyScanResult{}, err
		}
		if !ok {
			continue
		}
		if ktype != core.TypeAny && k.Type != ktype {
			continue
		}
		keys = append(keys, k)
	}
	return restypes.KeyScanResult{Cursor: 0, Keys: keys}, nil
}

// Rename copies the key's data to newKey and deletes the old (DESIGN §6.9:
// non-atomic across replicas, batched locally).
func (r keyRepo) Rename(key, newKey string) error {
	ctx := bg()
	if key == newKey {
		if ok, err := r.Exists(key); err != nil {
			return err
		} else if !ok {
			return core.ErrNotFound
		}
		return nil
	}
	// Lock both keys in a stable order to avoid deadlock.
	first, second := lockKey(r.db, key), lockKey(r.db, newKey)
	if first > second {
		first, second = second, first
	}
	u1 := r.s.locks.Lock(first)
	defer u1()
	u2 := r.s.locks.Lock(second)
	defer u2()

	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return err
	}
	if !ok {
		return core.ErrNotFound
	}
	if err := r.s.copyKey(ctx, r.db, key, newKey, k); err != nil {
		return err
	}
	return r.s.deleteKey(ctx, r.db, key, k.Type)
}

func (r keyRepo) RenameNotExists(key, newKey string) (bool, error) {
	ok, err := r.Exists(newKey)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	if err := r.Rename(key, newKey); err != nil {
		return false, err
	}
	return true, nil
}
