package crdtstore

import (
	"context"
	"strconv"
	"time"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// strRepo implements redisapi.RStr against the per-replica HLC-LWW slot
// encoding (DESIGN §6.1, §6.7), bound to one logical DB.
type strRepo struct {
	s  *Store
	db int
}

// Get returns the live string value, core.ErrNotFound if absent/expired, or
// core.ErrKeyType if the key holds a non-string type.
func (r strRepo) Get(key string) (core.Value, error) {
	return r.readValue(bg(), key)
}

// readValue returns a string key's live value, handling both the plain-string
// flavor (LWW value slot) and the counter flavors (sum of components, §6.4).
func (r strRepo) readValue(ctx context.Context, key string) (core.Value, error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, core.ErrNotFound
	}
	if k.Type != core.TypeString {
		return nil, core.ErrKeyType
	}
	m, _, _, err := r.s.readMeta(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	switch m.Flavor {
	case flavorCounter:
		sum, err := r.s.counterSumInt(ctx, r.db, key)
		if err != nil {
			return nil, err
		}
		return core.Value(strconv.FormatInt(sum, 10)), nil
	case flavorCounterFloat:
		sum, err := r.s.counterSumFloat(ctx, r.db, key)
		if err != nil {
			return nil, err
		}
		return core.Value(ftoa(sum)), nil
	}
	cands, err := r.s.readSlots(ctx, strBase(r.db, key))
	if err != nil {
		return nil, err
	}
	v, live := liveValue(cands)
	if !live {
		return nil, core.ErrNotFound
	}
	return core.Value(v), nil
}

// GetMany returns the values of the given keys; missing keys are omitted.
func (r strRepo) GetMany(keys ...string) (map[string]core.Value, error) {
	out := make(map[string]core.Value, len(keys))
	for _, key := range keys {
		v, err := r.Get(key)
		switch err {
		case nil:
			out[key] = v
		case core.ErrNotFound, core.ErrKeyType:
			// MGET reports a nil for missing or wrong-type keys.
		default:
			return nil, err
		}
	}
	return out, nil
}

// Set writes the key to value as a plain string, clearing any TTL (Redis SET
// semantics).
func (r strRepo) Set(key string, value any) error {
	return r.SetExpire(key, value, 0)
}

// SetExpire writes the key with a relative TTL (0 = no expiry).
func (r strRepo) SetExpire(key string, value any, ttl time.Duration) error {
	b, err := core.ToBytes(value)
	if err != nil {
		return err
	}
	etimeMS := int64(0)
	if ttl > 0 {
		etimeMS = nowMS() + ttl.Milliseconds()
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	return r.writeString(ctx, key, b, etimeMS, false)
}

// writeString writes the value slot and meta for a plain string. When keepTTL
// is false the TTL is set to etimeMS (0 clears it); when true the existing TTL
// is preserved.
func (r strRepo) writeString(ctx context.Context, key string, b []byte, etimeMS int64, keepTTL bool) error {
	// DESIGN §6.4: counters and plain strings are distinct flavors and don't
	// mix. SET on a counter key is rejected (default policy) to eliminate the
	// cross-replica SET-vs-INCR race.
	if m, _, ok, err := r.s.readMeta(ctx, r.db, key); err == nil && ok && m.Flavor.isCounter() {
		live, _ := r.s.counterExists(ctx, r.db, key)
		if live {
			return core.ErrKeyType
		}
	}
	if keepTTL {
		if m, _, ok, err := r.s.readMeta(ctx, r.db, key); err == nil && ok {
			etimeMS = m.ETimeMS
		}
	}
	if err := r.s.writeSlot(ctx, strSlot(r.db, key, r.s.replica()), tagPresent, b); err != nil {
		return err
	}
	return r.s.writeMeta(ctx, r.db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etimeMS},
		Flavor:  flavorString,
	})
}

// SetMany writes multiple key/value pairs (one CRDT batch would be ideal; for
// now each is an independent write — atomic propagation lands with MULTI).
func (r strRepo) SetMany(items map[string]any) error {
	for key, value := range items {
		if err := r.Set(key, value); err != nil {
			return err
		}
	}
	return nil
}

// SetWith applies the full SET grammar (NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL).
// Existence checks are node-local only (DESIGN §6.1: best-effort under
// concurrency).
func (r strRepo) SetWith(key string, value any, opts restypes.SetOpts) (restypes.SetOut, error) {
	b, err := core.ToBytes(value)
	if err != nil {
		return restypes.SetOut{}, err
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()

	var out restypes.SetOut
	prev, prevErr := r.getLocked(ctx, key)
	exists := prevErr == nil
	if opts.Get {
		if prevErr == core.ErrKeyType {
			return out, core.ErrKeyType
		}
		out.Prev = prev
	}
	if opts.IfNotExists && exists {
		return out, nil // NX on existing key: no write
	}
	if opts.IfExists && !exists {
		return out, nil // XX on missing key: no write
	}

	etimeMS := int64(0)
	switch {
	case opts.AtMS > 0:
		etimeMS = opts.AtMS
	case opts.TTLMS > 0:
		etimeMS = nowMS() + opts.TTLMS
	}
	if err := r.writeString(ctx, key, b, etimeMS, opts.KeepTTL); err != nil {
		return out, err
	}
	out.Created = !exists
	out.Updated = exists
	return out, nil
}

// getLocked reads the current value assuming the key lock is held.
func (r strRepo) getLocked(ctx context.Context, key string) (core.Value, error) {
	return r.readValue(ctx, key)
}

// flavorFor returns the existing counter flavor for a key, or flavorNone if the
// key is absent. Returns ErrKeyType if the key exists as a non-string type or
// as a plain (non-counter) string — INCR/INCRBYFLOAT don't mix with plain
// strings (DESIGN §6.4).
func (r strRepo) counterFlavor(ctx context.Context, key string, want strFlavor) (strFlavor, error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil {
		return flavorNone, err
	}
	if !ok {
		return flavorNone, nil // new key: caller creates the counter
	}
	if k.Type != core.TypeString {
		return flavorNone, core.ErrKeyType
	}
	m, _, _, err := r.s.readMeta(ctx, r.db, key)
	if err != nil {
		return flavorNone, err
	}
	if !m.Flavor.isCounter() || m.Flavor != want {
		// Plain string, or the other counter flavor: reject (no mixing).
		return flavorNone, core.ErrValueType
	}
	return m.Flavor, nil
}

// Incr applies an integer delta using the per-replica PN-counter codec (§6.4),
// returning the new global total. The key must be absent or an integer counter.
func (r strRepo) Incr(key string, delta int) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if _, err := r.counterFlavor(ctx, key, flavorCounter); err != nil {
		return 0, err
	}
	etimeMS, err := r.keepETime(ctx, key)
	if err != nil {
		return 0, err
	}
	total, err := r.s.incrInt(ctx, r.db, key, int64(delta), etimeMS)
	if err != nil {
		return 0, err
	}
	return int(total), nil
}

// IncrFloat applies a float delta using the per-replica PN-counter codec.
func (r strRepo) IncrFloat(key string, delta float64) (float64, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if _, err := r.counterFlavor(ctx, key, flavorCounterFloat); err != nil {
		return 0, err
	}
	etimeMS, err := r.keepETime(ctx, key)
	if err != nil {
		return 0, err
	}
	return r.s.incrFloat(ctx, r.db, key, delta, etimeMS)
}

// keepETime returns the key's current expiry so INCR preserves it.
func (r strRepo) keepETime(ctx context.Context, key string) (int64, error) {
	m, _, ok, err := r.s.readMeta(ctx, r.db, key)
	if err != nil || !ok {
		return 0, err
	}
	return m.ETimeMS, nil
}
