package crdtstore

import (
	"context"
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
	ctx := bg()
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

// Incr / IncrFloat: minimal single-node implementation on the string register
// (Phase 1). Phase 4 replaces this with the per-replica PN-counter component
// codec (§6.4) and the distinct counter flavor.
func (r strRepo) Incr(key string, delta int) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	cur := 0
	v, err := r.getLocked(ctx, key)
	switch err {
	case nil:
		n, perr := v.Int()
		if perr != nil {
			return 0, core.ErrValueType
		}
		cur = n
	case core.ErrNotFound:
	case core.ErrKeyType:
		return 0, core.ErrKeyType
	default:
		return 0, err
	}
	next := cur + delta
	if err := r.writeString(ctx, key, []byte(itoa(next)), 0, true); err != nil {
		return 0, err
	}
	return next, nil
}

func (r strRepo) IncrFloat(key string, delta float64) (float64, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	cur := 0.0
	v, err := r.getLocked(ctx, key)
	switch err {
	case nil:
		f, perr := v.Float()
		if perr != nil {
			return 0, core.ErrValueType
		}
		cur = f
	case core.ErrNotFound:
	case core.ErrKeyType:
		return 0, core.ErrKeyType
	default:
		return 0, err
	}
	next := cur + delta
	if err := r.writeString(ctx, key, ftoa(next), 0, true); err != nil {
		return 0, err
	}
	return next, nil
}
