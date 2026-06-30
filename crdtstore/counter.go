package crdtstore

import (
	"context"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
)

// PN-counter via per-replica component keys (DESIGN §6.4). Each replica writes
// ONLY its own component at /d/{db}/{key}/c/{replicaID}; the global value is the
// sum of all components, computed on read. Since no two replicas ever write the
// same component key, the store's height-wins register is exactly correct and
// the counter converges under concurrency — the showcase Tier-1 win.
//
// Integer counters (INCR family, flavorCounter) store int64 components; float
// counters (INCRBYFLOAT, flavorCounterFloat) store float64 components. The two
// flavors do not mix, just as counters and plain strings don't (§6.4).

// counterComponentsInt returns each replica's int64 component for a key.
func (s *Store) counterComponentsInt(ctx context.Context, db int, key string) (map[string]int64, error) {
	base := counterBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(entries))
	for _, e := range entries {
		replica, derr := decSeg(strings.TrimPrefix(e.Key, base))
		if derr != nil {
			continue
		}
		n, perr := strconv.ParseInt(string(e.Value), 10, 64)
		if perr != nil {
			continue
		}
		out[replica] = n
	}
	return out, nil
}

func (s *Store) counterSumInt(ctx context.Context, db int, key string) (int64, error) {
	comps, err := s.counterComponentsInt(ctx, db, key)
	if err != nil {
		return 0, err
	}
	var sum int64
	for _, n := range comps {
		sum += n
	}
	return sum, nil
}

func (s *Store) counterComponentsFloat(ctx context.Context, db int, key string) (map[string]float64, error) {
	base := counterBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(entries))
	for _, e := range entries {
		replica, derr := decSeg(strings.TrimPrefix(e.Key, base))
		if derr != nil {
			continue
		}
		f, perr := strconv.ParseFloat(string(e.Value), 64)
		if perr != nil {
			continue
		}
		out[replica] = f
	}
	return out, nil
}

func (s *Store) counterSumFloat(ctx context.Context, db int, key string) (float64, error) {
	comps, err := s.counterComponentsFloat(ctx, db, key)
	if err != nil {
		return 0, err
	}
	var sum float64
	for _, f := range comps {
		sum += f
	}
	return sum, nil
}

// counterExists reports whether any counter component exists for a key.
func (s *Store) counterExists(ctx context.Context, db int, key string) (bool, error) {
	return s.anyPrefix(ctx, counterBase(db, key))
}

// incrInt applies an integer delta to this replica's component and returns the
// new global total. Caller holds the key lock. flavor must already be verified
// as int-counter or new.
func (s *Store) incrInt(ctx context.Context, db int, key string, delta int64, etimeMS int64) (int64, error) {
	comps, err := s.counterComponentsInt(ctx, db, key)
	if err != nil {
		return 0, err
	}
	mine := comps[s.replica()]
	mine += delta
	if err := s.put(ctx, counterSlot(db, key, s.replica()), []byte(strconv.FormatInt(mine, 10))); err != nil {
		return 0, err
	}
	if err := s.writeMeta(ctx, db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etimeMS},
		Flavor:  flavorCounter,
	}); err != nil {
		return 0, err
	}
	var sum int64
	for r, n := range comps {
		if r == s.replica() {
			continue
		}
		sum += n
	}
	return sum + mine, nil
}

// incrFloat applies a float delta to this replica's component and returns the
// new global total.
func (s *Store) incrFloat(ctx context.Context, db int, key string, delta float64, etimeMS int64) (float64, error) {
	comps, err := s.counterComponentsFloat(ctx, db, key)
	if err != nil {
		return 0, err
	}
	mine := comps[s.replica()]
	mine += delta
	if err := s.put(ctx, counterSlot(db, key, s.replica()), ftoa(mine)); err != nil {
		return 0, err
	}
	if err := s.writeMeta(ctx, db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etimeMS},
		Flavor:  flavorCounterFloat,
	}); err != nil {
		return 0, err
	}
	var sum float64
	for r, n := range comps {
		if r == s.replica() {
			continue
		}
		sum += n
	}
	return sum + mine, nil
}

// deleteCounter tombstones every counter component of a key plus its meta.
func (s *Store) deleteCounter(ctx context.Context, db int, key string) error {
	base := counterBase(db, key)
	entries, err := s.query(ctx, base, true)
	if err != nil {
		return err
	}
	for _, e := range entries {
		replica, derr := decSeg(strings.TrimPrefix(e.Key, base))
		if derr != nil {
			continue
		}
		if err := s.del(ctx, counterSlot(db, key, replica)); err != nil {
			return err
		}
	}
	return s.deleteMeta(ctx, db, key)
}
