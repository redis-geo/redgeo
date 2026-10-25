package crdtstore

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"

	ds "github.com/ipfs/go-datastore"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// Probabilistic sketches (Bloom filter, count-min sketch, top-k). All three keep
// state as per-replica components that merge on read — Bloom bits by OR,
// count-min cells by sum, top-k summaries by summing and re-ranking — so
// concurrent writers on different replicas never touch the same key and the
// result is independent of arrival order (DESIGN §6.4-style).
//
// Layout under /{P}/d/{db}/{key}/s/:
//
//	p/{replica}            parameters: HLC-LWW slot of [kind][encoded params]
//	h{phash}/…             the data, namespaced by a hash of the parameters
//
// Merging only makes sense between components built with the same parameters, so
// data lives under a path derived from them: if two replicas reserve different
// parameters concurrently, the LWW winner's data is used and the loser's adds are
// ignored (never mixed in as garbage). Identical reservations — including the
// default parameters used for auto-creation — share one namespace.

const (
	skBloom byte = 1
	skCMS   byte = 2
	skTopK  byte = 3
)

// ErrSketchExists is returned when a reserve/init finds the key already holds
// a sketch.
var ErrSketchExists = redis.ErrSketchExists

func sketchType(kind byte) core.TypeID {
	switch kind {
	case skBloom:
		return core.TypeBloom
	case skCMS:
		return core.TypeCMS
	}
	return core.TypeTopK
}

func sketchBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/s/", part(db, key), db, encSeg(key))
}

func sketchParamsBase(db int, key string) string { return sketchBase(db, key) + "p/" }

type sketchParams struct {
	kind byte
	raw  []byte // encoded parameters
	data string // data prefix: .../s/h{phash}/
}

// sketchDataPrefix derives the data namespace from the stored parameter value.
func sketchDataPrefix(db int, key string, value []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(value)
	return fmt.Sprintf("%sh%016x/", sketchBase(db, key), h.Sum64())
}

// readSketchParams returns the winning live parameters, ok=false if none.
func (s *Store) readSketchParams(ctx context.Context, db int, key string) (sketchParams, bool, error) {
	cands, err := s.readSlots(ctx, sketchParamsBase(db, key))
	if err != nil {
		return sketchParams{}, false, err
	}
	v, live := liveValue(cands)
	if !live || len(v) < 1 {
		return sketchParams{}, false, nil
	}
	return sketchParams{kind: v[0], raw: append([]byte(nil), v[1:]...), data: sketchDataPrefix(db, key, v)}, true, nil
}

func (s *Store) writeSketchParams(ctx context.Context, db int, key string, kind byte, raw []byte, etimeMS int64) (sketchParams, error) {
	v := append([]byte{kind}, raw...)
	slotKey := ds.NewKey(sketchParamsBase(db, key) + encSeg(s.replica()))
	if err := s.writeSlot(ctx, slotKey, tagPresent, v); err != nil {
		return sketchParams{}, err
	}
	err := s.writeMeta(ctx, db, key, metaEnvelope{KeyMeta: KeyMeta{Type: sketchType(kind), ETimeMS: etimeMS}})
	return sketchParams{kind: kind, raw: raw, data: sketchDataPrefix(db, key, v)}, err
}

// sketchOpen resolves a key for a given sketch kind: its parameters if it holds
// one, core.ErrKeyType if it holds something else, ok=false if absent.
func (s *Store) sketchOpen(ctx context.Context, db int, key string, kind byte) (sp sketchParams, ok bool, etimeMS int64, err error) {
	k, live, err := s.probe(ctx, db, key)
	if err != nil || !live {
		return sketchParams{}, false, 0, err
	}
	if k.Type != sketchType(kind) {
		return sketchParams{}, false, 0, core.ErrKeyType
	}
	if k.ETime != nil {
		etimeMS = *k.ETime
	}
	sp, ok, err = s.readSketchParams(ctx, db, key)
	return sp, ok, etimeMS, err
}

// deleteSketch removes every component of every replica, then the parameters
// and meta. (Components are OR-Set removed like counters; a component another
// replica wrote and we have not seen yet will survive, add-wins.)
func (s *Store) deleteSketch(ctx context.Context, db int, key string) error {
	entries, err := s.query(ctx, sketchBase(db, key), true)
	if err != nil {
		return err
	}
	params := sketchParamsBase(db, key)
	for _, e := range entries {
		if strings.HasPrefix(e.Key, params) {
			continue
		}
		if err := s.del(ctx, ds.NewKey(e.Key)); err != nil {
			return err
		}
	}
	slotKey := ds.NewKey(params + encSeg(s.replica()))
	if err := s.writeSlot(ctx, slotKey, tagDeleted, nil); err != nil {
		return err
	}
	return s.deleteMeta(ctx, db, key)
}

// copySketch collapses a sketch's merged state into this replica's components
// under newKey (RENAME).
func (s *Store) copySketch(ctx context.Context, db int, key, newKey string) error {
	sp, ok, err := s.readSketchParams(ctx, db, key)
	if err != nil || !ok {
		return err
	}
	np, err := s.writeSketchParams(ctx, db, newKey, sp.kind, sp.raw, 0)
	if err != nil {
		return err
	}
	switch sp.kind {
	case skBloom:
		return s.copyBloom(ctx, sp, np)
	case skCMS:
		return s.copyCMS(ctx, sp, np)
	case skTopK:
		return s.copyTopK(ctx, sp, np)
	}
	return fmt.Errorf("crdtstore: RENAME of sketch kind %d not supported", sp.kind)
}

// lastSeg decodes the trailing replica segment of a component key.
func replicaOf(key string) (string, bool) {
	r, err := decSeg(lastSegment(key))
	return r, err == nil
}
