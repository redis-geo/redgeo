package crdtstore

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// Streams: an append-only log whose entries are per-replica HLC-LWW slots keyed
// by ID (DESIGN §6.7). Concurrent XADDs from different replicas never touch the
// same key. To keep IDs unique without coordination, auto-generated IDs embed a
// 16-bit replica discriminator in the low bits of seq:
//
//	seq = counter<<16 | disc(replica)
//
// so IDs stay valid, strictly increasing "ms-seq" pairs but the seq values are
// not 0,1,2… as in Redis. Each replica also keeps a max-merged "last ID"
// register (xl/); the stream exists while one is present, so — as in Redis — it
// survives XDEL of every entry. Entries are removed with deleted-tagged slots
// (XDEL/XTRIM/DEL); reads reduce each ID's slots to the max-HLC winner.
// Range reads scan the whole stream (O(n)); XADD itself is O(replicas).

var (
	errXaddZero  = errors.New("ERR The ID specified in XADD must be greater than 0-0")
	errXaddSmall = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
)

type streamRepo struct {
	s  *Store
	db int
}

func discriminator(replica string) uint64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(replica))
	return uint64(h.Sum32() & 0xffff)
}

// nextStreamID mints the ID for an XADD given the stream's highest known ID.
func nextStreamID(now uint64, last restypes.StreamID, o restypes.StreamAddOpts, disc uint64) (restypes.StreamID, error) {
	bump := func() restypes.StreamID { // strictly after last, within last.Ms
		return restypes.StreamID{Ms: last.Ms, Seq: ((last.Seq>>16)+1)<<16 | disc}
	}
	switch o.Mode {
	case restypes.IDAuto:
		if now > last.Ms {
			return restypes.StreamID{Ms: now, Seq: disc}, nil
		}
		return bump(), nil
	case restypes.IDAutoSeq:
		switch {
		case o.ID.Ms > last.Ms:
			seq := disc
			if o.ID.Ms == 0 && seq == 0 {
				seq = 1
			}
			return restypes.StreamID{Ms: o.ID.Ms, Seq: seq}, nil
		case o.ID.Ms == last.Ms:
			return bump(), nil
		}
		return restypes.StreamID{}, errXaddSmall
	}
	if o.ID == (restypes.StreamID{}) {
		return restypes.StreamID{}, errXaddZero
	}
	if o.ID.Cmp(last) <= 0 {
		return restypes.StreamID{}, errXaddSmall
	}
	return o.ID, nil
}

func encodeFields(f [][]byte) []byte {
	out := binary.AppendUvarint(nil, uint64(len(f)))
	for _, b := range f {
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
	}
	return out
}

func decodeFields(b []byte) ([][]byte, bool) {
	n, w := binary.Uvarint(b)
	if w <= 0 || n > uint64(len(b)) {
		return nil, false
	}
	b = b[w:]
	out := make([][]byte, 0, n)
	for i := uint64(0); i < n; i++ {
		l, w := binary.Uvarint(b)
		if w <= 0 || uint64(len(b)-w) < l {
			return nil, false
		}
		out = append(out, append([]byte(nil), b[w:w+int(l)]...))
		b = b[w+int(l):]
	}
	return out, true
}

func encodeID(id restypes.StreamID) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, id.Ms)
	binary.BigEndian.PutUint64(b[8:], id.Seq)
	return b
}

func decodeID(b []byte) (restypes.StreamID, bool) {
	if len(b) != 16 {
		return restypes.StreamID{}, false
	}
	return restypes.StreamID{Ms: binary.BigEndian.Uint64(b), Seq: binary.BigEndian.Uint64(b[8:])}, true
}

// streamLast returns the highest recorded ID across replicas and this
// replica's own recorded ID.
func (s *Store) streamLast(ctx context.Context, db int, key string) (max, own restypes.StreamID, err error) {
	base := streamLastBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return max, own, err
	}
	mine := s.replica()
	for _, e := range entries {
		id, ok := decodeID(e.Value)
		if !ok {
			continue
		}
		if id.Cmp(max) > 0 {
			max = id
		}
		if r, derr := decSeg(strings.TrimPrefix(e.Key, base)); derr == nil && r == mine {
			own = id
		}
	}
	return max, own, nil
}

// streamLive returns every live entry sorted by ID.
func (s *Store) streamLive(ctx context.Context, db int, key string) ([]restypes.StreamEntry, error) {
	base := streamBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return nil, err
	}
	byID := map[restypes.StreamID]map[string]slot{}
	for _, e := range entries {
		rest := strings.TrimPrefix(e.Key, base) // {ms}-{seq}/{replica}
		if len(rest) < 35 || rest[16] != '-' || rest[33] != '/' {
			continue
		}
		ms, e1 := strconv.ParseUint(rest[:16], 16, 64)
		seq, e2 := strconv.ParseUint(rest[17:33], 16, 64)
		replica, e3 := decSeg(rest[34:])
		sl, e4 := decodeSlot(e.Value)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			continue
		}
		id := restypes.StreamID{Ms: ms, Seq: seq}
		if byID[id] == nil {
			byID[id] = map[string]slot{}
		}
		byID[id][replica] = sl
	}
	out := make([]restypes.StreamEntry, 0, len(byID))
	for id, cands := range byID {
		v, ok := liveValue(cands)
		if !ok {
			continue
		}
		f, ok := decodeFields(v)
		if !ok {
			continue
		}
		out = append(out, restypes.StreamEntry{ID: id, Fields: f})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.Cmp(out[j].ID) < 0 })
	return out, nil
}

func (r streamRepo) ensureStream(ctx context.Context, key string) (exists bool, etimeMS int64, err error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil || !ok {
		return false, 0, err
	}
	if k.Type != core.TypeStream {
		return false, 0, core.ErrKeyType
	}
	if k.ETime != nil {
		etimeMS = *k.ETime
	}
	return true, etimeMS, nil
}

func (s *Store) streamBumpLast(ctx context.Context, db int, key string, id restypes.StreamID) error {
	return s.put(ctx, streamLastSlot(db, key, s.replica()), encodeID(id))
}

func (s *Store) streamTombstone(ctx context.Context, db int, key string, id restypes.StreamID) error {
	return s.writeSlot(ctx, streamSlot(db, key, id.Ms, id.Seq, s.replica()), tagDeleted, nil)
}

func (r streamRepo) Add(key string, opts restypes.StreamAddOpts, fields [][]byte) (restypes.StreamID, bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	exists, etime, err := r.ensureStream(ctx, key)
	if err != nil {
		return restypes.StreamID{}, false, err
	}
	if !exists && opts.NoMkStream {
		return restypes.StreamID{}, false, nil
	}
	last, own, err := r.s.streamLast(ctx, r.db, key)
	if err != nil {
		return restypes.StreamID{}, false, err
	}
	id, err := nextStreamID(uint64(nowMS()), last, opts, discriminator(r.s.replica()))
	if err != nil {
		return restypes.StreamID{}, false, err
	}
	if err := r.s.writeSlot(ctx, streamSlot(r.db, key, id.Ms, id.Seq, r.s.replica()), tagPresent, encodeFields(fields)); err != nil {
		return restypes.StreamID{}, false, err
	}
	if id.Cmp(own) > 0 {
		if err := r.s.streamBumpLast(ctx, r.db, key, id); err != nil {
			return restypes.StreamID{}, false, err
		}
	}
	if !exists {
		if err := r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeStream, ETimeMS: etime}}); err != nil {
			return restypes.StreamID{}, false, err
		}
	}
	if opts.Trim != nil {
		if _, err := r.trimLocked(ctx, key, *opts.Trim); err != nil {
			return restypes.StreamID{}, false, err
		}
	}
	return id, true, nil
}

func (r streamRepo) Len(key string) (int, error) {
	ctx := bg()
	if ok, _, err := r.ensureStream(ctx, key); err != nil || !ok {
		return 0, err
	}
	es, err := r.s.streamLive(ctx, r.db, key)
	return len(es), err
}

func (r streamRepo) Range(key string, lo, hi restypes.StreamBound, count int, rev bool) ([]restypes.StreamEntry, error) {
	ctx := bg()
	if ok, _, err := r.ensureStream(ctx, key); err != nil || !ok {
		return nil, err
	}
	es, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	var out []restypes.StreamEntry
	in := func(e restypes.StreamEntry) bool {
		c := e.ID.Cmp(lo.ID)
		if c < 0 || (c == 0 && lo.Excl) {
			return false
		}
		c = e.ID.Cmp(hi.ID)
		return c < 0 || (c == 0 && !hi.Excl)
	}
	if rev {
		for i := len(es) - 1; i >= 0; i-- {
			if in(es[i]) {
				out = append(out, es[i])
			}
			if count > 0 && len(out) == count {
				break
			}
		}
		return out, nil
	}
	for _, e := range es {
		if in(e) {
			out = append(out, e)
		}
		if count > 0 && len(out) == count {
			break
		}
	}
	return out, nil
}

func (r streamRepo) Del(key string, ids ...restypes.StreamID) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if ok, _, err := r.ensureStream(ctx, key); err != nil || !ok {
		return 0, err
	}
	es, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return 0, err
	}
	live := make(map[restypes.StreamID]bool, len(es))
	for _, e := range es {
		live[e.ID] = true
	}
	n := 0
	for _, id := range ids {
		if live[id] {
			if err := r.s.streamTombstone(ctx, r.db, key, id); err != nil {
				return n, err
			}
			live[id] = false
			n++
		}
	}
	return n, nil
}

func (r streamRepo) Trim(key string, t restypes.StreamTrim) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	if ok, _, err := r.ensureStream(ctx, key); err != nil || !ok {
		return 0, err
	}
	return r.trimLocked(ctx, key, t)
}

func (r streamRepo) trimLocked(ctx context.Context, key string, t restypes.StreamTrim) (int, error) {
	es, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return 0, err
	}
	cut := 0 // entries[:cut] are removed (they are the oldest)
	switch {
	case t.ByMaxLen && int64(len(es)) > t.MaxLen:
		cut = len(es) - int(t.MaxLen)
	case t.ByMinID:
		cut = sort.Search(len(es), func(i int) bool { return es[i].ID.Cmp(t.MinID) >= 0 })
	}
	for _, e := range es[:cut] {
		if err := r.s.streamTombstone(ctx, r.db, key, e.ID); err != nil {
			return 0, err
		}
	}
	return cut, nil
}

func (r streamRepo) LastID(key string) (restypes.StreamID, error) {
	ctx := bg()
	if ok, _, err := r.ensureStream(ctx, key); err != nil || !ok {
		return restypes.StreamID{}, err
	}
	last, _, err := r.s.streamLast(ctx, r.db, key)
	return last, err
}

func (s *Store) deleteStream(ctx context.Context, db int, key string) error {
	es, err := s.streamLive(ctx, db, key)
	if err != nil {
		return err
	}
	for _, e := range es {
		if err := s.streamTombstone(ctx, db, key, e.ID); err != nil {
			return err
		}
	}
	if err := s.deleteGroups(ctx, db, key); err != nil {
		return err
	}
	base := streamLastBase(db, key)
	ks, err := s.query(ctx, base, true)
	if err != nil {
		return err
	}
	for _, e := range ks {
		if r, derr := decSeg(strings.TrimPrefix(e.Key, base)); derr == nil {
			if err := s.del(ctx, streamLastSlot(db, key, r)); err != nil {
				return err
			}
		}
	}
	return s.deleteMeta(ctx, db, key)
}

func (s *Store) copyStream(ctx context.Context, db int, key, newKey string) error {
	es, err := s.streamLive(ctx, db, key)
	if err != nil {
		return err
	}
	for _, e := range es {
		if err := s.writeSlot(ctx, streamSlot(db, newKey, e.ID.Ms, e.ID.Seq, s.replica()), tagPresent, encodeFields(e.Fields)); err != nil {
			return err
		}
	}
	last, _, err := s.streamLast(ctx, db, key)
	if err != nil {
		return err
	}
	if err := s.streamBumpLast(ctx, db, newKey, last); err != nil {
		return err
	}
	if err := s.copyGroups(ctx, db, key, newKey); err != nil {
		return err
	}
	return s.writeMeta(ctx, db, newKey, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeStream}})
}
