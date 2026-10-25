package crdtstore

import (
	"context"
	"encoding/binary"
	ds "github.com/ipfs/go-datastore"
	"sort"
	"strconv"
	"strings"

	redis "github.com/redis-geo/redgeo/redisapi"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// Stream consumer groups. Everything a group knows is a set of per-replica
// slots, so replicas never write the same key (DESIGN §6.7):
//   - existence, pending entries and consumers are HLC-LWW slots (XACK and
//     XGROUP DESTROY write deleted-tagged slots);
//   - the last-delivered ID is a max-merged register of (epoch, id): deliveries
//     advance the id, XGROUP SETID bumps the epoch so it can move backwards.
//
// Nothing here can make two regions agree on who receives an entry: if both
// deliver from the same last-delivered ID before they sync, both hand out the
// same entries — delivery is at-least-once across replicas (exactly-once only
// within one node, where the key lock serialises XREADGROUP). The later
// delivery wins the pending record.

type pelValue struct {
	consumer    string
	deliveredMS int64
	count       int
}

func encodePel(v pelValue) []byte {
	b := make([]byte, 12, 12+len(v.consumer))
	binary.BigEndian.PutUint64(b, uint64(v.deliveredMS))
	binary.BigEndian.PutUint32(b[8:], uint32(v.count))
	return append(b, v.consumer...)
}

func decodePel(b []byte) (pelValue, bool) {
	if len(b) < 12 {
		return pelValue{}, false
	}
	return pelValue{
		deliveredMS: int64(binary.BigEndian.Uint64(b)),
		count:       int(binary.BigEndian.Uint32(b[8:])),
		consumer:    string(b[12:]),
	}, true
}

type groupData struct {
	meta  map[string]slot
	epoch uint64
	last  restypes.StreamID
	pel   map[restypes.StreamID]map[string]slot
	cons  map[string]map[string]slot
}

func newGroupData() *groupData {
	return &groupData{
		meta: map[string]slot{},
		pel:  map[restypes.StreamID]map[string]slot{},
		cons: map[string]map[string]slot{},
	}
}

func (g *groupData) live() bool {
	_, ok := liveValue(g.meta)
	return ok
}

// loadGroups reads the state of one group (only != "") or all groups of a stream.
func (s *Store) loadGroups(ctx context.Context, db int, key, only string) (map[string]*groupData, error) {
	prefix := groupsBase(db, key)
	if only != "" {
		prefix = groupBase(db, key, only)
	}
	entries, err := s.query(ctx, prefix, false)
	if err != nil {
		return nil, err
	}
	out := map[string]*groupData{}
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, prefix), "/")
		name := only
		if only == "" {
			if len(parts) < 3 {
				continue
			}
			n, err := decSeg(parts[0])
			if err != nil {
				continue
			}
			name, parts = n, parts[1:]
		}
		g := out[name]
		if g == nil {
			g = newGroupData()
			out[name] = g
		}
		replica, err := decSeg(parts[len(parts)-1])
		if err != nil {
			continue
		}
		switch {
		case parts[0] == "m" && len(parts) == 2:
			if sl, err := decodeSlot(e.Value); err == nil {
				g.meta[replica] = sl
			}
		case parts[0] == "l" && len(parts) == 2 && len(e.Value) == 24:
			epoch := binary.BigEndian.Uint64(e.Value)
			id, _ := decodeID(e.Value[8:])
			if epoch > g.epoch || (epoch == g.epoch && id.Cmp(g.last) > 0) {
				g.epoch, g.last = epoch, id
			}
		case parts[0] == "p" && len(parts) == 3:
			ms, e1 := strconv.ParseUint(parts[1][:16], 16, 64)
			seq, e2 := strconv.ParseUint(parts[1][17:], 16, 64)
			sl, e3 := decodeSlot(e.Value)
			if e1 != nil || e2 != nil || e3 != nil {
				continue
			}
			id := restypes.StreamID{Ms: ms, Seq: seq}
			if g.pel[id] == nil {
				g.pel[id] = map[string]slot{}
			}
			g.pel[id][replica] = sl
		case parts[0] == "c" && len(parts) == 3:
			cname, err := decSeg(parts[1])
			sl, e2 := decodeSlot(e.Value)
			if err != nil || e2 != nil {
				continue
			}
			if g.cons[cname] == nil {
				g.cons[cname] = map[string]slot{}
			}
			g.cons[cname][replica] = sl
		}
	}
	for name, g := range out {
		if !g.live() {
			delete(out, name)
		}
	}
	return out, nil
}

// info turns loaded state into the public view.
func (g *groupData) info(name string) restypes.GroupInfo {
	gi := restypes.GroupInfo{Name: name, LastID: g.last}
	pending := map[string]int{}
	for id, cands := range g.pel {
		v, ok := liveValue(cands)
		if !ok {
			continue
		}
		pv, ok := decodePel(v)
		if !ok {
			continue
		}
		gi.Pending = append(gi.Pending, restypes.PendingEntry{ID: id, Consumer: pv.consumer, DeliveredMS: pv.deliveredMS, Count: pv.count})
		pending[pv.consumer]++
	}
	sort.Slice(gi.Pending, func(i, j int) bool { return gi.Pending[i].ID.Cmp(gi.Pending[j].ID) < 0 })
	seen := map[string]bool{}
	for cname, cands := range g.cons {
		v, ok := liveValue(cands)
		if !ok {
			continue
		}
		ci := restypes.ConsumerInfo{Name: cname, Pending: pending[cname]}
		if len(v) == 8 {
			ci.SeenMS = int64(binary.BigEndian.Uint64(v))
		}
		gi.Consumers = append(gi.Consumers, ci)
		seen[cname] = true
	}
	sort.Slice(gi.Consumers, func(i, j int) bool { return gi.Consumers[i].Name < gi.Consumers[j].Name })
	return gi
}

func (g *groupData) consumerLive(name string) bool {
	_, ok := liveValue(g.cons[name])
	return ok
}

// ---- writers ----

func (s *Store) gWriteMeta(ctx context.Context, db int, key, group string, tag byte) error {
	return s.writeSlot(ctx, groupMetaSlot(db, key, group, s.replica()), tag, nil)
}

func (s *Store) gWriteLast(ctx context.Context, db int, key, group string, epoch uint64, id restypes.StreamID) error {
	v := binary.BigEndian.AppendUint64(nil, epoch)
	return s.put(ctx, groupLastSlot(db, key, group, s.replica()), append(v, encodeID(id)...))
}

func (s *Store) gWritePel(ctx context.Context, db int, key, group string, id restypes.StreamID, v pelValue) error {
	return s.writeSlot(ctx, groupPelSlot(db, key, group, id.Ms, id.Seq, s.replica()), tagPresent, encodePel(v))
}

func (s *Store) gDelPel(ctx context.Context, db int, key, group string, id restypes.StreamID) error {
	return s.writeSlot(ctx, groupPelSlot(db, key, group, id.Ms, id.Seq, s.replica()), tagDeleted, nil)
}

func (s *Store) gTouchConsumer(ctx context.Context, db int, key, group, consumer string) error {
	v := binary.BigEndian.AppendUint64(nil, uint64(nowMS()))
	return s.writeSlot(ctx, groupConsumerSlot(db, key, group, consumer, s.replica()), tagPresent, v)
}

// destroyGroupData tombstones every record of a group.
func (s *Store) destroyGroupData(ctx context.Context, db int, key, group string, g *groupData) error {
	for id, cands := range g.pel {
		if _, ok := liveValue(cands); ok {
			if err := s.gDelPel(ctx, db, key, group, id); err != nil {
				return err
			}
		}
	}
	for c, cands := range g.cons {
		if _, ok := liveValue(cands); ok {
			if err := s.writeSlot(ctx, groupConsumerSlot(db, key, group, c, s.replica()), tagDeleted, nil); err != nil {
				return err
			}
		}
	}
	last, err := s.query(ctx, groupBase(db, key, group)+"l/", true)
	if err != nil {
		return err
	}
	for _, e := range last {
		if err := s.del(ctx, ds.NewKey(e.Key)); err != nil {
			return err
		}
	}
	return s.gWriteMeta(ctx, db, key, group, tagDeleted)
}

// ---- repo methods ----

// open locks the stream key, verifies it exists and loads one group.
func (r streamRepo) open(ctx context.Context, key, group string) (*groupData, error) {
	ok, _, err := r.ensureStream(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, redis.ErrNoGroup
	}
	gs, err := r.s.loadGroups(ctx, r.db, key, group)
	if err != nil {
		return nil, err
	}
	g := gs[group]
	if g == nil {
		return nil, redis.ErrNoGroup
	}
	return g, nil
}

func (r streamRepo) GroupCreate(key, group string, id restypes.StreamID, mkstream bool) error {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	exists, _, err := r.ensureStream(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		if !mkstream {
			return redis.ErrNoGroup
		}
		if err := r.s.streamBumpLast(ctx, r.db, key, restypes.StreamID{}); err != nil {
			return err
		}
		if err := r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeStream}}); err != nil {
			return err
		}
	}
	gs, err := r.s.loadGroups(ctx, r.db, key, group)
	if err != nil {
		return err
	}
	if gs[group] != nil {
		return redis.ErrGroupExists
	}
	if err := r.s.gWriteMeta(ctx, r.db, key, group, tagPresent); err != nil {
		return err
	}
	return r.s.gWriteLast(ctx, r.db, key, group, 0, id)
}

func (r streamRepo) GroupDestroy(key, group string) (bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err == redis.ErrNoGroup {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, r.s.destroyGroupData(ctx, r.db, key, group, g)
}

func (r streamRepo) GroupSetID(key, group string, id restypes.StreamID) error {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return err
	}
	return r.s.gWriteLast(ctx, r.db, key, group, g.epoch+1, id)
}

func (r streamRepo) ConsumerCreate(key, group, consumer string) (bool, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return false, err
	}
	isNew := !g.consumerLive(consumer)
	return isNew, r.s.gTouchConsumer(ctx, r.db, key, group, consumer)
}

func (r streamRepo) ConsumerDelete(key, group, consumer string) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, p := range g.info(group).Pending {
		if p.Consumer == consumer {
			if err := r.s.gDelPel(ctx, r.db, key, group, p.ID); err != nil {
				return 0, err
			}
			n++
		}
	}
	if g.consumerLive(consumer) {
		err = r.s.writeSlot(ctx, groupConsumerSlot(r.db, key, group, consumer, r.s.replica()), tagDeleted, nil)
	}
	return n, err
}

func (r streamRepo) Groups(key string) ([]restypes.GroupInfo, error) {
	ctx := bg()
	ok, _, err := r.ensureStream(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, redis.ErrNoGroup
	}
	gs, err := r.s.loadGroups(ctx, r.db, key, "")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(gs))
	for n := range gs {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]restypes.GroupInfo, 0, len(names))
	for _, n := range names {
		out = append(out, gs[n].info(n))
	}
	return out, nil
}

func (r streamRepo) Group(key, group string) (restypes.GroupInfo, error) {
	g, err := r.open(bg(), key, group)
	if err != nil {
		return restypes.GroupInfo{}, err
	}
	return g.info(group), nil
}

func (r streamRepo) ReadNew(key, group, consumer string, count int, noack bool) ([]restypes.StreamEntry, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return nil, err
	}
	all, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	var out []restypes.StreamEntry
	for _, e := range all {
		if e.ID.Cmp(g.last) <= 0 {
			continue
		}
		out = append(out, e)
		if count > 0 && len(out) == count {
			break
		}
	}
	if err := r.s.gTouchConsumer(ctx, r.db, key, group, consumer); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	now := nowMS()
	if !noack {
		for _, e := range out {
			prev := 0
			if v, ok := liveValue(g.pel[e.ID]); ok {
				if pv, ok := decodePel(v); ok {
					prev = pv.count
				}
			}
			if err := r.s.gWritePel(ctx, r.db, key, group, e.ID, pelValue{consumer: consumer, deliveredMS: now, count: prev + 1}); err != nil {
				return nil, err
			}
		}
	}
	return out, r.s.gWriteLast(ctx, r.db, key, group, g.epoch, out[len(out)-1].ID)
}

func (r streamRepo) ReadPending(key, group, consumer string, after restypes.StreamID, count int) ([]restypes.StreamEntry, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return nil, err
	}
	all, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return nil, err
	}
	byID := make(map[restypes.StreamID]restypes.StreamEntry, len(all))
	for _, e := range all {
		byID[e.ID] = e
	}
	var out []restypes.StreamEntry
	for _, p := range g.info(group).Pending {
		if p.Consumer != consumer || p.ID.Cmp(after) <= 0 {
			continue
		}
		e, ok := byID[p.ID]
		if !ok {
			e = restypes.StreamEntry{ID: p.ID} // deleted from the stream: nil fields
		}
		out = append(out, e)
		if count > 0 && len(out) == count {
			break
		}
	}
	return out, r.s.gTouchConsumer(ctx, r.db, key, group, consumer)
}

func (r streamRepo) Ack(key, group string, ids ...restypes.StreamID) (int, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err == redis.ErrNoGroup {
		return 0, nil // XACK on a missing group acknowledges nothing
	}
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if _, ok := liveValue(g.pel[id]); !ok {
			continue
		}
		if err := r.s.gDelPel(ctx, r.db, key, group, id); err != nil {
			return n, err
		}
		delete(g.pel, id)
		n++
	}
	return n, nil
}

func (r streamRepo) Claim(key, group, consumer string, ids []restypes.StreamID, o restypes.ClaimOpts) ([]restypes.StreamEntry, []restypes.StreamID, error) {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	g, err := r.open(ctx, key, group)
	if err != nil {
		return nil, nil, err
	}
	all, err := r.s.streamLive(ctx, r.db, key)
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[restypes.StreamID]restypes.StreamEntry, len(all))
	for _, e := range all {
		byID[e.ID] = e
	}
	now := nowMS()
	var claimed []restypes.StreamEntry
	var deleted []restypes.StreamID
	for _, id := range ids {
		var pv pelValue
		v, inPel := liveValue(g.pel[id])
		if inPel {
			pv, _ = decodePel(v)
		}
		entry, inStream := byID[id]
		switch {
		case inPel && !inStream: // dropped from the stream: forget it
			if err := r.s.gDelPel(ctx, r.db, key, group, id); err != nil {
				return nil, nil, err
			}
			deleted = append(deleted, id)
			continue
		case !inPel && !(o.Force && inStream):
			continue
		case inPel && now-pv.deliveredMS < o.MinIdleMS:
			continue
		}
		pv.consumer = consumer
		switch {
		case o.TimeMS > 0:
			pv.deliveredMS = o.TimeMS
		case o.IdleMS > 0:
			pv.deliveredMS = now - o.IdleMS
		default:
			pv.deliveredMS = now
		}
		switch {
		case o.RetryCount >= 0:
			pv.count = o.RetryCount
		case !o.JustID:
			pv.count++
		}
		if !inPel && pv.count == 0 {
			pv.count = 1
		}
		if err := r.s.gWritePel(ctx, r.db, key, group, id, pv); err != nil {
			return nil, nil, err
		}
		claimed = append(claimed, entry)
	}
	return claimed, deleted, r.s.gTouchConsumer(ctx, r.db, key, group, consumer)
}

// copyGroups carries groups across RENAME.
func (s *Store) copyGroups(ctx context.Context, db int, key, newKey string) error {
	gs, err := s.loadGroups(ctx, db, key, "")
	if err != nil {
		return err
	}
	for name, g := range gs {
		if err := s.gWriteMeta(ctx, db, newKey, name, tagPresent); err != nil {
			return err
		}
		if err := s.gWriteLast(ctx, db, newKey, name, g.epoch, g.last); err != nil {
			return err
		}
		for id, cands := range g.pel {
			if v, ok := liveValue(cands); ok {
				if pv, ok := decodePel(v); ok {
					if err := s.gWritePel(ctx, db, newKey, name, id, pv); err != nil {
						return err
					}
				}
			}
		}
		for _, c := range g.info(name).Consumers {
			if err := s.gTouchConsumer(ctx, db, newKey, name, c.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) deleteGroups(ctx context.Context, db int, key string) error {
	gs, err := s.loadGroups(ctx, db, key, "")
	if err != nil {
		return err
	}
	for name, g := range gs {
		if err := s.destroyGroupData(ctx, db, key, name, g); err != nil {
			return err
		}
	}
	return nil
}
