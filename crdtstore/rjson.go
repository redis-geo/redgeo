package crdtstore

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/jsondoc"
)

// JSON documents (RedisJSON subset). A document is a tree of nodes and every
// node is a per-replica HLC-LWW slot (DESIGN §6.7) keyed by its path:
//
//	/{P}/d/{db}/{key}/j/{seg}/{seg}/_/{replicaID}
//
// where seg is "f"+base32(name) for an object field and "a"+posKey for an array
// element (posKey is the order-preserving key lists use), and "_" marks the end
// of the path. A slot holds [kind][raw] (containers only [kind]). Different
// paths are different keys, so concurrent edits to different parts of one
// document merge; the same path is last-writer-wins. A deleted/replaced node's
// whole subtree is tombstoned, so it cannot reappear if the node is re-created.
// Reads and writes load the whole document (O(size)).

type jsonRepo struct {
	s  *Store
	db int
}

func jsonBase(db int, key string) string {
	return fmt.Sprintf("%s/d/%d/%s/j/", part(db, key), db, encSeg(key))
}

func jsonSlotKey(db int, key, path, replica string) string {
	if path == "" {
		return jsonBase(db, key) + "_/" + encSeg(replica)
	}
	return jsonBase(db, key) + path + "/_/" + encSeg(replica)
}

// jsonNewPos mints a replica-unique array position strictly between prev and
// next (either may be "" for an open end).
func (s *Store) jsonNewPos(prev, next string) string {
	lo, hi := 0.0, 0.0
	hasLo, hasHi := false, false
	if prev != "" {
		lo, hasLo = decodePos(prev)
	}
	if next != "" {
		hi, hasHi = decodePos(next)
	}
	var pos float64
	switch {
	case hasLo && hasHi:
		pos = (lo + hi) / 2
	case hasLo:
		pos = lo + 1
	case hasHi:
		pos = hi - 1
	}
	seq := atomic.AddUint64(&s.listSeq, 1)
	return fmt.Sprintf("%s-%s-%016x", encodePos(pos), encSeg(s.replica()), seq)
}

func encodeJSONNode(n *jsondoc.Node) []byte {
	return append([]byte{byte(n.Kind)}, n.Raw...)
}

// flattenJSON maps node path → slot value for a whole tree.
func flattenJSON(n *jsondoc.Node) map[string][]byte {
	out := map[string][]byte{}
	if n == nil {
		return out
	}
	var walk func(path string, n *jsondoc.Node)
	walk = func(path string, n *jsondoc.Node) {
		out[path] = encodeJSONNode(n)
		join := func(seg string) string {
			if path == "" {
				return seg
			}
			return path + "/" + seg
		}
		switch n.Kind {
		case jsondoc.Object:
			for name, c := range n.Fields {
				walk(join("f"+encSeg(name)), c)
			}
		case jsondoc.Array:
			for _, e := range n.Elems {
				walk(join("a"+e.Pos), e.Node)
			}
		}
	}
	walk("", n)
	return out
}

// loadJSON reads a document: its root (nil if absent) and the flat slot view it
// was built from (for diffing after a mutation).
func (s *Store) loadJSON(ctx context.Context, db int, key string) (*jsondoc.Node, map[string][]byte, error) {
	base := jsonBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return nil, nil, err
	}
	byPath := map[string]map[string]slot{}
	for _, e := range entries {
		parts := strings.Split(strings.TrimPrefix(e.Key, base), "/")
		if len(parts) < 2 || parts[len(parts)-2] != "_" {
			continue
		}
		replica, derr := decSeg(parts[len(parts)-1])
		sl, serr := decodeSlot(e.Value)
		if derr != nil || serr != nil {
			continue
		}
		path := strings.Join(parts[:len(parts)-2], "/")
		if byPath[path] == nil {
			byPath[path] = map[string]slot{}
		}
		byPath[path][replica] = sl
	}
	live := map[string][]byte{}
	kids := map[string][]string{} // parent path → child segments
	for path, cands := range byPath {
		v, ok := liveValue(cands)
		if !ok || len(v) == 0 {
			continue
		}
		live[path] = v
		if path != "" {
			parent, seg := "", path
			if i := strings.LastIndexByte(path, '/'); i >= 0 {
				parent, seg = path[:i], path[i+1:]
			}
			kids[parent] = append(kids[parent], seg)
		}
	}
	if _, ok := live[""]; !ok {
		return nil, map[string][]byte{}, nil
	}
	flat := map[string][]byte{}
	var build func(path string) *jsondoc.Node
	build = func(path string) *jsondoc.Node {
		v := live[path]
		n := &jsondoc.Node{Kind: jsondoc.Kind(v[0]), Raw: string(v[1:])}
		flat[path] = v
		join := func(seg string) string {
			if path == "" {
				return seg
			}
			return path + "/" + seg
		}
		switch n.Kind {
		case jsondoc.Object:
			n.Raw = ""
			n.Fields = map[string]*jsondoc.Node{}
			for _, seg := range kids[path] {
				if seg[0] != 'f' {
					continue
				}
				if name, err := decSeg(seg[1:]); err == nil {
					n.Fields[name] = build(join(seg))
				}
			}
		case jsondoc.Array:
			n.Raw = ""
			segs := append([]string(nil), kids[path]...)
			sort.Strings(segs)
			for _, seg := range segs {
				if seg[0] == 'a' {
					n.Elems = append(n.Elems, &jsondoc.Elem{Pos: seg[1:], Node: build(join(seg))})
				}
			}
		}
		return n
	}
	return build(""), flat, nil
}

// saveJSONDiff writes the slots that differ between the loaded and mutated
// views: changed/new nodes as present, vanished nodes as deleted (with a fresh
// HLC so they win the max-HLC read).
func (s *Store) saveJSONDiff(ctx context.Context, db int, key string, old, cur map[string][]byte) error {
	for path := range old {
		if _, ok := cur[path]; !ok {
			if err := s.writeSlot(ctx, ds.NewKey(jsonSlotKey(db, key, path, s.replica())), tagDeleted, nil); err != nil {
				return err
			}
		}
	}
	for path, v := range cur {
		if o, ok := old[path]; ok && bytes.Equal(o, v) {
			continue
		}
		if err := s.writeSlot(ctx, ds.NewKey(jsonSlotKey(db, key, path, s.replica())), tagPresent, v); err != nil {
			return err
		}
	}
	return nil
}

func (r jsonRepo) check(ctx context.Context, key string) (exists bool, etimeMS int64, err error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil || !ok {
		return false, 0, err
	}
	if k.Type != core.TypeJSON {
		return false, 0, core.ErrKeyType
	}
	if k.ETime != nil {
		etimeMS = *k.ETime
	}
	return true, etimeMS, nil
}

// View runs fn over a read-only snapshot of the document.
func (r jsonRepo) View(key string, fn func(d *jsondoc.Doc) error) error {
	ctx := bg()
	exists, _, err := r.check(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return core.ErrNotFound
	}
	root, _, err := r.s.loadJSON(ctx, r.db, key)
	if err != nil {
		return err
	}
	return fn(&jsondoc.Doc{Root: root, NewPos: r.s.jsonNewPos})
}

// Update runs fn over the document under the key lock and persists what changed.
// With create, a missing key yields an empty document fn may set a root on;
// otherwise a missing key is core.ErrNotFound. If fn returns an error nothing is
// written.
func (r jsonRepo) Update(key string, create bool, fn func(d *jsondoc.Doc) error) error {
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	exists, etime, err := r.check(ctx, key)
	if err != nil {
		return err
	}
	if !exists && !create {
		return core.ErrNotFound
	}
	root, old, err := r.s.loadJSON(ctx, r.db, key)
	if err != nil {
		return err
	}
	doc := &jsondoc.Doc{Root: root, NewPos: r.s.jsonNewPos}
	if err := fn(doc); err != nil {
		return err
	}
	if err := r.s.saveJSONDiff(ctx, r.db, key, old, flattenJSON(doc.Root)); err != nil {
		return err
	}
	switch {
	case doc.Root == nil && exists:
		return r.s.deleteMeta(ctx, r.db, key)
	case doc.Root != nil && !exists:
		return r.s.writeMeta(ctx, r.db, key, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeJSON, ETimeMS: etime}})
	}
	return nil
}

func (s *Store) jsonAnyLive(ctx context.Context, db int, key string) (bool, error) {
	cands, err := s.readSlots(ctx, jsonBase(db, key)+"_/")
	if err != nil {
		return false, err
	}
	v, ok := liveValue(cands)
	return ok && len(v) > 0, nil
}

// deleteJSON tombstones every live node of a document plus its meta.
func (s *Store) deleteJSON(ctx context.Context, db int, key string) error {
	_, flat, err := s.loadJSON(ctx, db, key)
	if err != nil {
		return err
	}
	if err := s.saveJSONDiff(ctx, db, key, flat, map[string][]byte{}); err != nil {
		return err
	}
	return s.deleteMeta(ctx, db, key)
}

func (s *Store) copyJSON(ctx context.Context, db int, key, newKey string) error {
	root, _, err := s.loadJSON(ctx, db, key)
	if err != nil || root == nil {
		return err
	}
	if err := s.saveJSONDiff(ctx, db, newKey, map[string][]byte{}, flattenJSON(root)); err != nil {
		return err
	}
	return s.writeMeta(ctx, db, newKey, metaEnvelope{KeyMeta: KeyMeta{Type: core.TypeJSON}})
}
