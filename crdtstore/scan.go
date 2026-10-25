package crdtstore

import (
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/match"
)

// defaultScanCount is the COUNT hint used when a client omits it.
const defaultScanCount = 10

// resumeTable is the per-node SCAN cursor resume table (DESIGN §6.9). The wire
// cursor is a numeric token; this table maps token → resume position. It is
// idle-TTL'd so abandoned scans don't leak. Cursors only resolve on the issuing
// node (clients pin a pool to one endpoint); a token presented to a different
// node simply won't be found and the scan restarts.
type resumeTable struct {
	mu      sync.Mutex
	next    uint64
	entries map[uint64]*resumeEntry
	ttlMS   int64
}

type resumeEntry struct {
	db        int
	scopeKey  string // "" for keyspace SCAN; the collection key for H/S/ZSCAN
	partition int    // next partition to resume from (keyspace SCAN)
	last      string // last position consumed (full meta key, or last member)
	expiresMS int64
}

func newResumeTable(ttlMS int64) *resumeTable {
	return &resumeTable{entries: make(map[uint64]*resumeEntry), ttlMS: ttlMS}
}

// alloc stores a resume entry and returns its non-zero token.
func (rt *resumeTable) alloc(e *resumeEntry) uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.gcLocked()
	rt.next++
	if rt.next == 0 {
		rt.next = 1 // 0 is the start/end sentinel, never a live token
	}
	tok := rt.next
	e.expiresMS = nowMS() + rt.ttlMS
	rt.entries[tok] = e
	return tok
}

// get returns the entry for a token (refreshing its TTL) and whether it exists.
func (rt *resumeTable) get(tok uint64) (*resumeEntry, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	e, ok := rt.entries[tok]
	if !ok || e.expiresMS <= nowMS() {
		delete(rt.entries, tok)
		return nil, false
	}
	e.expiresMS = nowMS() + rt.ttlMS
	return e, true
}

func (rt *resumeTable) free(tok uint64) {
	rt.mu.Lock()
	delete(rt.entries, tok)
	rt.mu.Unlock()
}

// gcLocked drops expired entries. Caller holds the lock.
func (rt *resumeTable) gcLocked() {
	now := nowMS()
	for tok, e := range rt.entries {
		if e.expiresMS <= now {
			delete(rt.entries, tok)
		}
	}
}

// scanKeys collects up to ~count live keys of the DB starting from (startP,
// last), applying an optional probe. It cuts only at key boundaries — a key's
// per-replica meta slots are contiguous when sorted, so `lastConsumed` always
// sits at the end of a fully-returned key and the next page (which skips
// k <= lastConsumed) never re-emits or straddles. Returns the collected keys,
// the resume position, and whether the scan is complete.
func (r keyRepo) scanKeys(startP int, last string, count int, keep func(name string) (bool, error)) (keys []string, nextP int, nextLast string, done bool, err error) {
	ctx := bg()
	collected := 0
	curName := ""        // the key currently being consumed
	lastConsumed := last // last meta entry fully consumed
	for p := startP; p < NumBuckets; p++ {
		prefix := dbMetaPrefixes(r.db)[p]
		entries, qerr := r.s.query(ctx, prefix, true)
		if qerr != nil {
			return nil, 0, "", false, qerr
		}
		raw := make([]string, 0, len(entries))
		for _, e := range entries {
			raw = append(raw, e.Key)
		}
		sort.Strings(raw)

		for _, k := range raw {
			if p == startP && last != "" && k <= last {
				continue // returned on a previous page
			}
			name := keyNameFromMeta(k, prefix)
			if name == "" {
				continue
			}
			if name != curName {
				// A new logical key begins. If we've met COUNT, stop here and
				// resume from this key (its entries are all > lastConsumed).
				if collected >= count {
					return keys, p, lastConsumed, false, nil
				}
				curName = name
				ok, kerr := keep(name)
				if kerr != nil {
					return nil, 0, "", false, kerr
				}
				if ok {
					keys = append(keys, name)
					collected++
				}
			}
			lastConsumed = k
		}
	}
	return keys, NumBuckets, "", true, nil
}

// keyNameFromMeta decodes the logical key from a meta key
// "/{P}/m/{db}/{encKey}/{encReplica}".
func keyNameFromMeta(metaKey, prefix string) string {
	rest := strings.TrimPrefix(metaKey, prefix)
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest = rest[:i]
	}
	name, err := decSeg(rest)
	if err != nil {
		return ""
	}
	return name
}

// scanPage pages through one collection's members for HSCAN/SSCAN/ZSCAN
// (DESIGN §6.9, §12.3). names must be sorted ascending; ordering by member
// name (not score/insertion) keeps resume positions stable under concurrent
// writes. The cursor is a token in the same per-node resume table as SCAN,
// holding the last member returned, so every member present for the whole scan
// is returned at least once. COUNT bounds matching members per page; each page
// still reads the whole collection (O(n)), it just bounds the reply. An unknown
// or expired cursor restarts from the beginning, as keyspace SCAN does.
// It returns the indexes into names of this page's matches and the next
// cursor (0 = done).
func (s *Store) scanPage(db int, key string, cursor, count int, pattern string, names []string) ([]int, int) {
	if count <= 0 {
		count = defaultScanCount
	}
	start := 0
	if cursor != 0 {
		if e, ok := s.resume.get(uint64(cursor)); ok && e.scopeKey == key && e.db == db {
			start = sort.SearchStrings(names, e.last)
			if start < len(names) && names[start] == e.last {
				start++
			}
		}
	}
	var page []int
	last := -1
	for i := start; i < len(names); i++ {
		if pattern != "" && pattern != "*" && !match.Match(names[i], pattern) {
			continue
		}
		page = append(page, i)
		last = i
		if len(page) >= count {
			break
		}
	}
	if cursor != 0 {
		s.resume.free(uint64(cursor))
	}
	if last < 0 || len(page) < count || last == len(names)-1 {
		return page, 0 // fewer than COUNT matches left, or nothing after the last
	}
	tok := s.resume.alloc(&resumeEntry{db: db, scopeKey: key, last: names[last]})
	return page, int(tok)
}
