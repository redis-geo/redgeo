package crdtstore

import (
	"context"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

// txn is a write overlay used by MULTI/EXEC (DESIGN §6.10). Queued commands run
// sequentially against a txn-bound Store: their writes accumulate into one
// go-ds-crdt Batch (committed as a single atomic delta) AND into an in-memory
// overlay so later commands in the same EXEC observe earlier ones
// (read-your-writes). Without the overlay, batched writes would be invisible to
// reads until commit and sequential transactions would compute wrong results.
type txn struct {
	batch ds.Batch
	puts  map[string][]byte
	dels  map[string]struct{}
}

// BeginTxn returns a Store bound to a fresh transaction plus a commit func that
// flushes the batch as one atomic delta. The bound Store shares the engine,
// locks, and resume table; only its read/write path is overlaid. Used by
// MULTI/EXEC (DESIGN §6.10).
func (s *Store) BeginTxn(ctx context.Context) (*Store, func() error, error) {
	b, err := s.eng.Batch(ctx)
	if err != nil {
		return nil, nil, err
	}
	t := &txn{batch: b, puts: map[string][]byte{}, dels: map[string]struct{}{}}
	cp := *s
	cp.txn = t
	commit := func() error { return t.batch.Commit(ctx) }
	return &cp, commit, nil
}

// ---- txn-aware backend wrappers (used everywhere instead of s.eng.*) ----

func (s *Store) put(ctx context.Context, key ds.Key, val []byte) error {
	if s.txn == nil {
		return s.eng.Put(ctx, key, val)
	}
	k := key.String()
	if err := s.txn.batch.Put(ctx, key, val); err != nil {
		return err
	}
	s.txn.puts[k] = val
	delete(s.txn.dels, k)
	return nil
}

func (s *Store) del(ctx context.Context, key ds.Key) error {
	if s.txn == nil {
		return s.eng.Delete(ctx, key)
	}
	k := key.String()
	if err := s.txn.batch.Delete(ctx, key); err != nil {
		return err
	}
	s.txn.dels[k] = struct{}{}
	delete(s.txn.puts, k)
	return nil
}

func (s *Store) get(ctx context.Context, key ds.Key) ([]byte, error) {
	if s.txn == nil {
		return s.eng.Get(ctx, key)
	}
	k := key.String()
	if _, deleted := s.txn.dels[k]; deleted {
		return nil, ds.ErrNotFound
	}
	if v, ok := s.txn.puts[k]; ok {
		return v, nil
	}
	return s.eng.Get(ctx, key)
}

func (s *Store) has(ctx context.Context, key ds.Key) (bool, error) {
	if s.txn == nil {
		return s.eng.Has(ctx, key)
	}
	k := key.String()
	if _, deleted := s.txn.dels[k]; deleted {
		return false, nil
	}
	if _, ok := s.txn.puts[k]; ok {
		return true, nil
	}
	return s.eng.Has(ctx, key)
}

// query returns prefix-scan results merged with the txn overlay: base entries
// minus deletes, plus overlay puts under the prefix.
func (s *Store) query(ctx context.Context, prefix string, keysOnly bool) ([]query.Entry, error) {
	base, err := s.eng.QueryPrefix(ctx, prefix, keysOnly)
	if err != nil {
		return nil, err
	}
	if s.txn == nil {
		return base, nil
	}
	out := make([]query.Entry, 0, len(base))
	inBase := make(map[string]struct{}, len(base))
	for _, e := range base {
		inBase[e.Key] = struct{}{}
		if _, deleted := s.txn.dels[e.Key]; deleted {
			continue
		}
		if v, ok := s.txn.puts[e.Key]; ok {
			out = append(out, entry(e.Key, v, keysOnly))
			continue
		}
		out = append(out, e)
	}
	for k, v := range s.txn.puts {
		if _, ok := inBase[k]; ok {
			continue
		}
		if _, deleted := s.txn.dels[k]; deleted {
			continue
		}
		if strings.HasPrefix(k, prefix) {
			out = append(out, entry(k, v, keysOnly))
		}
	}
	return out, nil
}

func entry(key string, val []byte, keysOnly bool) query.Entry {
	if keysOnly {
		return query.Entry{Key: key}
	}
	return query.Entry{Key: key, Value: val}
}
