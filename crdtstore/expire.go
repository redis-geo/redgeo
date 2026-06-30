package crdtstore

import (
	"context"
	"strings"
	"time"
)

// SweepExpired scans every DB's meta space for keys whose absolute expiry has
// passed and issues CRDT deletes (DESIGN §6.8 active sweeper). Lazy filtering
// in probe already hides expired keys from reads; the sweeper reclaims them so
// tombstones and element keys don't linger. The delete is itself a CRDT op, so
// it propagates to other replicas. Returns the number of keys swept.
//
// Race (documented, §6.8): if replica A sweeps+deletes while B refreshed the
// TTL, B's higher-HLC meta write wins and the key survives — acceptable.
func (s *Store) SweepExpired(ctx context.Context) (int, error) {
	now := nowMS()
	swept := 0
	for db := 0; db < numDBs; db++ {
		for _, prefix := range dbMetaPrefixes(db) {
			entries, err := s.query(ctx, prefix, false)
			if err != nil {
				return swept, err
			}
			// Group meta slots by key to evaluate the winning expiry.
			byKey := make(map[string]map[string]slot)
			for _, e := range entries {
				rest := strings.TrimPrefix(e.Key, prefix) // {encKey}/{encReplica}
				i := strings.IndexByte(rest, '/')
				if i < 0 {
					continue
				}
				key, derr := decSeg(rest[:i])
				if derr != nil {
					continue
				}
				replica, derr := decSeg(rest[i+1:])
				if derr != nil {
					continue
				}
				sl, derr := decodeSlot(e.Value)
				if derr != nil {
					continue
				}
				if byKey[key] == nil {
					byKey[key] = make(map[string]slot)
				}
				byKey[key][replica] = sl
			}
			for key, cands := range byKey {
				best, _, found := winner(cands)
				if !found || best.tag == tagDeleted {
					continue
				}
				m, ok := decodeMeta(best.value)
				if !ok || m.ETimeMS == 0 || m.ETimeMS > now {
					continue
				}
				unlock := s.locks.Lock(lockKey(db, key))
				if err := s.deleteKey(ctx, db, key, m.Type); err != nil {
					unlock()
					return swept, err
				}
				unlock()
				swept++
			}
		}
	}
	return swept, nil
}

// numDBs mirrors the server's logical DB count (DESIGN §6.11).
const numDBs = 16

// Sweeper periodically runs SweepExpired in the background.
type Sweeper struct {
	store    *Store
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

// NewSweeper creates a sweeper with the given tick interval.
func NewSweeper(store *Store, interval time.Duration) *Sweeper {
	return &Sweeper{
		store:    store,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start runs the sweep loop until Stop is called.
func (sw *Sweeper) Start(ctx context.Context) {
	go func() {
		defer close(sw.done)
		t := time.NewTicker(sw.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sw.stop:
				return
			case <-t.C:
				_, _ = sw.store.SweepExpired(ctx)
			}
		}
	}()
}

// Stop halts the sweep loop and waits for it to exit.
func (sw *Sweeper) Stop() {
	close(sw.stop)
	<-sw.done
}
