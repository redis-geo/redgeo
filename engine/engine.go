// Package engine wires the go-ds-crdt storage substrate: a CRDT datastore over
// a Pebble (or in-memory) backing store, a DAG service, and a broadcaster.
//
// Phase 0 is single-node: the broadcaster is a no-op and the DAG service is a
// local offline block service. libp2p + gossipsub replication arrives in
// Phase 8. All keys live in one CRDT namespace / one named DAG for now; the
// leading /{P}/ partition segment in keys (DESIGN §5.5) is reserved for the
// per-partition named-DAG rotation introduced in Phase 9.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bsrv "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	crdt "github.com/ipfs/go-ds-crdt"
	pebbleds "github.com/ipfs/go-ds-pebble"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"

	"github.com/redis-geo/redgeo/hlc"
)

// Namespace is the single CRDT namespace all redgeo keys live under.
var Namespace = ds.NewKey("/redgeo")

// Config configures an Engine.
type Config struct {
	// PebbleDir is the on-disk backing store directory. Empty = in-memory
	// (tests / ephemeral nodes).
	PebbleDir string
	// ReplicaID is this node's stable identity, used to own its HLC slots
	// (DESIGN §6.7). Empty is rejected; the caller persists/derives it.
	ReplicaID string
	// PutHook / DeleteHook fire on prevalent add/remove (local or remote) and
	// drive keyspace notifications and local indexes. Optional.
	PutHook    func(k ds.Key, v []byte)
	DeleteHook func(k ds.Key)
	// Broadcaster propagates deltas to peer replicas. nil = single-node no-op.
	// In-process clusters use a memory network (NewMemNetwork); production uses
	// the libp2p gossipsub broadcaster (NewCluster).
	Broadcaster crdt.Broadcaster
	// DAGService exchanges DAG blocks with peers. nil = a local offline service
	// (single node). In-process clusters share one service so blocks written by
	// any replica are visible to all (simulating perfect block exchange).
	DAGService ipld.DAGService
	// RebroadcastInterval controls how often heads are re-published for
	// anti-entropy (lagging/healed replicas catch up). 0 = go-ds-crdt's default
	// (1m). Lower it for faster convergence after a partition heals.
	RebroadcastInterval time.Duration
}

// Engine is the storage substrate handed to the crdtstore backend. The active
// CRDT datastore is swappable (guarded by mu) so global-purge compaction can
// rotate to a fresh genesis DAG (DESIGN §5.5).
type Engine struct {
	mu      sync.RWMutex
	crdt    *crdt.Datastore
	backing ds.Datastore
	gen     int // rotation generation (for the on-disk backing path)

	replica string
	clock   *hlc.Clock

	// Retained dependencies for rebuilding the datastore on rotation.
	dagSvc    ipld.DAGService
	bcast     crdt.Broadcaster
	opts      *crdt.Options
	pebbleDir string
}

// noopBroadcaster is a Broadcaster that never sends and blocks on receive —
// correct for a single node with no peers (Phase 0).
type noopBroadcaster struct{ ctx context.Context }

func (n noopBroadcaster) Broadcast(context.Context, []byte) error { return nil }
func (n noopBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case <-n.ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	}
}

// New constructs an Engine from cfg.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.ReplicaID == "" {
		return nil, fmt.Errorf("engine: ReplicaID is required")
	}

	gen := 0
	backing, err := makeBacking(cfg.PebbleDir, gen)
	if err != nil {
		return nil, err
	}

	// DAG service: injected (shared cluster service) or a local offline one.
	dagSvc := cfg.DAGService
	if dagSvc == nil {
		bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
		dagSvc = merkledag.NewDAGService(bsrv.New(bs, offline.Exchange(bs)))
	}

	// Broadcaster: injected (cluster) or a single-node no-op.
	var bcast crdt.Broadcaster = noopBroadcaster{ctx: ctx}
	if cfg.Broadcaster != nil {
		bcast = cfg.Broadcaster
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logging.Logger("redgeo/crdt")
	opts.NumWorkers = 1 // DESIGN §11: low per-partition worker count
	opts.PutHook = cfg.PutHook
	opts.DeleteHook = cfg.DeleteHook
	if cfg.RebroadcastInterval > 0 {
		opts.RebroadcastInterval = cfg.RebroadcastInterval
	}

	store, err := crdt.New(backing, Namespace, dagSvc, bcast, opts)
	if err != nil {
		if c, ok := backing.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return nil, fmt.Errorf("engine: crdt.New: %w", err)
	}

	return &Engine{
		crdt:      store,
		backing:   backing,
		gen:       gen,
		replica:   cfg.ReplicaID,
		clock:     hlc.New(),
		dagSvc:    dagSvc,
		bcast:     bcast,
		opts:      opts,
		pebbleDir: cfg.PebbleDir,
	}, nil
}

// makeBacking builds a backing datastore for a rotation generation. In-memory
// when pebbleDir is empty; otherwise a per-generation subdirectory so a rotation
// writes to fresh storage and the old generation can be dropped.
func makeBacking(pebbleDir string, gen int) (ds.Datastore, error) {
	if pebbleDir == "" {
		return dssync.MutexWrap(ds.NewMapDatastore()), nil
	}
	dir := genDir(pebbleDir, gen)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("engine: mkdir backing dir: %w", err)
	}
	pb, err := pebbleds.NewDatastore(dir)
	if err != nil {
		return nil, fmt.Errorf("engine: open pebble: %w", err)
	}
	return pb, nil
}

func genDir(pebbleDir string, gen int) string {
	return filepath.Join(pebbleDir, fmt.Sprintf("gen%d", gen))
}

// Replica returns this node's replica ID.
func (e *Engine) Replica() string { return e.replica }

// Clock returns this node's hybrid logical clock.
func (e *Engine) Clock() *hlc.Clock { return e.clock }

// active returns the current datastore under a read lock held for the call's
// duration via the returned release func. Rotation takes the write lock, so it
// waits for in-flight ops to finish and new ops see the rotated datastore.
func (e *Engine) active() (*crdt.Datastore, func()) {
	e.mu.RLock()
	return e.crdt, e.mu.RUnlock
}

// Get reads the value at key, or ds.ErrNotFound.
func (e *Engine) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	c, done := e.active()
	defer done()
	return c.Get(ctx, key)
}

// Has reports whether key exists.
func (e *Engine) Has(ctx context.Context, key ds.Key) (bool, error) {
	c, done := e.active()
	defer done()
	return c.Has(ctx, key)
}

// Put writes value at key (a blind CRDT add).
func (e *Engine) Put(ctx context.Context, key ds.Key, value []byte) error {
	c, done := e.active()
	defer done()
	return c.Put(ctx, key, value)
}

// Delete tombstones key.
func (e *Engine) Delete(ctx context.Context, key ds.Key) error {
	c, done := e.active()
	defer done()
	return c.Delete(ctx, key)
}

// QueryPrefix returns all (key,value) results whose key has the given prefix.
// Ordering is by key (the natural prefix-scan order).
func (e *Engine) QueryPrefix(ctx context.Context, prefix string, keysOnly bool) ([]query.Entry, error) {
	c, done := e.active()
	defer done()
	res, err := c.Query(ctx, query.Query{Prefix: prefix, KeysOnly: keysOnly})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var out []query.Entry
	for r := range res.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		out = append(out, r.Entry)
	}
	return out, nil
}

// Batch returns a CRDT batch accumulating into one atomic delta (DESIGN §6.10).
func (e *Engine) Batch(ctx context.Context) (ds.Batch, error) {
	c, done := e.active()
	defer done()
	return c.Batch(ctx)
}

// Sync flushes the named prefix to the backing store (Pebble is async).
func (e *Engine) Sync(ctx context.Context, prefix ds.Key) error {
	c, done := e.active()
	defer done()
	return c.Sync(ctx, prefix)
}

// Rotate performs global-purge compaction by DAG rotation (DESIGN §5.5 v1):
// snapshot the live key→value state, seed a fresh genesis datastore with only
// that state (no tombstones), atomically swap it in, then drop the old one.
//
// Single-node correct. In a cluster this must be coordinated — every replica
// rotates together in a maintenance window, gated on the causal-stability
// watermark — otherwise a replica that hasn't rotated would merge the fresh
// genesis with its old DAG and resurrect tombstoned keys. Returns the number of
// live entries carried forward.
func (e *Engine) Rotate(ctx context.Context) (int, error) {
	// Snapshot live state from the current datastore.
	e.mu.RLock()
	cur := e.crdt
	e.mu.RUnlock()
	res, err := cur.Query(ctx, query.Query{})
	if err != nil {
		return 0, err
	}
	type kv struct {
		k string
		v []byte
	}
	var live []kv
	for r := range res.Next() {
		if r.Error != nil {
			res.Close()
			return 0, r.Error
		}
		live = append(live, kv{r.Key, r.Value})
	}
	res.Close()

	// Build a fresh datastore over new backing and reseed the snapshot.
	newGen := e.gen + 1
	newBacking, err := makeBacking(e.pebbleDir, newGen)
	if err != nil {
		return 0, err
	}
	newCRDT, err := crdt.New(newBacking, Namespace, e.dagSvc, e.bcast, e.opts)
	if err != nil {
		if c, ok := newBacking.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return 0, fmt.Errorf("engine: rotate: new datastore: %w", err)
	}
	batch, err := newCRDT.Batch(ctx)
	if err != nil {
		return 0, err
	}
	for _, e := range live {
		if err := batch.Put(ctx, ds.NewKey(e.k), e.v); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(ctx); err != nil {
		return 0, err
	}

	// Swap atomically; old ops drain on the write lock.
	e.mu.Lock()
	old := e.crdt
	oldBacking := e.backing
	oldGen := e.gen
	e.crdt = newCRDT
	e.backing = newBacking
	e.gen = newGen
	e.mu.Unlock()

	// Drop the old generation.
	_ = old.Close()
	if c, ok := oldBacking.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	if e.pebbleDir != "" {
		_ = os.RemoveAll(genDir(e.pebbleDir, oldGen))
	}
	return len(live), nil
}

// Stats reports CRDT replication internals for INFO (DESIGN §6.11).
type Stats struct {
	Heads      int
	MaxHeight  uint64
	QueuedJobs int
}

// Stats returns current replication statistics.
func (e *Engine) Stats(ctx context.Context) Stats {
	c, done := e.active()
	defer done()
	s := c.InternalStats(ctx)
	return Stats{Heads: len(s.Heads), MaxHeight: s.MaxHeight, QueuedJobs: s.QueuedJobs}
}

// Close shuts down the CRDT datastore and backing store.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.crdt.Close()
	if c, ok := e.backing.(interface{ Close() error }); ok {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
