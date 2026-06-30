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

// Engine is the storage substrate handed to the crdtstore backend.
type Engine struct {
	crdt    *crdt.Datastore
	backing ds.Datastore
	replica string
	clock   *hlc.Clock
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

	var backing ds.Datastore
	if cfg.PebbleDir == "" {
		backing = dssync.MutexWrap(ds.NewMapDatastore())
	} else {
		if err := os.MkdirAll(cfg.PebbleDir, 0o700); err != nil {
			return nil, fmt.Errorf("engine: mkdir pebble dir: %w", err)
		}
		pb, err := pebbleds.NewDatastore(cfg.PebbleDir)
		if err != nil {
			return nil, fmt.Errorf("engine: open pebble: %w", err)
		}
		backing = pb
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
		crdt:    store,
		backing: backing,
		replica: cfg.ReplicaID,
		clock:   hlc.New(),
	}, nil
}

// Replica returns this node's replica ID.
func (e *Engine) Replica() string { return e.replica }

// Clock returns this node's hybrid logical clock.
func (e *Engine) Clock() *hlc.Clock { return e.clock }

// Get reads the value at key, or ds.ErrNotFound.
func (e *Engine) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	return e.crdt.Get(ctx, key)
}

// Has reports whether key exists.
func (e *Engine) Has(ctx context.Context, key ds.Key) (bool, error) {
	return e.crdt.Has(ctx, key)
}

// Put writes value at key (a blind CRDT add).
func (e *Engine) Put(ctx context.Context, key ds.Key, value []byte) error {
	return e.crdt.Put(ctx, key, value)
}

// Delete tombstones key.
func (e *Engine) Delete(ctx context.Context, key ds.Key) error {
	return e.crdt.Delete(ctx, key)
}

// QueryPrefix returns all (key,value) results whose key has the given prefix.
// Ordering is by key (the natural prefix-scan order).
func (e *Engine) QueryPrefix(ctx context.Context, prefix string, keysOnly bool) ([]query.Entry, error) {
	res, err := e.crdt.Query(ctx, query.Query{Prefix: prefix, KeysOnly: keysOnly})
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
	return e.crdt.Batch(ctx)
}

// Sync flushes the named prefix to the backing store (Pebble is async).
func (e *Engine) Sync(ctx context.Context, prefix ds.Key) error {
	return e.crdt.Sync(ctx, prefix)
}

// Stats reports CRDT replication internals for INFO (DESIGN §6.11).
type Stats struct {
	Heads      int
	MaxHeight  uint64
	QueuedJobs int
}

// Stats returns current replication statistics.
func (e *Engine) Stats(ctx context.Context) Stats {
	s := e.crdt.InternalStats(ctx)
	return Stats{Heads: len(s.Heads), MaxHeight: s.MaxHeight, QueuedJobs: s.QueuedJobs}
}

// Close shuts down the CRDT datastore and backing store.
func (e *Engine) Close() error {
	err := e.crdt.Close()
	if c, ok := e.backing.(interface{ Close() error }); ok {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
