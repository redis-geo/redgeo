// Package engine wires the go-ds-crdt storage substrate: CRDT datastore(s) over
// a Pebble (or in-memory) backing store, a DAG service, and broadcaster(s).
//
// The engine can run a single DAG (NumPartitions <= 1) or route keys to N named
// partition DAGs by their leading /{P}/ segment (DESIGN §5.5), each with its own
// namespace and broadcaster, enabling rolling per-partition rotation. The
// active datastore of each partition is swappable (mutex-guarded) so compaction
// can rotate it to a fresh genesis without tombstones.
package engine

import (
	"context"
	"fmt"
	"os"
	"strconv"
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

// Namespace is the base CRDT namespace all redgeo keys live under.
var Namespace = ds.NewKey("/redgeo")

// BroadcasterFactory returns the broadcaster for partition p. Used in multi-DAG
// mode so each partition gossips on its own topic.
type BroadcasterFactory func(p int) crdt.Broadcaster

// Config configures an Engine.
type Config struct {
	// PebbleDir is the on-disk backing store directory. Empty = in-memory.
	PebbleDir string
	// ReplicaID is this node's stable identity (DESIGN §6.7). Required.
	ReplicaID string
	// PutHook / DeleteHook fire on prevalent add/remove (local or remote).
	PutHook    func(k ds.Key, v []byte)
	DeleteHook func(k ds.Key)
	// Broadcaster is the single-DAG broadcaster (NumPartitions <= 1). nil =
	// no-op (single node).
	Broadcaster crdt.Broadcaster
	// BroadcasterFactory supplies a per-partition broadcaster (NumPartitions >
	// 1). nil = no-op for every partition.
	BroadcasterFactory BroadcasterFactory
	// DAGService exchanges DAG blocks with peers (shared across partitions).
	// nil = a local offline service.
	DAGService ipld.DAGService
	// RebroadcastInterval controls anti-entropy head re-publishing. 0 = default.
	RebroadcastInterval time.Duration
	// NumPartitions is the number of named partition DAGs. 0 or 1 = a single
	// DAG (the leading /{P}/ segment is still in keys but all route to one DAG).
	// >1 routes key bucket P to DAG (P mod NumPartitions).
	NumPartitions int
}

// partition is one named DAG: a swappable CRDT datastore over the shared
// backing under its own namespace, with its own rotation generation.
type partition struct {
	mu     sync.RWMutex
	crdt   *crdt.Datastore
	idx    int
	nsBase string // e.g. "/redgeo/p07"
	gen    int
	bcast  crdt.Broadcaster
}

// Engine is the storage substrate handed to the crdtstore backend.
type Engine struct {
	parts    []*partition
	numParts int
	backing  ds.Datastore // shared by all partitions

	replica string
	clock   *hlc.Clock

	dagSvc ipld.DAGService
	opts   *crdt.Options
}

// noopBroadcaster never sends and blocks on receive (single node, no peers).
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
	numParts := cfg.NumPartitions
	if numParts < 1 {
		numParts = 1
	}

	backing, err := makeBacking(cfg.PebbleDir)
	if err != nil {
		return nil, err
	}

	dagSvc := cfg.DAGService
	if dagSvc == nil {
		bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
		dagSvc = merkledag.NewDAGService(bsrv.New(bs, offline.Exchange(bs)))
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logging.Logger("redgeo/crdt")
	opts.NumWorkers = 1 // DESIGN §11
	opts.PutHook = cfg.PutHook
	opts.DeleteHook = cfg.DeleteHook
	if cfg.RebroadcastInterval > 0 {
		opts.RebroadcastInterval = cfg.RebroadcastInterval
	}

	e := &Engine{
		numParts: numParts,
		backing:  backing,
		replica:  cfg.ReplicaID,
		clock:    hlc.New(),
		dagSvc:   dagSvc,
		opts:     opts,
	}

	bcastFor := func(p int) crdt.Broadcaster {
		if numParts == 1 {
			if cfg.Broadcaster != nil {
				return cfg.Broadcaster
			}
			return noopBroadcaster{ctx: ctx}
		}
		if cfg.BroadcasterFactory != nil {
			return cfg.BroadcasterFactory(p)
		}
		return noopBroadcaster{ctx: ctx}
	}

	e.parts = make([]*partition, numParts)
	for i := 0; i < numParts; i++ {
		nsBase := Namespace.String()
		if numParts > 1 {
			nsBase = fmt.Sprintf("%s/p%02x", Namespace.String(), i)
		}
		p := &partition{idx: i, nsBase: nsBase, gen: 0, bcast: bcastFor(i)}
		store, err := crdt.New(backing, p.namespace(), dagSvc, p.bcast, opts)
		if err != nil {
			_ = e.Close()
			return nil, fmt.Errorf("engine: crdt.New (partition %d): %w", i, err)
		}
		p.crdt = store
		e.parts[i] = p
	}
	return e, nil
}

// namespace always includes a /g{gen} segment so no generation's namespace is a
// prefix of another's — purging an old generation must not match the new one.
// The segment is transparent to the crdtstore layer (its keys are stored
// relative to the datastore namespace).
func (p *partition) namespace() ds.Key {
	return ds.NewKey(fmt.Sprintf("%s/g%d", p.nsBase, p.gen))
}

// makeBacking builds the shared backing datastore.
func makeBacking(pebbleDir string) (ds.Datastore, error) {
	if pebbleDir == "" {
		return dssync.MutexWrap(ds.NewMapDatastore()), nil
	}
	if err := os.MkdirAll(pebbleDir, 0o700); err != nil {
		return nil, fmt.Errorf("engine: mkdir backing dir: %w", err)
	}
	pb, err := pebbleds.NewDatastore(pebbleDir)
	if err != nil {
		return nil, fmt.Errorf("engine: open pebble: %w", err)
	}
	return pb, nil
}

// Replica returns this node's replica ID.
func (e *Engine) Replica() string { return e.replica }

// Clock returns this node's hybrid logical clock.
func (e *Engine) Clock() *hlc.Clock { return e.clock }

// NumPartitions returns the number of named partition DAGs.
func (e *Engine) NumPartitions() int { return e.numParts }

// partForKey routes a key/prefix string to its partition by the leading /{P}/
// bucket segment. Single-DAG mode always returns partition 0.
func (e *Engine) partForKey(key string) *partition {
	return e.parts[e.partIdx(key)]
}

func (e *Engine) partIdx(key string) int {
	if e.numParts == 1 {
		return 0
	}
	return parseLeadingBucket(key) % e.numParts
}

// parseLeadingBucket reads the 2-hex partition from "/{P}/...". Returns 0 if
// absent (defensive; all redgeo keys carry the segment).
func parseLeadingBucket(key string) int {
	if len(key) >= 3 && key[0] == '/' {
		if n, err := strconv.ParseUint(key[1:3], 16, 16); err == nil {
			return int(n)
		}
	}
	return 0
}

// active returns a partition's datastore under a read lock held for the call's
// duration via the returned release func.
func (p *partition) active() (*crdt.Datastore, func()) {
	p.mu.RLock()
	return p.crdt, p.mu.RUnlock
}

// Get reads the value at key, or ds.ErrNotFound.
func (e *Engine) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	c, done := e.partForKey(key.String()).active()
	defer done()
	return c.Get(ctx, key)
}

// Has reports whether key exists.
func (e *Engine) Has(ctx context.Context, key ds.Key) (bool, error) {
	c, done := e.partForKey(key.String()).active()
	defer done()
	return c.Has(ctx, key)
}

// Put writes value at key (a blind CRDT add).
func (e *Engine) Put(ctx context.Context, key ds.Key, value []byte) error {
	c, done := e.partForKey(key.String()).active()
	defer done()
	return c.Put(ctx, key, value)
}

// Delete tombstones key.
func (e *Engine) Delete(ctx context.Context, key ds.Key) error {
	c, done := e.partForKey(key.String()).active()
	defer done()
	return c.Delete(ctx, key)
}

// QueryPrefix returns all results whose key has the given prefix. Every redgeo
// prefix begins with a concrete /{P}/ segment, so it routes to one partition.
func (e *Engine) QueryPrefix(ctx context.Context, prefix string, keysOnly bool) ([]query.Entry, error) {
	c, done := e.partForKey(prefix).active()
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

// Batch returns a batch that fans writes out to the right partition (DESIGN
// §6.10). Writes within one partition land atomically; a batch spanning
// partitions is NOT atomic across them (the documented cross-partition MULTI
// trade-off, §5.5).
func (e *Engine) Batch(ctx context.Context) (ds.Batch, error) {
	return &multiBatch{e: e, subs: map[int]ds.Batch{}}, nil
}

// Sync flushes the named prefix's partition to the backing store.
func (e *Engine) Sync(ctx context.Context, prefix ds.Key) error {
	c, done := e.partForKey(prefix.String()).active()
	defer done()
	return c.Sync(ctx, prefix)
}

// Stats aggregates replication internals across partitions (DESIGN §6.11).
func (e *Engine) Stats(ctx context.Context) Stats {
	var s Stats
	for _, p := range e.parts {
		c, done := p.active()
		ps := c.InternalStats(ctx)
		done()
		s.Heads += len(ps.Heads)
		if ps.MaxHeight > s.MaxHeight {
			s.MaxHeight = ps.MaxHeight
		}
		s.QueuedJobs += ps.QueuedJobs
	}
	return s
}

// Stats reports CRDT replication internals for INFO.
type Stats struct {
	Heads      int
	MaxHeight  uint64
	QueuedJobs int
}

// RotatePartition performs global-purge compaction of one partition by DAG
// rotation (DESIGN §5.5): snapshot the partition's live state, seed a fresh
// genesis namespace with only that state, swap it in, and purge the old
// namespace from the backing. Returns the live entry count carried forward.
//
// Single-node correct; in a cluster, partitions must be rotated in coordination
// (gated on the causal-stability watermark) or an un-rotated peer would
// resurrect tombstoned keys.
func (e *Engine) RotatePartition(ctx context.Context, idx int) (int, error) {
	p := e.parts[idx]

	p.mu.RLock()
	cur := p.crdt
	oldNS := p.namespace().String()
	p.mu.RUnlock()

	// Snapshot live state of this partition.
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

	// Build the fresh-generation datastore and reseed.
	p.mu.Lock()
	newGen := p.gen + 1
	newNSBase := p.nsBase
	p.mu.Unlock()
	newNS := ds.NewKey(fmt.Sprintf("%s/g%d", newNSBase, newGen))
	newCRDT, err := crdt.New(e.backing, newNS, e.dagSvc, p.bcast, e.opts)
	if err != nil {
		return 0, fmt.Errorf("engine: rotate partition %d: %w", idx, err)
	}
	batch, err := newCRDT.Batch(ctx)
	if err != nil {
		return 0, err
	}
	for _, kv := range live {
		if err := batch.Put(ctx, ds.NewKey(kv.k), kv.v); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(ctx); err != nil {
		return 0, err
	}

	// Swap.
	p.mu.Lock()
	old := p.crdt
	p.crdt = newCRDT
	p.gen = newGen
	p.mu.Unlock()

	// Drop the old DAG: close it and purge its namespace from the backing.
	_ = old.Close()
	_ = e.purgeNamespace(ctx, oldNS)
	return len(live), nil
}

// Rotate compacts every partition (a full global purge).
func (e *Engine) Rotate(ctx context.Context) (int, error) {
	total := 0
	for i := range e.parts {
		n, err := e.RotatePartition(ctx, i)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// purgeNamespace deletes every backing key under an old DAG namespace.
func (e *Engine) purgeNamespace(ctx context.Context, ns string) error {
	res, err := e.backing.Query(ctx, query.Query{Prefix: ns, KeysOnly: true})
	if err != nil {
		return err
	}
	defer res.Close()
	for r := range res.Next() {
		if r.Error != nil {
			return r.Error
		}
		_ = e.backing.Delete(ctx, ds.NewKey(r.Key))
	}
	return nil
}

// Close shuts down all partition datastores and the backing store.
func (e *Engine) Close() error {
	var err error
	for _, p := range e.parts {
		if p == nil || p.crdt == nil {
			continue
		}
		p.mu.Lock()
		if cerr := p.crdt.Close(); cerr != nil && err == nil {
			err = cerr
		}
		p.mu.Unlock()
	}
	if c, ok := e.backing.(interface{ Close() error }); ok {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// multiBatch fans Put/Delete out to per-partition sub-batches.
type multiBatch struct {
	e    *Engine
	subs map[int]ds.Batch
}

func (b *multiBatch) sub(ctx context.Context, key string) (ds.Batch, error) {
	idx := b.e.partIdx(key)
	if sb, ok := b.subs[idx]; ok {
		return sb, nil
	}
	c, done := b.e.parts[idx].active()
	sb, err := c.Batch(ctx)
	done()
	if err != nil {
		return nil, err
	}
	b.subs[idx] = sb
	return sb, nil
}

func (b *multiBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	sb, err := b.sub(ctx, key.String())
	if err != nil {
		return err
	}
	return sb.Put(ctx, key, value)
}

func (b *multiBatch) Delete(ctx context.Context, key ds.Key) error {
	sb, err := b.sub(ctx, key.String())
	if err != nil {
		return err
	}
	return sb.Delete(ctx, key)
}

func (b *multiBatch) Commit(ctx context.Context) error {
	for _, sb := range b.subs {
		if err := sb.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
