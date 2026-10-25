package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	crdt "github.com/ipfs/go-ds-crdt"
)

// Generation rotation (DESIGN §5.5, §12.2): compaction swaps a partition's CRDT
// datastore for a fresh generation seeded with only the live state, then purges
// the old generation and its tombstones. The primitives below are what the
// cluster protocol (package rotation) composes; RotatePartition composes them
// for a single node.
//
//	Freeze → Stage(seed) → Activate → [WaitSeed] → Commit      (or Rollback)
//
// A partition's generation, and a pending previous generation, are persisted
// outside the CRDT namespace so a restart resumes the right generation (or
// notices an unfinished rotation).

// metaPrefix holds engine metadata. It deliberately does not share a prefix
// with Namespace ("/redgeo"), so CRDT-namespace queries and purges never see it.
const metaPrefix = "/_redgeo_meta"

func genKey(idx int) ds.Key  { return ds.NewKey(fmt.Sprintf("%s/gen/%02x", metaPrefix, idx)) }
func prevKey(idx int) ds.Key { return ds.NewKey(fmt.Sprintf("%s/prev/%02x", metaPrefix, idx)) }

// MetaGet / MetaPut / MetaDelete store small node-local values (e.g. cluster
// membership) beside the CRDT data, never replicated.
func (e *Engine) MetaGet(ctx context.Context, name string) ([]byte, error) {
	return e.backing.Get(ctx, ds.NewKey(metaPrefix+"/kv/"+name))
}
func (e *Engine) MetaPut(ctx context.Context, name string, v []byte) error {
	return e.backing.Put(ctx, ds.NewKey(metaPrefix+"/kv/"+name), v)
}
func (e *Engine) MetaDelete(ctx context.Context, name string) error {
	return e.backing.Delete(ctx, ds.NewKey(metaPrefix+"/kv/"+name))
}

// restoreGen loads a partition's persisted generation, and — if a rotation was
// in flight when the node stopped — leaves the partition frozen and marked
// in-doubt for the cluster protocol to resolve.
func (e *Engine) restoreGen(ctx context.Context, p *partition) {
	if b, err := e.backing.Get(ctx, genKey(p.idx)); err == nil {
		if n, err := strconv.Atoi(string(b)); err == nil {
			p.gen = n
		}
	}
	if b, err := e.backing.Get(ctx, prevKey(p.idx)); err == nil {
		if n, err := strconv.Atoi(string(b)); err == nil {
			p.hasPrev, p.prevGen = true, n
			p.gate.set(true, false)
		}
	}
}

func (e *Engine) persistGen(ctx context.Context, p *partition) error {
	if err := e.backing.Put(ctx, genKey(p.idx), []byte(strconv.Itoa(p.gen))); err != nil {
		return err
	}
	if p.hasPrev {
		return e.backing.Put(ctx, prevKey(p.idx), []byte(strconv.Itoa(p.prevGen)))
	}
	return e.backing.Delete(ctx, prevKey(p.idx))
}

func (p *partition) nsFor(gen int) ds.Key { return ds.NewKey(fmt.Sprintf("%s/g%d", p.nsBase, gen)) }

// Generation reports a partition's active generation.
func (e *Engine) Generation(idx int) int {
	p := e.parts[idx]
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.gen
}

// InDoubt reports whether a rotation of the partition is staged or activated
// but not yet committed or rolled back.
func (e *Engine) InDoubt(idx int) bool {
	p := e.parts[idx]
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hasPrev || p.staged != nil
}

// Freeze stops new writes to the partition and waits for in-flight ones.
func (e *Engine) Freeze(ctx context.Context, idx int) error { return e.parts[idx].gate.freeze(ctx) }

// Unfreeze lets writes (and reads) through again.
func (e *Engine) Unfreeze(idx int) { e.parts[idx].gate.set(false, false) }

// Heads lists the CIDs of the partition's current DAG heads, sorted.
func (e *Engine) Heads(ctx context.Context, idx int) []string {
	p := e.parts[idx]
	c, done := p.active()
	defer done()
	heads := c.InternalStats(ctx).Heads
	out := make([]string, len(heads))
	for i, h := range heads {
		out[i] = h.Cid.String()
	}
	sort.Strings(out)
	return out
}

// Missing returns those of the given block CIDs this replica has not processed.
// Compaction needs every replica to have processed every block any replica
// holds; comparing head sets alone is not enough, since go-ds-crdt's head
// bookkeeping is path dependent (equal state can have different heads).
func (e *Engine) Missing(ctx context.Context, idx int, cids []string) []string {
	p := e.parts[idx]
	p.mu.RLock()
	ns := p.nsFor(p.gen)
	p.mu.RUnlock()
	var missing []string
	for _, s := range cids {
		c, err := cid.Decode(s)
		if err != nil {
			missing = append(missing, s)
			continue
		}
		key := ns.ChildString("b").ChildString(dshelp.MultihashToDsKey(c.Hash()).String())
		if ok, err := e.backing.Has(ctx, key); err != nil || !ok {
			missing = append(missing, s)
		}
	}
	return missing
}

// Quiescent reports whether the partition's DAG processing queue is empty.
func (e *Engine) Quiescent(ctx context.Context, idx int) bool {
	c, done := e.parts[idx].active()
	defer done()
	return c.InternalStats(ctx).QueuedJobs == 0
}

// stagedGen is a prepared next generation.
type stagedGen struct {
	crdt *crdt.Datastore
	view *bview
	gen  int
}

// Stage opens the next generation's datastore. With seed it is populated from
// the active generation's live state in one batch; the returned genesis is the
// seed's head CID ("" if there was nothing to seed) and n the live entry count.
func (e *Engine) Stage(ctx context.Context, idx int, seed bool) (genesis string, n int, err error) {
	p := e.parts[idx]
	p.mu.Lock()
	if p.staged != nil || p.hasPrev {
		p.mu.Unlock()
		return "", 0, errors.New("engine: rotation already in progress")
	}
	cur, gen := p.crdt, p.gen+1
	p.mu.Unlock()

	view := p.sb.newView(gen)
	next, err := crdt.New(e.backing, p.nsFor(gen), e.dagSvc, view, e.opts)
	if err != nil {
		return "", 0, fmt.Errorf("engine: stage partition %d: %w", idx, err)
	}
	if seed {
		res, err := cur.Query(ctx, query.Query{})
		if err != nil {
			_ = next.Close()
			return "", 0, err
		}
		batch, err := next.Batch(ctx)
		if err != nil {
			res.Close()
			_ = next.Close()
			return "", 0, err
		}
		for r := range res.Next() {
			if r.Error != nil {
				res.Close()
				_ = next.Close()
				return "", 0, r.Error
			}
			if err := batch.Put(ctx, ds.NewKey(r.Key), r.Value); err != nil {
				res.Close()
				_ = next.Close()
				return "", 0, err
			}
			n++
		}
		res.Close()
		if err := batch.Commit(ctx); err != nil {
			_ = next.Close()
			return "", 0, err
		}
		if heads := next.InternalStats(ctx).Heads; len(heads) > 0 {
			genesis = heads[0].Cid.String()
		}
	}
	p.mu.Lock()
	p.staged = &stagedGen{crdt: next, view: view, gen: gen}
	p.mu.Unlock()
	return genesis, n, nil
}

// Activate makes the staged generation current, remembering the previous one
// until Commit/Rollback. With waitSeed, reads block until WaitSeed returns
// (the new generation may not yet hold the seeded data).
func (e *Engine) Activate(ctx context.Context, idx int, waitSeed bool) error {
	p := e.parts[idx]
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.staged
	if st == nil {
		return errors.New("engine: nothing staged")
	}
	p.oldDS, p.oldView = p.crdt, p.view
	p.hasPrev, p.prevGen = true, p.gen
	p.crdt, p.view, p.gen = st.crdt, st.view, st.gen
	p.staged = nil
	p.sb.setCurrent(p.view)
	if waitSeed {
		p.gate.setBlockReads(true)
	}
	return e.persistGen(ctx, p)
}

// ResendSeed re-broadcasts the active generation's latest heads. A coordinator
// calls it after peers have switched, since the seed's first broadcast reaches
// them while they are still on the old generation.
func (e *Engine) ResendSeed(ctx context.Context, idx int) error {
	p := e.parts[idx]
	p.mu.RLock()
	v := p.view
	p.mu.RUnlock()
	return v.resend(ctx)
}

// WaitSeed blocks until the active generation's heads include genesis (the
// coordinator's seed has replicated here), then re-opens reads.
func (e *Engine) WaitSeed(ctx context.Context, idx int, genesis string, timeout time.Duration) error {
	p := e.parts[idx]
	deadline := time.Now().Add(timeout)
	for {
		if genesis == "" || e.hasHead(ctx, p, genesis) {
			p.gate.setBlockReads(false)
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("engine: timed out waiting for the seeded generation")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (e *Engine) hasHead(ctx context.Context, p *partition, cidStr string) bool {
	c, done := p.active()
	defer done()
	for _, h := range c.InternalStats(ctx).Heads {
		if h.Cid.String() == cidStr {
			return true
		}
	}
	return false
}

// Commit finishes a rotation: the previous generation is closed and purged.
func (e *Engine) Commit(ctx context.Context, idx int) error {
	p := e.parts[idx]
	p.mu.Lock()
	old, oldGen := p.oldDS, p.prevGen
	had := p.hasPrev
	p.oldDS, p.oldView, p.hasPrev = nil, nil, false
	err := e.persistGen(ctx, p)
	p.mu.Unlock()
	if had {
		if old != nil {
			_ = old.Close()
		}
		_ = e.purgeNamespace(ctx, p.nsFor(oldGen).String())
	}
	return err
}

// Rollback abandons a staged or activated rotation, restoring the previous
// generation and discarding the new one.
func (e *Engine) Rollback(ctx context.Context, idx int) error {
	p := e.parts[idx]
	p.mu.Lock()
	var drop *crdt.Datastore
	var dropGen int
	switch {
	case p.staged != nil:
		drop, dropGen = p.staged.crdt, p.staged.gen
		p.staged = nil
	case p.hasPrev:
		restored, view := p.oldDS, p.oldView
		if restored == nil { // restarted mid-rotation: reopen the old generation
			view = p.sb.newView(p.prevGen)
			var err error
			if restored, err = crdt.New(e.backing, p.nsFor(p.prevGen), e.dagSvc, view, e.opts); err != nil {
				p.mu.Unlock()
				return fmt.Errorf("engine: reopen generation %d: %w", p.prevGen, err)
			}
		}
		drop, dropGen = p.crdt, p.gen
		p.crdt, p.view, p.gen = restored, view, p.prevGen
		p.oldDS, p.oldView, p.hasPrev = nil, nil, false
		p.sb.setCurrent(p.view)
	}
	err := e.persistGen(ctx, p)
	p.mu.Unlock()
	if drop != nil {
		_ = drop.Close()
		_ = e.purgeNamespace(ctx, p.nsFor(dropGen).String())
	}
	p.gate.set(p.gate.isFrozen(), false)
	return err
}

func (g *gate) isFrozen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.frozen
}

// RotatePartition compacts one partition on this node alone: snapshot live
// state, seed a fresh generation, swap, purge the old one. Single-node correct;
// in a cluster use package rotation, which establishes that every replica holds
// identical state first. Returns the live entry count carried forward.
func (e *Engine) RotatePartition(ctx context.Context, idx int) (int, error) {
	if err := e.Freeze(ctx, idx); err != nil {
		e.Unfreeze(idx)
		return 0, err
	}
	defer e.Unfreeze(idx)
	_, n, err := e.Stage(ctx, idx, true)
	if err != nil {
		return 0, err
	}
	if err := e.Activate(ctx, idx, false); err != nil {
		_ = e.Rollback(ctx, idx)
		return 0, err
	}
	return n, e.Commit(ctx, idx)
}

// Rotate compacts every partition on this node (a full local purge).
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
