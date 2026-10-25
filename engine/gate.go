package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	crdt "github.com/ipfs/go-ds-crdt"
)

// ErrPartitionBusy is returned when a write (or read) had to wait out a
// compaction window longer than gateWait.
var ErrPartitionBusy = errors.New("LOADING partition is being compacted, retry")

// gateWait bounds how long an operation waits for a rotation window to end.
const gateWait = 15 * time.Second

// gate is a partition's compaction fence. Rotation freezes writes (waiting for
// in-flight ones to drain) so the replicas can be compared and switched while
// no new deltas are minted, and blocks reads for the instant a freshly
// activated generation has not yet received its seed.
type gate struct {
	mu         sync.Mutex
	changed    chan struct{} // closed and replaced on every state change
	frozen     bool
	blockReads bool
	inflight   int
}

func newGate() *gate { return &gate{changed: make(chan struct{})} }

func (g *gate) signal() { close(g.changed); g.changed = make(chan struct{}) }

// wait blocks until pred (called with g.mu held) is false.
func (g *gate) waitWhile(ctx context.Context, pred func() bool) error {
	deadline := time.NewTimer(gateWait)
	defer deadline.Stop()
	for pred() {
		ch := g.changed
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			g.mu.Lock()
			return ctx.Err()
		case <-deadline.C:
			g.mu.Lock()
			return ErrPartitionBusy
		}
		g.mu.Lock()
	}
	return nil
}

// beginWrite admits a write once the partition is unfrozen; the returned func
// must be called when the write finishes.
func (g *gate) beginWrite(ctx context.Context) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.waitWhile(ctx, func() bool { return g.frozen }); err != nil {
		return nil, err
	}
	g.inflight++
	return func() {
		g.mu.Lock()
		g.inflight--
		g.signal()
		g.mu.Unlock()
	}, nil
}

func (g *gate) waitReadable(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waitWhile(ctx, func() bool { return g.blockReads })
}

// freeze stops new writes and waits for in-flight ones to finish.
func (g *gate) freeze(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.frozen = true
	g.signal()
	return g.waitWhile(ctx, func() bool { return g.inflight > 0 })
}

func (g *gate) set(frozen, blockReads bool) {
	g.mu.Lock()
	g.frozen, g.blockReads = frozen, blockReads
	g.signal()
	g.mu.Unlock()
}

func (g *gate) setBlockReads(b bool) {
	g.mu.Lock()
	g.blockReads = b
	g.signal()
	g.mu.Unlock()
}

// switchBcast fans one underlying Broadcaster out to whichever CRDT generation
// is current. Without it, the old and new datastores of a rotation would both
// read the same inbox and steal each other's messages.
type switchBcast struct {
	inner crdt.Broadcaster
	mu    sync.Mutex
	cur   *bview
}

func newSwitchBcast(ctx context.Context, inner crdt.Broadcaster) *switchBcast {
	s := &switchBcast{inner: inner}
	go s.pump(ctx)
	return s
}

func (s *switchBcast) pump(ctx context.Context) {
	for {
		data, err := s.inner.Next(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, crdt.ErrNoMoreBroadcast) {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.mu.Lock()
		v := s.cur
		s.mu.Unlock()
		if v != nil {
			select {
			case v.in <- data:
			default: // slow consumer: drop; anti-entropy re-sends heads
			}
		}
	}
}

func (s *switchBcast) newView() *bview { return &bview{s: s, in: make(chan []byte, 1024)} }

func (s *switchBcast) setCurrent(v *bview) {
	s.mu.Lock()
	s.cur = v
	s.mu.Unlock()
}

// bview is one generation's Broadcaster.
type bview struct {
	s  *switchBcast
	in chan []byte
}

func (v *bview) Broadcast(ctx context.Context, data []byte) error {
	return v.s.inner.Broadcast(ctx, data)
}

func (v *bview) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case d := <-v.in:
		return d, nil
	}
}
