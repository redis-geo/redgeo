package engine

import (
	"context"
	"sync"

	bsrv "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	crdt "github.com/ipfs/go-ds-crdt"
	ipld "github.com/ipfs/go-ipld-format"
)

// memNetwork is an in-process gossip network: each replica's Broadcast is
// delivered to every other replica's inbox. Paired with a shared DAGService it
// lets a test or embedded cluster run N fully-connected CRDT replicas without
// libp2p, which is how convergence and partition/heal behavior is exercised
// (DESIGN §9 phase 8 testing strategy).
type MemNetwork struct {
	mu      sync.RWMutex
	inboxes []chan []byte
	// partitioned[i] drops i's outgoing and incoming traffic (simulated split).
	partitioned []bool
}

// NewMemNetwork builds an n-node gossip network and returns the control handle
// (for partition/heal), one Broadcaster per node, and a shared DAGService.
func NewMemNetwork(n int) (*MemNetwork, []crdt.Broadcaster, ipld.DAGService) {
	net := &MemNetwork{
		inboxes:     make([]chan []byte, n),
		partitioned: make([]bool, n),
	}
	for i := range net.inboxes {
		net.inboxes[i] = make(chan []byte, 1024)
	}
	bcasts := make([]crdt.Broadcaster, n)
	for i := range bcasts {
		bcasts[i] = &memBroadcaster{net: net, id: i}
	}
	// One shared DAG service: blocks written by any replica are readable by all
	// (perfect block exchange). Head propagation is what the broadcaster gossips.
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dag := merkledag.NewDAGService(bsrv.New(bs, offline.Exchange(bs)))
	return net, bcasts, dag
}

// SetPartitioned isolates (or rejoins) node i from the rest of the network.
func (net *MemNetwork) SetPartitioned(i int, p bool) {
	net.mu.Lock()
	net.partitioned[i] = p
	net.mu.Unlock()
}

func (net *MemNetwork) broadcast(from int, payload []byte) {
	net.mu.RLock()
	defer net.mu.RUnlock()
	if net.partitioned[from] {
		return // isolated node's writes don't leave
	}
	for i, inbox := range net.inboxes {
		if i == from || net.partitioned[i] {
			continue
		}
		// Copy so receivers can't alias the sender's buffer.
		buf := make([]byte, len(payload))
		copy(buf, payload)
		select {
		case inbox <- buf:
		default: // drop if a slow replica's inbox is full
		}
	}
}

type memBroadcaster struct {
	net *MemNetwork
	id  int
}

func (b *memBroadcaster) Broadcast(_ context.Context, payload []byte) error {
	b.net.broadcast(b.id, payload)
	return nil
}

func (b *memBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case payload := <-b.net.inboxes[b.id]:
		return payload, nil
	}
}
