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

// MemNetwork is an in-process gossip network for tests and embedded clusters.
// A message broadcast by (node, partition) is delivered to every other node's
// inbox for the SAME partition, so each named partition DAG gossips on its own
// logical channel (DESIGN §5.5). Paired with a shared DAGService it runs N
// fully-connected CRDT replicas without libp2p, with partition/heal control.
type MemNetwork struct {
	mu          sync.RWMutex
	nodes       int
	parts       int
	inboxes     [][]chan []byte // [node][partition]
	partitioned []bool          // [node] — isolates a node across all partitions
}

// NewMemNetwork builds an n-node single-DAG network: the control handle, one
// Broadcaster per node, and a shared DAGService.
func NewMemNetwork(n int) (*MemNetwork, []crdt.Broadcaster, ipld.DAGService) {
	net, factories, dag := NewMemNetworkP(n, 1)
	bcasts := make([]crdt.Broadcaster, n)
	for i := range bcasts {
		bcasts[i] = factories[i](0)
	}
	return net, bcasts, dag
}

// NewMemNetworkP builds an n-node, p-partition network: the control handle, a
// per-node BroadcasterFactory (factory(partition) → broadcaster), and a shared
// DAGService.
func NewMemNetworkP(nodes, parts int) (*MemNetwork, []BroadcasterFactory, ipld.DAGService) {
	net := &MemNetwork{
		nodes:       nodes,
		parts:       parts,
		inboxes:     make([][]chan []byte, nodes),
		partitioned: make([]bool, nodes),
	}
	for i := 0; i < nodes; i++ {
		net.inboxes[i] = make([]chan []byte, parts)
		for p := 0; p < parts; p++ {
			net.inboxes[i][p] = make(chan []byte, 1024)
		}
	}
	factories := make([]BroadcasterFactory, nodes)
	for i := range factories {
		node := i
		factories[node] = func(p int) crdt.Broadcaster {
			return &memBroadcaster{net: net, node: node, part: p}
		}
	}
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dag := merkledag.NewDAGService(bsrv.New(bs, offline.Exchange(bs)))
	return net, factories, dag
}

// SetPartitioned isolates (or rejoins) node i from the rest of the network.
func (net *MemNetwork) SetPartitioned(i int, p bool) {
	net.mu.Lock()
	net.partitioned[i] = p
	net.mu.Unlock()
}

func (net *MemNetwork) broadcast(fromNode, part int, payload []byte) {
	net.mu.RLock()
	defer net.mu.RUnlock()
	if net.partitioned[fromNode] {
		return
	}
	for i := 0; i < net.nodes; i++ {
		if i == fromNode || net.partitioned[i] {
			continue
		}
		buf := make([]byte, len(payload))
		copy(buf, payload)
		select {
		case net.inboxes[i][part] <- buf:
		default: // drop if a slow replica's inbox is full
		}
	}
}

type memBroadcaster struct {
	net  *MemNetwork
	node int
	part int
}

func (b *memBroadcaster) Broadcast(_ context.Context, payload []byte) error {
	b.net.broadcast(b.node, b.part, payload)
	return nil
}

func (b *memBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case payload := <-b.net.inboxes[b.node][b.part]:
		return payload, nil
	}
}
