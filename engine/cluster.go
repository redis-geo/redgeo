package engine

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"

	crdt "github.com/ipfs/go-ds-crdt"
	ipld "github.com/ipfs/go-ipld-format"

	ipfslite "github.com/hsanjuan/ipfs-lite"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// dataTopic is the gossipsub topic CRDT deltas are published on.
const dataTopic = "redgeo/crdt/v1"

// ClusterConfig configures the libp2p replication mesh (DESIGN §7).
type ClusterConfig struct {
	// ListenAddrs are libp2p multiaddrs to listen on (e.g.
	// "/ip4/0.0.0.0/tcp/4001"). Empty = a sane default TCP listener.
	ListenAddrs []string
	// KeyPath persists the node's ed25519 identity key; the peer ID derived
	// from it is the stable replicaID (DESIGN §8). Empty = ephemeral identity.
	KeyPath string
	// Bootstraps are peer multiaddrs to dial on startup (e.g.
	// "/ip4/1.2.3.4/tcp/4001/p2p/Qm...").
	Bootstraps []string
}

// Cluster is a live libp2p replication mesh: a Broadcaster (single-DAG) or
// BroadcasterFactory (per-partition topics) + DAGService to feed into
// engine.Config, plus the peer identity and a Close.
type Cluster struct {
	Broadcaster crdt.Broadcaster
	// BroadcasterFactory joins a distinct gossipsub topic per partition so each
	// named partition DAG gossips independently (DESIGN §5.5).
	BroadcasterFactory func(p int) crdt.Broadcaster
	DAGService         ipld.DAGService
	ReplicaID          string // libp2p peer ID
	closers            []func() error
}

// NewCluster builds the libp2p host, DHT, gossipsub, IPFS-Lite DAG service, and
// the PubSubBroadcaster, mirroring the go-ds-crdt globaldb example.
func NewCluster(ctx context.Context, cfg ClusterConfig) (*Cluster, error) {
	priv, err := loadOrCreateKey(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("cluster: identity key: %w", err)
	}
	pid, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		return nil, fmt.Errorf("cluster: peer id: %w", err)
	}

	listen := cfg.ListenAddrs
	if len(listen) == 0 {
		listen = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	listenMA := make([]multiaddr.Multiaddr, 0, len(listen))
	for _, a := range listen {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			return nil, fmt.Errorf("cluster: listen addr %q: %w", a, err)
		}
		listenMA = append(listenMA, ma)
	}

	h, dht, err := ipfslite.SetupLibp2p(ctx, priv, nil, listenMA, nil, ipfslite.Libp2pOptionsExtra...)
	if err != nil {
		return nil, fmt.Errorf("cluster: setup libp2p: %w", err)
	}

	lite, err := ipfslite.New(ctx, nil, nil, h, dht, nil)
	if err != nil {
		_ = h.Close()
		_ = dht.Close()
		return nil, fmt.Errorf("cluster: ipfs-lite: %w", err)
	}

	// Dial bootstrap peers if any.
	if len(cfg.Bootstraps) > 0 {
		var infos []peer.AddrInfo
		for _, b := range cfg.Bootstraps {
			ma, err := multiaddr.NewMultiaddr(b)
			if err != nil {
				continue
			}
			if ai, err := peer.AddrInfoFromP2pAddr(ma); err == nil {
				infos = append(infos, *ai)
			}
		}
		lite.Bootstrap(infos)
	}

	psub, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		_ = h.Close()
		_ = dht.Close()
		return nil, fmt.Errorf("cluster: gossipsub: %w", err)
	}
	bcast, err := crdt.NewPubSubBroadcaster(ctx, psub, dataTopic)
	if err != nil {
		_ = h.Close()
		_ = dht.Close()
		return nil, fmt.Errorf("cluster: broadcaster: %w", err)
	}

	// Per-partition broadcaster: one gossipsub topic per partition DAG. Joins
	// lazily; a failed join falls back to a no-op so a single bad topic doesn't
	// take the node down.
	factory := func(p int) crdt.Broadcaster {
		topic := fmt.Sprintf("%s/p%02x", dataTopic, p)
		b, jerr := crdt.NewPubSubBroadcaster(ctx, psub, topic)
		if jerr != nil {
			return noopBroadcaster{ctx: ctx}
		}
		return b
	}

	return &Cluster{
		Broadcaster:        bcast,
		BroadcasterFactory: factory,
		DAGService:         lite,
		ReplicaID:          pid.String(),
		closers:            []func() error{dht.Close, h.Close},
	}, nil
}

// Close tears down the mesh.
func (c *Cluster) Close() error {
	var err error
	for _, fn := range c.closers {
		if e := fn(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// loadOrCreateKey loads the ed25519 identity from path, creating and persisting
// one if absent. Empty path => ephemeral in-memory key.
func loadOrCreateKey(path string) (crypto.PrivKey, error) {
	if path != "" {
		if b, err := os.ReadFile(path); err == nil {
			return crypto.UnmarshalPrivateKey(b)
		}
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	if path != "" {
		if b, err := crypto.MarshalPrivateKey(priv); err == nil {
			_ = os.WriteFile(path, b, 0o400)
		}
	}
	return priv, nil
}
