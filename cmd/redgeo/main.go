// Command redgeo runs an active/active, geo-distributed, Redis-compatible
// server backed by go-ds-crdt (DESIGN). Phase 0 is single-node.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
	"github.com/redis-geo/redgeo/rotation"
	"github.com/redis-geo/redgeo/server"
)

func main() {
	addr := flag.String("addr", ":6380", "RESP listen address")
	dataDir := flag.String("data", "", "data directory (empty = in-memory, ephemeral)")
	replicaID := flag.String("replica", "", "stable replica ID (empty = derive/persist one)")
	p2p := flag.Bool("p2p", false, "enable libp2p replication mesh (multi-node)")
	p2pListen := flag.String("p2p-listen", "/ip4/0.0.0.0/tcp/0", "libp2p listen multiaddr")
	bootstraps := flag.String("bootstrap", "", "comma-separated bootstrap peer multiaddrs")
	partitions := flag.Int("partitions", 1, "number of named partition DAGs (multi-node; 256 = one per bucket)")
	compactEvery := flag.Duration("compact-every", 0, "run cluster-wide compaction this often (0 = only on REDGEO.COMPACT); only the lowest replica ID acts")
	requirePass := flag.String("requirepass", "", "require clients to AUTH with this password (or set REDGEO_REQUIREPASS, which keeps it out of the process list)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file for the RESP listener (with -tls-key)")
	tlsKey := flag.String("tls-key", "", "TLS private key file for the RESP listener")
	metricsAddr := flag.String("metrics-addr", "", "serve Prometheus metrics at /metrics on this address (empty = off)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pebbleDir := ""
	if *dataDir != "" {
		pebbleDir = filepath.Join(*dataDir, "pebble")
	}

	cfg := engine.Config{PebbleDir: pebbleDir}
	var bus server.Bus         // geo pub/sub, only with -p2p
	var ctl rotation.Transport // compaction control channel, only with -p2p

	// Multi-node mode: stand up the libp2p mesh and derive the replica ID from
	// the peer identity (DESIGN §7, §8).
	if *p2p {
		keyPath, blocksDir := "", ""
		if *dataDir != "" {
			keyPath = filepath.Join(*dataDir, "p2p.key")
			blocksDir = filepath.Join(*dataDir, "blocks")
			_ = os.MkdirAll(*dataDir, 0o700)
		}
		var boots []string
		if *bootstraps != "" {
			boots = strings.Split(*bootstraps, ",")
		}
		cl, err := engine.NewCluster(ctx, engine.ClusterConfig{
			ListenAddrs:   []string{*p2pListen},
			KeyPath:       keyPath,
			BlockstoreDir: blocksDir,
			Bootstraps:    boots,
		})
		if err != nil {
			log.Fatalf("cluster: %v", err)
		}
		defer cl.Close()
		cfg.DAGService = cl.DAGService
		if cl.Bus != nil {
			bus = cl.Bus
		}
		if cl.Ctl != nil {
			ctl = cl.Ctl
		}
		cfg.RebroadcastInterval = 5 * time.Second
		cfg.ReplicaID = cl.ReplicaID
		cfg.NumPartitions = *partitions
		if *partitions > 1 {
			cfg.BroadcasterFactory = cl.BroadcasterFactory
		} else {
			cfg.Broadcaster = cl.Broadcaster
		}
		log.Printf("libp2p replication enabled (peer %s, %d partition DAGs)", cl.ReplicaID, *partitions)
	} else {
		rid, err := resolveReplicaID(*replicaID, *dataDir)
		if err != nil {
			log.Fatalf("replica id: %v", err)
		}
		cfg.ReplicaID = rid
	}

	eng, err := engine.New(ctx, cfg)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	defer eng.Close()
	rid := cfg.ReplicaID

	store := crdtstore.NewStore(eng)

	// Active TTL sweeper (DESIGN §6.8). Lazy filtering already hides expired
	// keys on read; the sweeper reclaims their storage.
	sweeper := crdtstore.NewSweeper(store, 10*time.Second)
	sweeper.Start(ctx)
	defer sweeper.Stop()

	// Compaction: every node can coordinate one; membership comes from the
	// replicated registry plus the control channel (package rotation).
	if err := store.RegisterSelf(ctx); err != nil {
		log.Fatalf("register replica: %v", err)
	}
	node := rotation.New(rotation.Config{
		Engine: eng, ID: rid, Transport: ctl,
		Observe: store.Replicas, Unregister: store.UnregisterReplica,
	})
	node.Start(ctx)
	if *compactEvery > 0 {
		go autoCompact(ctx, node, rid, *compactEvery)
	}

	srv := server.New(*addr, store)
	srv.SetCompactor(node)
	pw := *requirePass
	if pw == "" {
		pw = os.Getenv("REDGEO_REQUIREPASS")
	}
	if pw != "" {
		srv.SetPassword(pw)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("-tls-cert and -tls-key must be given together")
	}
	if *tlsCert != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
		srv.SetTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", srv.MetricsHandler())
		go func() {
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				log.Printf("metrics server: %v", err)
			}
		}()
		log.Printf("metrics on %s/metrics", *metricsAddr)
	}
	if bus != nil {
		srv.SetBus(bus, rid)
	}

	ready := make(chan error, 1)
	go func() {
		if err := srv.Start(ready); err != nil {
			log.Printf("server stopped: %v", err)
		}
	}()
	if err := <-ready; err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("redgeo listening on %s (replica %s)", srv.Addr(), rid)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	log.Print("shutting down")
	_ = srv.Stop()
}

// resolveReplicaID returns the configured replica ID, or loads/creates a
// persisted random one under dataDir, or (in-memory mode) mints an ephemeral
// one. A stable ID is required so this node always owns the same HLC slots
// (DESIGN §6.7, §8).
func resolveReplicaID(configured, dataDir string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	if dataDir == "" {
		return randID()
	}
	path := filepath.Join(dataDir, "replica_id")
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
		return string(b), nil
	}
	id, err := randID()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return "", err
	}
	return id, nil
}

func randID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// autoCompact periodically compacts every partition. All nodes may run it with
// the same interval; only the lexicographically lowest known replica ID acts, so
// they do not collide.
func autoCompact(ctx context.Context, node *rotation.Node, self string, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lowest := true
		for _, m := range node.Members() {
			if m < self {
				lowest = false
			}
		}
		if !lowest {
			continue
		}
		if err := node.CompactAll(ctx); err != nil {
			log.Printf("compaction: %v", err)
		} else {
			log.Printf("compaction complete")
		}
	}
}
