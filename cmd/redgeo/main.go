// Command redgeo runs an active/active, geo-distributed, Redis-compatible
// server backed by go-ds-crdt (DESIGN). Phase 0 is single-node.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
	"github.com/redis-geo/redgeo/server"
)

func main() {
	addr := flag.String("addr", ":6380", "RESP listen address")
	dataDir := flag.String("data", "", "data directory (empty = in-memory, ephemeral)")
	replicaID := flag.String("replica", "", "stable replica ID (empty = derive/persist one)")
	p2p := flag.Bool("p2p", false, "enable libp2p replication mesh (multi-node)")
	p2pListen := flag.String("p2p-listen", "/ip4/0.0.0.0/tcp/0", "libp2p listen multiaddr")
	bootstraps := flag.String("bootstrap", "", "comma-separated bootstrap peer multiaddrs")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pebbleDir := ""
	if *dataDir != "" {
		pebbleDir = filepath.Join(*dataDir, "pebble")
	}

	cfg := engine.Config{PebbleDir: pebbleDir}

	// Multi-node mode: stand up the libp2p mesh and derive the replica ID from
	// the peer identity (DESIGN §7, §8).
	if *p2p {
		keyPath := ""
		if *dataDir != "" {
			keyPath = filepath.Join(*dataDir, "p2p.key")
			_ = os.MkdirAll(*dataDir, 0o700)
		}
		var boots []string
		if *bootstraps != "" {
			boots = strings.Split(*bootstraps, ",")
		}
		cl, err := engine.NewCluster(ctx, engine.ClusterConfig{
			ListenAddrs: []string{*p2pListen},
			KeyPath:     keyPath,
			Bootstraps:  boots,
		})
		if err != nil {
			log.Fatalf("cluster: %v", err)
		}
		defer cl.Close()
		cfg.Broadcaster = cl.Broadcaster
		cfg.DAGService = cl.DAGService
		cfg.RebroadcastInterval = 5 * time.Second
		cfg.ReplicaID = cl.ReplicaID
		log.Printf("libp2p replication enabled (peer %s)", cl.ReplicaID)
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

	srv := server.New(*addr, store)

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
