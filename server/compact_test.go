package server

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
	"github.com/redis-geo/redgeo/rotation"
)

func TestRedgeoCompactCommands(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.New(ctx, engine.Config{ReplicaID: "solo", NumPartitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	store := crdtstore.NewStore(eng)
	store.RegisterSelf(ctx)
	node := rotation.New(rotation.Config{Engine: eng, ID: "solo", Observe: store.Replicas})
	node.Start(ctx)

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	srv := New(addr, store)
	srv.SetCompactor(node)
	ready := make(chan error, 1)
	go func() { _ = srv.Start(ready) }()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	cl := dialC(t, addr)
	defer cl.close()
	for i := 0; i < 20; i++ {
		cl.do(t, "SADD", "s", string(rune('a'+i)))
	}
	cl.do(t, "SREM", "s", "a", "b", "c")

	if r := cl.do(t, "REDGEO.COMPACT"); r.Str != "OK" {
		t.Fatalf("REDGEO.COMPACT = %+v", r)
	}
	for p := 0; p < 4; p++ {
		if g := eng.Generation(p); g != 1 {
			t.Fatalf("partition %d generation = %d", p, g)
		}
	}
	if r := cl.do(t, "SCARD", "s"); r.Str != "17" {
		t.Fatalf("SCARD after compaction = %+v", r)
	}
	if r := cl.do(t, "SISMEMBER", "s", "a"); r.Str != "0" {
		t.Fatalf("deleted member back: %+v", r)
	}
	cl.do(t, "SADD", "s", "zz")
	if r := cl.do(t, "SCARD", "s"); r.Str != "18" {
		t.Fatalf("SCARD after post-compaction write = %+v", r)
	}
	if r := cl.do(t, "REDGEO.COMPACT", "2"); r.Str != "OK" || eng.Generation(2) != 2 {
		t.Fatalf("REDGEO.COMPACT 2 = %+v gen=%d", r, eng.Generation(2))
	}
	if r := cl.do(t, "REDGEO.MEMBERS"); r.Kind != '*' || len(r.Arr) != 0 {
		t.Fatalf("REDGEO.MEMBERS = %+v", r)
	}
	if r := cl.do(t, "REDGEO.FORGET", "nobody"); r.Str != "OK" {
		t.Fatalf("REDGEO.FORGET = %+v", r)
	}
	for _, bad := range [][]string{{"REDGEO.COMPACT", "x"}, {"REDGEO.COMPACT", "99"}, {"REDGEO.FORGET"}, {"REDGEO.COMPACT", "1", "2"}} {
		if r := cl.do(t, bad...); r.Kind != '-' {
			t.Fatalf("%v = %+v want error", bad, r)
		}
	}

	// Without a compactor the commands say so.
	addr2, stop := startTestServer(t)
	defer stop()
	cl2 := dialC(t, addr2)
	defer cl2.close()
	if r := cl2.do(t, "REDGEO.COMPACT"); r.Kind != '-' || !strings.Contains(r.Str, "not enabled") {
		t.Fatalf("no compactor = %+v", r)
	}
}
