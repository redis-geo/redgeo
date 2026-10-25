package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis-geo/redgeo/crdtstore"
	"github.com/redis-geo/redgeo/engine"
)

// startBusServer starts a node whose PUBLISH is relayed over bus.
func startBusServer(t *testing.T, id string, bus Bus) (string, func()) {
	t.Helper()
	eng, err := engine.New(context.Background(), engine.Config{ReplicaID: id})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	srv := New(addr, crdtstore.NewStore(eng))
	srv.SetBus(bus, id)
	ready := make(chan error, 1)
	go func() { _ = srv.Start(ready) }()
	if err := <-ready; err != nil {
		t.Fatalf("server start: %v", err)
	}
	return addr, func() { _ = srv.Stop(); _ = eng.Close() }
}

func TestGeoPubSub(t *testing.T) {
	mnet, _, _ := engine.NewMemNetwork(3)
	addrA, stopA := startBusServer(t, "a", mnet.Bus(0))
	defer stopA()
	addrB, stopB := startBusServer(t, "b", mnet.Bus(1))
	defer stopB()
	addrC, stopC := startBusServer(t, "c", mnet.Bus(2))
	defer stopC()

	// Subscribers: B (channel), C (pattern), A (channel, to prove no self-echo).
	subA, subB, subC := dialC(t, addrA), dialC(t, addrB), dialC(t, addrC)
	defer subA.close()
	defer subB.close()
	defer subC.close()
	subA.do(t, "SUBSCRIBE", "news")
	subB.do(t, "SUBSCRIBE", "news")
	subC.do(t, "PSUBSCRIBE", "n*")

	pub := dialC(t, addrA)
	defer pub.close()
	// PUBLISH reports LOCAL receivers only (A has one).
	var n reply
	for i := 0; i < 50; i++ {
		if n = pub.do(t, "PUBLISH", "news", "hello"); n.Str == "1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.Str != "1" {
		t.Fatalf("PUBLISH local count = %+v want 1", n)
	}

	expect := func(name string, cl *client, kind string, args ...string) {
		t.Helper()
		cl.c.SetReadDeadline(time.Now().Add(3 * time.Second))
		msg, err := readReply(cl.r)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(msg.Arr) != len(args)+1 || msg.Arr[0].Str != kind {
			t.Fatalf("%s frame = %+v", name, msg)
		}
		for i, a := range args {
			if msg.Arr[i+1].Str != a {
				t.Fatalf("%s frame = %+v", name, msg)
			}
		}
	}
	expect("A local", subA, "message", "news", "hello")
	expect("B remote", subB, "message", "news", "hello")
	expect("C pattern", subC, "pmessage", "n*", "news", "hello")

	// A must not receive its own message a second time via the bus.
	subA.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if msg, err := readReply(subA.r); err == nil {
		t.Fatalf("A got an echo: %+v", msg)
	}

	// A partitioned node neither sends nor receives.
	mnet.SetPartitioned(1, true)
	pub.do(t, "PUBLISH", "news", "lost")
	expect("A local 2", subA, "message", "news", "lost")
	subB.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if msg, err := readReply(subB.r); err == nil {
		t.Fatalf("partitioned B received: %+v", msg)
	}
}

func TestBusFrame(t *testing.T) {
	o, c, m, err := parseBusFrame(busFrame("origin", "ch\x00an", "mess\r\nage"))
	if err != nil || o != "origin" || c != "ch\x00an" || m != "mess\r\nage" {
		t.Fatalf("round trip = %q %q %q %v", o, c, m, err)
	}
	for _, bad := range [][]byte{nil, {5, 'a'}, {1, 'a', 1, 'b'}} {
		if _, _, _, err := parseBusFrame(bad); err == nil {
			t.Fatalf("accepted malformed frame %v", bad)
		}
	}
}
