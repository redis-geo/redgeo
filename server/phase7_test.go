package server

import (
	"testing"
	"time"
)

func TestMultiExecDiscard(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// MULTI queues commands; EXEC runs them and returns an array of replies.
	if r := cl.do(t, "MULTI"); r.Str != "OK" {
		t.Fatalf("MULTI = %+v", r)
	}
	if r := cl.do(t, "SET", "k", "v"); r.Str != "QUEUED" {
		t.Fatalf("queued SET = %+v", r)
	}
	if r := cl.do(t, "INCR", "n"); r.Str != "QUEUED" {
		t.Fatalf("queued INCR = %+v", r)
	}
	r := cl.do(t, "EXEC")
	if len(r.Arr) != 2 || r.Arr[0].Str != "OK" || r.Arr[1].Str != "1" {
		t.Fatalf("EXEC = %+v", r)
	}
	// Effects are visible after EXEC.
	if g := cl.do(t, "GET", "k"); g.Str != "v" {
		t.Fatalf("GET after EXEC = %+v", g)
	}

	// DISCARD throws away the queue.
	cl.do(t, "MULTI")
	cl.do(t, "SET", "k", "changed")
	if r := cl.do(t, "DISCARD"); r.Str != "OK" {
		t.Fatalf("DISCARD = %+v", r)
	}
	if g := cl.do(t, "GET", "k"); g.Str != "v" {
		t.Fatalf("GET after DISCARD = %+v (discard didn't roll back queue)", g)
	}

	// EXEC without MULTI errors.
	if r := cl.do(t, "EXEC"); r.Kind != '-' {
		t.Fatalf("bare EXEC = %+v, want error", r)
	}

	// A bad queued command aborts EXEC.
	cl.do(t, "MULTI")
	cl.do(t, "SET", "k") // wrong arity -> queue parse error
	if r := cl.do(t, "EXEC"); r.Kind != '-' {
		t.Fatalf("EXEC after bad queue = %+v, want EXECABORT", r)
	}
}

func TestPubSub(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	sub := dialC(t, addr)
	defer sub.close()
	pub := dialC(t, addr)
	defer pub.close()

	// Subscribe and read the subscribe confirmation.
	conf := sub.do(t, "SUBSCRIBE", "news")
	if len(conf.Arr) != 3 || conf.Arr[0].Str != "subscribe" || conf.Arr[1].Str != "news" {
		t.Fatalf("SUBSCRIBE confirmation = %+v", conf)
	}

	// Publish from the other connection; one subscriber should receive it.
	// (Allow a brief moment for the detached subscriber to be registered.)
	var n reply
	for i := 0; i < 50; i++ {
		n = pub.do(t, "PUBLISH", "news", "hello")
		if n.Str == "1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.Str != "1" {
		t.Fatalf("PUBLISH receiver count = %+v, want 1", n)
	}

	// The subscriber receives the message frame.
	sub.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := readReply(sub.r)
	if err != nil {
		t.Fatalf("read published message: %v", err)
	}
	if len(msg.Arr) != 3 || msg.Arr[0].Str != "message" || msg.Arr[1].Str != "news" || msg.Arr[2].Str != "hello" {
		t.Fatalf("message frame = %+v", msg)
	}
}
