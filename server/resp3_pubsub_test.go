package server

import (
	"testing"
	"time"
)

// TestRESP3PubSubPush verifies that a RESP3 subscriber receives subscribe
// confirmations and messages as push frames ('>'), while a RESP2 subscriber
// gets plain arrays ('*') — native support from the forked redcon.
func TestRESP3PubSubPush(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	// RESP3 subscriber.
	sub3 := dialC(t, addr)
	defer sub3.close()
	if r := sub3.do(t, "HELLO", "3"); r.Kind != '%' {
		t.Fatalf("HELLO 3 = %q", r.Kind)
	}
	conf := sub3.do(t, "SUBSCRIBE", "news")
	if conf.Kind != '>' || len(conf.Arr) != 3 || conf.Arr[0].Str != "subscribe" {
		t.Fatalf("RESP3 subscribe confirmation = kind %q %+v, want push", conf.Kind, conf)
	}

	// RESP2 subscriber (default).
	sub2 := dialC(t, addr)
	defer sub2.close()
	if conf := sub2.do(t, "SUBSCRIBE", "news"); conf.Kind != '*' {
		t.Fatalf("RESP2 subscribe confirmation = kind %q, want array", conf.Kind)
	}

	// Publish from a third connection.
	pub := dialC(t, addr)
	defer pub.close()
	var n reply
	for i := 0; i < 50; i++ {
		n = pub.do(t, "PUBLISH", "news", "hello")
		if n.Str == "2" { // both subscribers
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.Str != "2" {
		t.Fatalf("PUBLISH receivers = %+v, want 2", n)
	}

	// RESP3 subscriber gets a push-framed message.
	sub3.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	m3, err := readReply(sub3.r)
	if err != nil {
		t.Fatalf("resp3 read: %v", err)
	}
	if m3.Kind != '>' || len(m3.Arr) != 3 || m3.Arr[0].Str != "message" || m3.Arr[2].Str != "hello" {
		t.Fatalf("RESP3 message = kind %q %+v, want push message frame", m3.Kind, m3)
	}

	// RESP2 subscriber gets an array-framed message.
	sub2.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	m2, err := readReply(sub2.r)
	if err != nil {
		t.Fatalf("resp2 read: %v", err)
	}
	if m2.Kind != '*' || m2.Arr[0].Str != "message" {
		t.Fatalf("RESP2 message = kind %q, want array", m2.Kind)
	}
}
