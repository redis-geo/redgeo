package server

import (
	"strings"
	"testing"
	"time"
)

func TestStreamConsumerGroups(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	want := func(s string, args ...string) {
		t.Helper()
		if r := cl.do(t, args...); r.Str != s || r.Kind == '-' {
			t.Fatalf("%v = %+v want %q", args, r, s)
		}
	}
	errOf := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind != '-' {
			t.Fatalf("%v = %+v want error", args, r)
		}
		return r.Str
	}
	// ids lists the entry IDs of a [[id, fields]...] reply.
	ids := func(r reply) string {
		var out []string
		for _, e := range r.Arr {
			out = append(out, e.Arr[0].Str)
		}
		return strings.Join(out, " ")
	}
	// readIDs flattens an XREADGROUP reply: "key:id id|key:id".
	readIDs := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind == '-' {
			return "ERR:" + r.Str
		}
		if r.Null {
			return "nil"
		}
		var parts []string
		for _, s := range r.Arr {
			parts = append(parts, s.Arr[0].Str+":"+ids(s.Arr[1]))
		}
		return strings.Join(parts, "|")
	}

	cl.do(t, "XADD", "s", "1-1", "a", "1")
	cl.do(t, "XADD", "s", "2-1", "b", "2")
	cl.do(t, "XADD", "s", "3-1", "c", "3")

	// ---- groups
	want("OK", "XGROUP", "CREATE", "s", "g", "0")
	if e := errOf("XGROUP", "CREATE", "s", "g", "0"); !strings.HasPrefix(e, "BUSYGROUP") {
		t.Fatalf("duplicate group: %s", e)
	}
	if e := errOf("XGROUP", "CREATE", "nostream", "g", "0"); !strings.Contains(e, "MKSTREAM") {
		t.Fatalf("missing stream: %s", e)
	}
	want("OK", "XGROUP", "CREATE", "made", "g", "$", "MKSTREAM")
	want("0", "XLEN", "made")
	want("stream", "TYPE", "made")
	want("1", "XGROUP", "CREATECONSUMER", "s", "g", "idle-one")
	want("0", "XGROUP", "CREATECONSUMER", "s", "g", "idle-one")
	want("0", "XGROUP", "DELCONSUMER", "s", "g", "idle-one")
	if e := errOf("XGROUP", "CREATE", "s", "g2", "garbage"); e == "" {
		t.Fatal("bad id accepted")
	}
	errOf("XGROUP", "BOGUS", "s", "g")

	// ---- delivery: ">" gives each entry to exactly one consumer
	if got := readIDs("XREADGROUP", "GROUP", "g", "alice", "COUNT", "2", "STREAMS", "s", ">"); got != "s:1-1 2-1" {
		t.Fatalf("alice = %s", got)
	}
	if got := readIDs("XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">"); got != "s:3-1" {
		t.Fatalf("bob = %s", got)
	}
	if got := readIDs("XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">"); got != "nil" {
		t.Fatalf("nothing new = %s", got)
	}
	if e := errOf("XREADGROUP", "GROUP", "nogroup", "c", "STREAMS", "s", ">"); !strings.HasPrefix(e, "NOGROUP") {
		t.Fatalf("no group: %s", e)
	}

	// ---- XPENDING: summary and extended forms
	r := cl.do(t, "XPENDING", "s", "g")
	if len(r.Arr) != 4 || r.Arr[0].Str != "3" || r.Arr[1].Str != "1-1" || r.Arr[2].Str != "3-1" ||
		len(r.Arr[3].Arr) != 2 || r.Arr[3].Arr[0].Arr[0].Str != "alice" || r.Arr[3].Arr[0].Arr[1].Str != "2" {
		t.Fatalf("XPENDING summary = %+v", r)
	}
	r = cl.do(t, "XPENDING", "s", "g", "-", "+", "10")
	if len(r.Arr) != 3 || r.Arr[0].Arr[0].Str != "1-1" || r.Arr[0].Arr[1].Str != "alice" || r.Arr[0].Arr[3].Str != "1" {
		t.Fatalf("XPENDING extended = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g", "-", "+", "10", "bob"); len(r.Arr) != 1 {
		t.Fatalf("XPENDING consumer = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g", "IDLE", "3600000", "-", "+", "10"); len(r.Arr) != 0 {
		t.Fatalf("XPENDING IDLE = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g", "(1-1", "2-1", "10"); len(r.Arr) != 1 || r.Arr[0].Arr[0].Str != "2-1" {
		t.Fatalf("XPENDING exclusive range = %+v", r)
	}

	// ---- history replay and XACK
	r = cl.do(t, "XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0")
	if ids(r.Arr[0].Arr[1]) != "1-1 2-1" || r.Arr[0].Arr[1].Arr[0].Arr[1].Arr[0].Str != "a" {
		t.Fatalf("history = %+v", r)
	}
	if got := readIDs("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "1-1"); got != "s:2-1" {
		t.Fatalf("history after 1-1 = %s", got)
	}
	want("1", "XACK", "s", "g", "1-1")
	want("0", "XACK", "s", "g", "1-1")
	want("0", "XACK", "s", "nogroup", "1-1")
	if r = cl.do(t, "XPENDING", "s", "g"); r.Arr[0].Str != "2" {
		t.Fatalf("pending after ack = %+v", r)
	}

	// ---- XCLAIM / XAUTOCLAIM
	if r = cl.do(t, "XCLAIM", "s", "g", "carol", "3600000", "2-1"); len(r.Arr) != 0 {
		t.Fatalf("claim of a not-idle entry = %+v", r)
	}
	r = cl.do(t, "XCLAIM", "s", "g", "carol", "0", "2-1")
	if ids(r) != "2-1" || r.Arr[0].Arr[1].Arr[0].Str != "b" {
		t.Fatalf("XCLAIM = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g", "-", "+", "10", "carol"); r.Arr[0].Arr[3].Str != "2" { // delivery count bumped
		t.Fatalf("claimed entry = %+v", r)
	}
	if r = cl.do(t, "XCLAIM", "s", "g", "carol", "0", "3-1", "JUSTID"); len(r.Arr) != 1 || r.Arr[0].Str != "3-1" {
		t.Fatalf("XCLAIM JUSTID = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g", "-", "+", "10", "carol"); r.Arr[1].Arr[3].Str != "1" { // JUSTID keeps the count
		t.Fatalf("JUSTID bumped the count: %+v", r)
	}
	r = cl.do(t, "XAUTOCLAIM", "s", "g", "dave", "0", "0-0", "COUNT", "1")
	if r.Arr[0].Str != "3-1" || ids(r.Arr[1]) != "2-1" { // one claimed, cursor points at the next
		t.Fatalf("XAUTOCLAIM page 1 = %+v", r)
	}
	r = cl.do(t, "XAUTOCLAIM", "s", "g", "dave", "0", r.Arr[0].Str, "JUSTID")
	if r.Arr[0].Str != "0-0" || len(r.Arr[1].Arr) != 1 || r.Arr[1].Arr[0].Str != "3-1" {
		t.Fatalf("XAUTOCLAIM page 2 = %+v", r)
	}
	// An entry deleted from the stream is dropped from the pending list.
	want("1", "XDEL", "s", "3-1")
	r = cl.do(t, "XAUTOCLAIM", "s", "g", "dave", "0", "0-0")
	if len(r.Arr[2].Arr) != 1 || r.Arr[2].Arr[0].Str != "3-1" || ids(r.Arr[1]) != "2-1" {
		t.Fatalf("XAUTOCLAIM with a deleted entry = %+v", r)
	}
	if r = cl.do(t, "XPENDING", "s", "g"); r.Arr[0].Str != "1" {
		t.Fatalf("pending after dropping deleted = %+v", r)
	}

	// ---- XINFO
	r = cl.do(t, "XINFO", "GROUPS", "s")
	if len(r.Arr) != 1 || r.Arr[0].Arr[1].Str != "g" || r.Arr[0].Arr[7].Str != "3-1" {
		t.Fatalf("XINFO GROUPS = %+v", r)
	}
	r = cl.do(t, "XINFO", "CONSUMERS", "s", "g")
	if len(r.Arr) < 3 {
		t.Fatalf("XINFO CONSUMERS = %+v", r)
	}
	r = cl.do(t, "XINFO", "STREAM", "s")
	if r.Arr[0].Str != "length" || r.Arr[1].Str != "2" {
		t.Fatalf("XINFO STREAM = %+v", r)
	}
	errOf("XINFO", "STREAM", "nokey")
	errOf("XINFO", "CONSUMERS", "s", "nogroup")

	// ---- SETID rewinds delivery (the epoch makes a lower id win)
	want("OK", "XGROUP", "SETID", "s", "g", "0")
	if got := readIDs("XREADGROUP", "GROUP", "g", "erin", "STREAMS", "s", ">"); got != "s:1-1 2-1" { // acked entries are still in the stream
		t.Fatalf("after SETID 0 = %s", got)
	}
	want("OK", "XGROUP", "SETID", "s", "g", "$")
	if got := readIDs("XREADGROUP", "GROUP", "g", "erin", "STREAMS", "s", ">"); got != "nil" {
		t.Fatalf("after SETID $ = %s", got)
	}

	// ---- NOACK, BLOCK, multi-stream
	cl.do(t, "XADD", "s", "5-1", "e", "5")
	if got := readIDs("XREADGROUP", "GROUP", "g", "erin", "NOACK", "STREAMS", "s", ">"); got != "s:5-1" {
		t.Fatalf("NOACK = %s", got)
	}
	r = cl.do(t, "XPENDING", "s", "g", "-", "+", "10", "erin")
	for _, e := range r.Arr {
		if e.Arr[0].Str == "5-1" {
			t.Fatal("NOACK entry is pending")
		}
	}
	other := dialC(t, addr)
	defer other.close()
	go func() {
		time.Sleep(150 * time.Millisecond)
		other.do(t, "XADD", "s", "9-9", "late", "1")
	}()
	start := time.Now()
	if got := readIDs("XREADGROUP", "GROUP", "g", "erin", "BLOCK", "3000", "STREAMS", "s", ">"); got != "s:9-9" || time.Since(start) > 2*time.Second {
		t.Fatalf("BLOCK = %s after %v", got, time.Since(start))
	}
	cl.do(t, "XADD", "t", "1-1", "x", "y")
	want("OK", "XGROUP", "CREATE", "t", "g", "0")
	got := readIDs("XREADGROUP", "GROUP", "g", "m", "STREAMS", "s", "t", ">", ">")
	if got != "t:1-1" {
		t.Fatalf("multi-stream = %s", got)
	}

	// ---- groups follow RENAME and die with DEL
	want("OK", "RENAME", "t", "t2")
	if r = cl.do(t, "XPENDING", "t2", "g"); r.Arr[0].Str != "1" {
		t.Fatalf("group lost on RENAME: %+v", r)
	}
	want("1", "DEL", "t2")
	want("OK", "RENAME", "s", "s2")
	want("1", "XGROUP", "DESTROY", "s2", "g")
	want("0", "XGROUP", "DESTROY", "s2", "g")
	if e := errOf("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s2", ">"); !strings.HasPrefix(e, "NOGROUP") {
		t.Fatalf("after destroy: %s", e)
	}
	// A destroyed group can be created again from scratch.
	want("OK", "XGROUP", "CREATE", "s2", "g", "0")
	if got := readIDs("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s2", ">"); !strings.HasPrefix(got, "s2:1-1 2-1") {
		t.Fatalf("recreated group = %s", got)
	}
}
