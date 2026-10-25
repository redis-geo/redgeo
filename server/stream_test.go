package server

import (
	"strings"
	"testing"
	"time"
)

func TestStreams(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	if r := cl.do(t, "XADD", "s", "5-1", "a", "1"); r.Str != "5-1" {
		t.Fatalf("XADD explicit = %+v", r)
	}
	cl.do(t, "XADD", "s", "5-2", "b", "2", "c", "3")
	cl.do(t, "XADD", "s", "7-0", "d", "4")
	if r := cl.do(t, "XADD", "s", "7-0", "d", "4"); r.Kind != '-' {
		t.Fatalf("XADD duplicate = %+v", r)
	}
	if r := cl.do(t, "XADD", "s", "0-0", "d", "4"); r.Kind != '-' || !strings.Contains(r.Str, "greater than 0-0") {
		t.Fatalf("XADD 0-0 = %+v", r)
	}
	if r := cl.do(t, "XADD", "s", "*", "e", "5"); !strings.Contains(r.Str, "-") || r.Kind != '$' {
		t.Fatalf("XADD * = %+v", r)
	}
	if r := cl.do(t, "XADD", "s", "7-*", "x", "y"); r.Kind != '-' { // below the "*" entry's ms
		t.Fatalf("XADD 7-* = %+v", r)
	}
	if r := cl.do(t, "XLEN", "s"); r.Str != "4" {
		t.Fatalf("XLEN = %+v", r)
	}
	if r := cl.do(t, "TYPE", "s"); r.Str != "stream" {
		t.Fatalf("TYPE = %+v", r)
	}

	r := cl.do(t, "XRANGE", "s", "5", "5")
	if len(r.Arr) != 2 || r.Arr[0].Arr[0].Str != "5-1" || r.Arr[1].Arr[1].Arr[2].Str != "c" {
		t.Fatalf("XRANGE 5 5 = %+v", r)
	}
	if r := cl.do(t, "XRANGE", "s", "-", "+", "COUNT", "2"); len(r.Arr) != 2 {
		t.Fatalf("XRANGE COUNT = %+v", r)
	}
	if r := cl.do(t, "XRANGE", "s", "(5-1", "(7-0"); len(r.Arr) != 1 || r.Arr[0].Arr[0].Str != "5-2" {
		t.Fatalf("XRANGE exclusive = %+v", r)
	}
	if r := cl.do(t, "XREVRANGE", "s", "+", "-", "COUNT", "1"); len(r.Arr) != 1 || r.Arr[0].Arr[1].Arr[0].Str != "e" {
		t.Fatalf("XREVRANGE = %+v", r)
	}

	// XREAD: entries after an ID, multiple streams, null when nothing new.
	cl.do(t, "XADD", "t", "1-1", "k", "v")
	r = cl.do(t, "XREAD", "COUNT", "1", "STREAMS", "s", "t", "5-1", "0")
	if len(r.Arr) != 2 || r.Arr[0].Arr[0].Str != "s" || r.Arr[0].Arr[1].Arr[0].Arr[0].Str != "5-2" || r.Arr[1].Arr[0].Str != "t" {
		t.Fatalf("XREAD = %+v", r)
	}
	if r := cl.do(t, "XREAD", "STREAMS", "t", "1-1"); !r.Null {
		t.Fatalf("XREAD nothing new = %+v", r)
	}
	if r := cl.do(t, "XREAD", "STREAMS", "t", "$"); !r.Null {
		t.Fatalf("XREAD $ = %+v", r)
	}

	// BLOCK: another connection adds while this one waits.
	other := dialC(t, addr)
	defer other.close()
	go func() {
		time.Sleep(150 * time.Millisecond)
		other.do(t, "XADD", "t", "9-9", "late", "1")
	}()
	start := time.Now()
	r = cl.do(t, "XREAD", "BLOCK", "3000", "STREAMS", "t", "$")
	if len(r.Arr) != 1 || r.Arr[0].Arr[1].Arr[0].Arr[0].Str != "9-9" || time.Since(start) > 2*time.Second {
		t.Fatalf("XREAD BLOCK = %+v after %v", r, time.Since(start))
	}
	start = time.Now()
	if r := cl.do(t, "XREAD", "BLOCK", "100", "STREAMS", "t", "$"); !r.Null || time.Since(start) < 90*time.Millisecond {
		t.Fatalf("XREAD BLOCK timeout = %+v after %v", r, time.Since(start))
	}

	// XDEL / XTRIM / MAXLEN on XADD.
	if r := cl.do(t, "XDEL", "s", "5-1", "5-1", "99-0"); r.Str != "1" {
		t.Fatalf("XDEL = %+v", r)
	}
	if r := cl.do(t, "XTRIM", "s", "MAXLEN", "~", "2"); r.Str != "1" {
		t.Fatalf("XTRIM = %+v", r)
	}
	if r := cl.do(t, "XTRIM", "s", "MINID", "9999999999999"); r.Str != "2" {
		t.Fatalf("XTRIM MINID = %+v", r)
	}
	if r := cl.do(t, "XLEN", "s"); r.Str != "0" {
		t.Fatalf("XLEN after trims = %+v", r)
	}
	cl.do(t, "XADD", "m", "MAXLEN", "2", "1-1", "a", "b")
	cl.do(t, "XADD", "m", "MAXLEN", "2", "1-2", "a", "b")
	cl.do(t, "XADD", "m", "MAXLEN", "2", "1-3", "a", "b")
	if r := cl.do(t, "XLEN", "m"); r.Str != "2" {
		t.Fatalf("XADD MAXLEN len = %+v", r)
	}
	if r := cl.do(t, "XADD", "nomk", "NOMKSTREAM", "*", "a", "b"); !r.Null {
		t.Fatalf("NOMKSTREAM = %+v", r)
	}
	if r := cl.do(t, "EXISTS", "nomk"); r.Str != "0" {
		t.Fatalf("NOMKSTREAM created key: %+v", r)
	}

	// Errors and SCAN TYPE.
	cl.do(t, "SET", "str", "x")
	for _, bad := range [][]string{
		{"XADD", "str", "*", "a", "b"}, {"XLEN", "str"}, {"XADD", "s", "*", "odd"},
		{"XRANGE", "s", "x", "+"}, {"XREAD", "STREAMS", "s"}, {"XTRIM", "s", "MAXLEN", "-1"},
		{"XADD", "s", "bad-id", "a", "b"},
	} {
		if r := cl.do(t, bad...); r.Kind != '-' {
			t.Fatalf("%v = %+v want error", bad, r)
		}
	}
	if r := cl.do(t, "SCAN", "0", "TYPE", "stream", "COUNT", "100"); len(r.Arr[1].Arr) != 3 {
		t.Fatalf("SCAN TYPE stream = %+v", r)
	}
	if r := cl.do(t, "DEL", "s", "t", "m"); r.Str != "3" {
		t.Fatalf("DEL = %+v", r)
	}
}
