package server

import "testing"

func TestBitmapCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	want := func(args []string, s string) {
		t.Helper()
		if r := cl.do(t, args...); r.Str != s {
			t.Fatalf("%v = %+v want %q", args, r, s)
		}
	}
	want([]string{"SETBIT", "b", "7", "1"}, "0")
	want([]string{"SETBIT", "b", "7", "0"}, "1")
	want([]string{"SETBIT", "b", "100", "1"}, "0")
	want([]string{"GETBIT", "b", "100"}, "1")
	want([]string{"GETBIT", "b", "101"}, "0")
	want([]string{"GETBIT", "nokey", "5"}, "0")
	want([]string{"BITCOUNT", "b"}, "1")
	want([]string{"BITPOS", "b", "1"}, "100")
	want([]string{"STRLEN", "b"}, "13")
	want([]string{"TYPE", "b"}, "string")

	cl.do(t, "SET", "foobar", "foobar")
	want([]string{"BITCOUNT", "foobar"}, "26")
	want([]string{"BITCOUNT", "foobar", "1", "1"}, "6")
	want([]string{"BITCOUNT", "foobar", "5", "30", "BIT"}, "17")
	cl.do(t, "SET", "k1", "foobar")
	cl.do(t, "SET", "k2", "abcdef")
	want([]string{"BITOP", "AND", "d", "k1", "k2"}, "6")
	want([]string{"GET", "d"}, "`bc`ab")

	for _, bad := range [][]string{
		{"SETBIT", "b", "x", "1"}, {"SETBIT", "b", "1", "2"}, {"SETBIT", "b", "4294967296", "1"},
		{"BITCOUNT", "b", "1"}, {"BITOP", "NOT", "d", "k1", "k2"}, {"BITOP", "FOO", "d", "k1"},
	} {
		if r := cl.do(t, bad...); r.Kind != '-' {
			t.Fatalf("%v = %+v want error", bad, r)
		}
	}
	cl.do(t, "SADD", "set", "x")
	if r := cl.do(t, "SETBIT", "set", "1", "1"); r.Kind != '-' {
		t.Fatalf("SETBIT on set = %+v", r)
	}
	cl.do(t, "PFADD", "h", "a")
	// An HLL is a string: bit reads work, bit writes would corrupt it and are refused.
	if r := cl.do(t, "GETBIT", "h", "1"); r.Kind != ':' {
		t.Fatalf("GETBIT on HLL = %+v", r)
	}
	if r := cl.do(t, "SETBIT", "h", "1", "1"); r.Kind != '-' {
		t.Fatalf("SETBIT on HLL = %+v", r)
	}
}
