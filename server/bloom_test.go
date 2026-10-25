package server

import (
	"fmt"
	"strings"
	"testing"
)

func TestBloomCommands(t *testing.T) {
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
	ints := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind == '-' {
			return "ERR:" + r.Str
		}
		var out []string
		for _, e := range r.Arr {
			out = append(out, e.Str)
		}
		return strings.Join(out, " ")
	}
	isErr := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind != '-' {
			t.Fatalf("%v = %+v want error", args, r)
		}
		return r.Str
	}

	want("OK", "BF.RESERVE", "bf", "0.01", "1000")
	if e := isErr("BF.RESERVE", "bf", "0.01", "1000"); !strings.Contains(e, "item exists") {
		t.Fatalf("duplicate reserve: %s", e)
	}
	want("1", "BF.ADD", "bf", "hello")
	want("0", "BF.ADD", "bf", "hello")
	want("1", "BF.EXISTS", "bf", "hello")
	want("0", "BF.EXISTS", "bf", "absent-item-xyz")
	if got := ints("BF.MADD", "bf", "a", "b", "hello"); got != "1 1 0" {
		t.Fatalf("MADD = %q", got)
	}
	if got := ints("BF.MEXISTS", "bf", "a", "nope-nope", "b"); got != "1 0 1" {
		t.Fatalf("MEXISTS = %q", got)
	}
	want("3", "BF.CARD", "bf")
	want("0", "BF.CARD", "missing")
	want("0", "BF.EXISTS", "missing", "x")
	want("MBbloom--", "TYPE", "bf")

	// BF.INFO: full map, single fields, errors.
	r := cl.do(t, "BF.INFO", "bf")
	if len(r.Arr) != 10 || r.Arr[0].Str != "Capacity" || r.Arr[1].Str != "1000" || r.Arr[5].Str != "1" || r.Arr[7].Str != "3" || r.Arr[9].Str != "2" {
		t.Fatalf("BF.INFO = %+v", r)
	}
	want("1000", "BF.INFO", "bf", "CAPACITY")
	want("3", "BF.INFO", "bf", "ITEMS")
	want("1", "BF.INFO", "bf", "FILTERS")
	want("2", "BF.INFO", "bf", "EXPANSION")
	isErr("BF.INFO", "bf", "BOGUS")
	if e := isErr("BF.INFO", "missing"); !strings.Contains(e, "not found") {
		t.Fatalf("INFO missing: %s", e)
	}

	// Auto-creation (defaults: error 0.01, capacity 100) and BF.INSERT options.
	want("1", "BF.ADD", "auto", "x")
	want("100", "BF.INFO", "auto", "CAPACITY")
	if got := ints("BF.INSERT", "ins", "CAPACITY", "500", "ERROR", "0.001", "ITEMS", "p", "q", "p"); got != "1 1 0" {
		t.Fatalf("INSERT = %q", got)
	}
	want("500", "BF.INFO", "ins", "CAPACITY")
	if e := isErr("BF.INSERT", "nope", "NOCREATE", "ITEMS", "a"); !strings.Contains(e, "not found") {
		t.Fatalf("NOCREATE: %s", e)
	}
	want("OK", "BF.RESERVE", "ns", "0.01", "10", "NONSCALING")
	if r := cl.do(t, "BF.INFO", "ns", "EXPANSION"); !r.Null {
		t.Fatalf("non-scaling expansion = %+v", r)
	}
	var many []string
	for i := 0; i < 30; i++ {
		many = append(many, fmt.Sprint("item", i))
	}
	if e := isErr(append([]string{"BF.MADD", "ns"}, many...)...); !strings.Contains(e, "full") {
		t.Fatalf("full non-scaling filter: %s", e)
	}
	want("OK", "BF.RESERVE", "ex", "0.01", "10", "EXPANSION", "4")
	want("4", "BF.INFO", "ex", "EXPANSION")

	// Argument validation.
	for _, bad := range [][]string{
		{"BF.RESERVE", "k", "0", "100"}, {"BF.RESERVE", "k", "1", "100"}, {"BF.RESERVE", "k", "0.1", "0"},
		{"BF.RESERVE", "k", "x", "100"}, {"BF.RESERVE", "k", "0.1", "x"}, {"BF.RESERVE", "k", "0.1", "10", "BOGUS"},
		{"BF.ADD", "k"}, {"BF.ADD", "k", "a", "b"}, {"BF.MADD", "k"}, {"BF.EXISTS", "k"}, {"BF.INSERT", "k", "CAPACITY", "5"},
		{"BF.INSERT", "k", "ITEMS"},
	} {
		isErr(bad...)
	}
	cl.do(t, "SET", "str", "v")
	isErr("BF.ADD", "str", "x")
	isErr("BF.EXISTS", "str", "x")

	// Keyspace integration: RENAME, EXPIRE, DEL, SCAN TYPE.
	want("OK", "RENAME", "bf", "bf2")
	want("1", "BF.EXISTS", "bf2", "hello")
	want("1", "EXPIRE", "bf2", "100")
	if r := cl.do(t, "SCAN", "0", "TYPE", "MBbloom--", "COUNT", "100"); len(r.Arr[1].Arr) < 4 {
		t.Fatalf("SCAN TYPE = %+v", r)
	}
	want("1", "DEL", "bf2")
	want("0", "BF.EXISTS", "bf2", "hello")

	// Inside MULTI and a Lua script.
	cl.do(t, "MULTI")
	cl.do(t, "BF.ADD", "tx", "one")
	cl.do(t, "BF.EXISTS", "tx", "one")
	if r := cl.do(t, "EXEC"); len(r.Arr) != 2 || r.Arr[0].Str != "1" || r.Arr[1].Str != "1" {
		t.Fatalf("EXEC = %+v", r)
	}
	want("1", "EVAL", "return redis.call('BF.EXISTS', KEYS[1], ARGV[1])", "1", "tx", "one")
}
