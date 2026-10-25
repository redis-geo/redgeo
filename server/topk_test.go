package server

import (
	"strings"
	"testing"
)

func TestTopKCommands(t *testing.T) {
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
	// arr renders an array reply; nils appear as "nil".
	arr := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind == '-' {
			return "ERR:" + r.Str
		}
		var out []string
		for _, e := range r.Arr {
			if e.Null {
				out = append(out, "nil")
			} else {
				out = append(out, e.Str)
			}
		}
		return strings.Join(out, " ")
	}
	isErr := func(sub string, args ...string) {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind != '-' || !strings.Contains(r.Str, sub) {
			t.Fatalf("%v = %+v want error containing %q", args, r, sub)
		}
	}

	want("OK", "TOPK.RESERVE", "t", "2")
	isErr("already exists", "TOPK.RESERVE", "t", "2")
	want("OK", "TOPK.RESERVE", "t2", "5", "20", "5", "0.8")
	if got := arr("TOPK.INFO", "t"); got != "k 2 width 8 depth 7 decay 0.9" {
		t.Fatalf("INFO defaults = %q", got)
	}
	if got := arr("TOPK.INFO", "t2"); got != "k 5 width 20 depth 5 decay 0.8" {
		t.Fatalf("INFO = %q", got)
	}

	if got := arr("TOPK.ADD", "t", "a", "a", "b"); got != "nil nil nil" {
		t.Fatalf("ADD = %q", got)
	}
	if got := arr("TOPK.INCRBY", "t", "c", "10"); got != "b" { // c pushes b out of the top 2
		t.Fatalf("INCRBY = %q", got)
	}
	if got := arr("TOPK.LIST", "t"); got != "c a" {
		t.Fatalf("LIST = %q", got)
	}
	if got := arr("TOPK.LIST", "t", "WITHCOUNT"); got != "c 10 a 2" {
		t.Fatalf("LIST WITHCOUNT = %q", got)
	}
	if got := arr("TOPK.QUERY", "t", "a", "b", "zzz"); got != "1 0 0" {
		t.Fatalf("QUERY = %q", got)
	}
	if got := arr("TOPK.COUNT", "t", "a", "b", "zzz"); got != "2 1 0" {
		t.Fatalf("COUNT = %q", got)
	}

	for _, c := range []struct {
		sub  string
		args []string
	}{
		{"does not exist", []string{"TOPK.ADD", "nokey", "x"}},
		{"does not exist", []string{"TOPK.LIST", "nokey"}},
		{"does not exist", []string{"TOPK.QUERY", "nokey", "x"}},
		{"does not exist", []string{"TOPK.COUNT", "nokey", "x"}},
		{"does not exist", []string{"TOPK.INFO", "nokey"}},
		{"invalid k", []string{"TOPK.RESERVE", "z", "0"}},
		{"invalid k", []string{"TOPK.RESERVE", "z", "x"}},
		{"width", []string{"TOPK.RESERVE", "z", "3", "0", "7", "0.9"}},
		{"depth", []string{"TOPK.RESERVE", "z", "3", "8", "0", "0.9"}},
		{"decay", []string{"TOPK.RESERVE", "z", "3", "8", "7", "2"}},
		{"", []string{"TOPK.RESERVE", "z", "3", "8"}},
		{"increment", []string{"TOPK.INCRBY", "t", "a", "0"}},
		{"increment", []string{"TOPK.INCRBY", "t", "a", "100001"}},
		{"", []string{"TOPK.INCRBY", "t", "a"}},
		{"", []string{"TOPK.ADD", "t"}},
		{"", []string{"TOPK.LIST", "t", "BOGUS"}},
	} {
		isErr(c.sub, c.args...)
	}
	isErr("", "BF.ADD", "t", "x") // not a Bloom filter
	isErr("", "CMS.QUERY", "t", "x")

	want("TopK-TYPE", "TYPE", "t")
	want("OK", "RENAME", "t", "t3")
	if got := arr("TOPK.LIST", "t3"); got != "c a" {
		t.Fatalf("after RENAME = %q", got)
	}
	want("1", "DEL", "t3")
	if r := cl.do(t, "SCAN", "0", "TYPE", "topk-type", "COUNT", "100"); len(r.Arr[1].Arr) != 1 || r.Arr[1].Arr[0].Str != "t2" {
		t.Fatalf("SCAN TYPE = %+v", r)
	}
	cl.do(t, "MULTI")
	cl.do(t, "TOPK.ADD", "t2", "q")
	cl.do(t, "TOPK.COUNT", "t2", "q")
	if r := cl.do(t, "EXEC"); len(r.Arr) != 2 || r.Arr[1].Arr[0].Str != "1" {
		t.Fatalf("EXEC = %+v", r)
	}
}
