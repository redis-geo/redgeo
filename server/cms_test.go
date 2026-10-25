package server

import (
	"strings"
	"testing"
)

func TestCMSCommands(t *testing.T) {
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
	isErr := func(sub string, args ...string) {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind != '-' || !strings.Contains(r.Str, sub) {
			t.Fatalf("%v = %+v want error containing %q", args, r, sub)
		}
	}

	want("OK", "CMS.INITBYDIM", "a", "1000", "5")
	isErr("already exists", "CMS.INITBYDIM", "a", "1000", "5")
	want("OK", "CMS.INITBYPROB", "p", "0.001", "0.01")
	r := cl.do(t, "CMS.INFO", "p")
	if len(r.Arr) != 6 || r.Arr[0].Str != "width" || r.Arr[2].Str != "depth" || r.Arr[4].Str != "count" {
		t.Fatalf("INFO = %+v", r)
	}
	// width = ceil(2/0.001) = 2000, depth = ceil(log2(1/0.01)) = 7
	if r.Arr[1].Str != "2000" || r.Arr[3].Str != "7" || r.Arr[5].Str != "0" {
		t.Fatalf("INITBYPROB dimensions = %+v", r)
	}
	if got := ints("CMS.INCRBY", "a", "apple", "5", "pear", "2", "apple", "1"); got != "5 2 6" {
		t.Fatalf("INCRBY = %q", got)
	}
	if got := ints("CMS.QUERY", "a", "apple", "pear", "none"); got != "6 2 0" {
		t.Fatalf("QUERY = %q", got)
	}
	r = cl.do(t, "CMS.INFO", "a")
	if r.Arr[5].Str != "8" {
		t.Fatalf("count = %+v", r)
	}

	want("OK", "CMS.INITBYDIM", "b", "1000", "5")
	cl.do(t, "CMS.INCRBY", "b", "apple", "10")
	want("OK", "CMS.INITBYDIM", "m", "1000", "5")
	want("OK", "CMS.MERGE", "m", "2", "a", "b")
	if got := ints("CMS.QUERY", "m", "apple", "pear"); got != "16 2" {
		t.Fatalf("MERGE = %q", got)
	}
	want("OK", "CMS.MERGE", "m", "2", "a", "b", "WEIGHTS", "2", "1")
	if got := ints("CMS.QUERY", "m", "apple"); got != "22" {
		t.Fatalf("weighted MERGE = %q", got)
	}

	for _, c := range []struct {
		sub  string
		args []string
	}{
		{"does not exist", []string{"CMS.INCRBY", "nokey", "x", "1"}},
		{"does not exist", []string{"CMS.QUERY", "nokey", "x"}},
		{"does not exist", []string{"CMS.INFO", "nokey"}},
		{"does not exist", []string{"CMS.MERGE", "nokey", "1", "a"}},
		{"does not exist", []string{"CMS.MERGE", "m", "1", "nokey"}},
		{"doesn't match", []string{"CMS.MERGE", "m", "1", "p"}},
		{"positive", []string{"CMS.INCRBY", "a", "x", "0"}},
		{"positive", []string{"CMS.INCRBY", "a", "x", "abc"}},
		{"", []string{"CMS.INCRBY", "a", "x"}},
		{"", []string{"CMS.INCRBY", "a"}},
		{"width", []string{"CMS.INITBYDIM", "z", "0", "5"}},
		{"depth", []string{"CMS.INITBYDIM", "z", "5", "x"}},
		{"overestimation", []string{"CMS.INITBYPROB", "z", "2", "0.5"}},
		{"prob", []string{"CMS.INITBYPROB", "z", "0.1", "0"}},
		{"numKeys", []string{"CMS.MERGE", "m", "5", "a"}},
		{"weights", []string{"CMS.MERGE", "m", "2", "a", "b", "WEIGHTS", "1"}},
		{"weight", []string{"CMS.MERGE", "m", "1", "a", "WEIGHTS", "x"}},
	} {
		isErr(c.sub, c.args...)
	}
	cl.do(t, "SET", "str", "v")
	isErr("", "CMS.QUERY", "str", "x")
	isErr("", "BF.ADD", "a", "x") // a count-min key is not a Bloom filter

	want("CMSk-TYPE", "TYPE", "a")
	want("OK", "RENAME", "a", "a2")
	if got := ints("CMS.QUERY", "a2", "apple"); got != "6" {
		t.Fatalf("after RENAME = %q", got)
	}
	cl.do(t, "MULTI")
	cl.do(t, "CMS.INCRBY", "a2", "apple", "4")
	cl.do(t, "CMS.QUERY", "a2", "apple")
	if r := cl.do(t, "EXEC"); len(r.Arr) != 2 || r.Arr[1].Arr[0].Str != "10" {
		t.Fatalf("EXEC = %+v", r)
	}
}
