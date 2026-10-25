package server

import (
	"fmt"
	"testing"
)

// TestScanPagination inserts many keys and walks SCAN across pages following
// the numeric cursor until it returns to 0, asserting every key is seen.
func TestScanPagination(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	const n = 50
	for i := 0; i < n; i++ {
		cl.do(t, "SET", fmt.Sprintf("key:%02d", i), "v")
	}

	seen := map[string]bool{}
	cursor := "0"
	pages := 0
	for {
		r := cl.do(t, "SCAN", cursor, "COUNT", "10")
		if len(r.Arr) != 2 {
			t.Fatalf("SCAN reply shape = %+v", r)
		}
		cursor = r.Arr[0].Str
		for _, k := range r.Arr[1].Arr {
			seen[k.Str] = true
		}
		pages++
		if cursor == "0" {
			break
		}
		if pages > 100 {
			t.Fatal("SCAN did not terminate")
		}
	}
	if pages < 2 {
		t.Fatalf("expected multiple pages with COUNT 10 over %d keys, got %d", n, pages)
	}
	if len(seen) != n {
		t.Fatalf("SCAN saw %d distinct keys, want %d", len(seen), n)
	}

	// MATCH filter across pages.
	cl.do(t, "SET", "user:1", "a")
	cl.do(t, "SET", "user:2", "b")
	seen = map[string]bool{}
	cursor = "0"
	for {
		r := cl.do(t, "SCAN", cursor, "MATCH", "user:*", "COUNT", "5")
		cursor = r.Arr[0].Str
		for _, k := range r.Arr[1].Arr {
			seen[k.Str] = true
		}
		if cursor == "0" {
			break
		}
	}
	if !seen["user:1"] || !seen["user:2"] || len(seen) != 2 {
		t.Fatalf("SCAN MATCH user:* = %v, want {user:1,user:2}", seen)
	}
}

// TestCollectionScanPagination walks HSCAN/SSCAN/ZSCAN across pages: every
// member is returned, cursors terminate at 0, and MATCH applies across pages.
func TestCollectionScanPagination(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	const n = 45
	for i := 0; i < n; i++ {
		cl.do(t, "HSET", "h", fmt.Sprintf("f%02d", i), fmt.Sprint(i))
		cl.do(t, "SADD", "s", fmt.Sprintf("m%02d", i))
		cl.do(t, "ZADD", "z", fmt.Sprint(n-i), fmt.Sprintf("m%02d", i)) // score order != name order
	}

	walk := func(cmd, key string, step int, extra ...string) (names []string, pages int) {
		cursor := "0"
		for {
			args := append([]string{cmd, key, cursor}, extra...)
			r := cl.do(t, args...)
			if len(r.Arr) != 2 {
				t.Fatalf("%s reply = %+v", cmd, r)
			}
			cursor = r.Arr[0].Str
			for i := 0; i < len(r.Arr[1].Arr); i += step {
				names = append(names, r.Arr[1].Arr[i].Str)
			}
			pages++
			if cursor == "0" {
				return
			}
			if pages > 100 {
				t.Fatalf("%s did not terminate", cmd)
			}
		}
	}
	for _, c := range []struct {
		cmd, key string
		step     int
	}{{"HSCAN", "h", 2}, {"SSCAN", "s", 1}, {"ZSCAN", "z", 2}} {
		names, pages := walk(c.cmd, c.key, c.step, "COUNT", "10")
		if pages != 5 || len(names) != n {
			t.Fatalf("%s: %d pages, %d members; want 5 pages, %d members", c.cmd, pages, len(names), n)
		}
		seen := map[string]bool{}
		for _, nm := range names {
			if seen[nm] {
				t.Fatalf("%s returned %q twice", c.cmd, nm)
			}
			seen[nm] = true
		}
		// MATCH across pages.
		names, _ = walk(c.cmd, c.key, c.step, "COUNT", "3", "MATCH", "*1?")
		if len(names) != 10 { // 10..19
			t.Fatalf("%s MATCH saw %d members, want 10: %v", c.cmd, len(names), names)
		}
	}
	// Default COUNT is 10 (Redis); small collections still finish in one page.
	cl.do(t, "SADD", "small", "a", "b", "c")
	if r := cl.do(t, "SSCAN", "small", "0"); r.Arr[0].Str != "0" || len(r.Arr[1].Arr) != 3 {
		t.Fatalf("small SSCAN = %+v", r)
	}
	// An unknown cursor restarts instead of erroring.
	if r := cl.do(t, "SSCAN", "s", "999999", "COUNT", "5"); len(r.Arr[1].Arr) != 5 {
		t.Fatalf("stale cursor SSCAN = %+v", r)
	}
}
