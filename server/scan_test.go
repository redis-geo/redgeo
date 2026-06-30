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
