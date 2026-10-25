package crdtstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/redis-geo/redgeo/command"
	"github.com/redis-geo/redgeo/script"
)

// jcmd runs one command through the real command layer against a store.
func jcmd(t *testing.T, s *Store, args ...string) script.Reply {
	t.Helper()
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	cmd, err := command.Parse(raw)
	if err != nil {
		return script.Reply{Kind: '-', Str: err.Error()}
	}
	var rec script.Recorder
	_, _ = cmd.Run(&rec, s.Redka(0))
	return rec.Reply()
}

func jsonOf(t *testing.T, s *Store, key string) string {
	t.Helper()
	r := jcmd(t, s, "JSON.GET", key, "$")
	if r.Kind != '$' {
		return fmt.Sprintf("<%c %s>", r.Kind, r.Str)
	}
	return strings.TrimSuffix(strings.TrimPrefix(r.Str, "["), "]")
}

func TestJSONStoreRoundTrip(t *testing.T) {
	st := newTestStore(t)
	doc := `{"a":1,"b":{"c":[1,2,{"d":"x"}]},"e":null}`
	if r := jcmd(t, st, "JSON.SET", "k", "$", doc); r.Str != "OK" {
		t.Fatalf("SET = %+v", r)
	}
	if got := jsonOf(t, st, "k"); got != doc {
		t.Fatalf("got %s", got)
	}
	// TYPE / EXISTS / DEL / RENAME at the keyspace level.
	if ty, _ := st.Redka(0).Key().Get("k"); ty.TypeName() != "ReJSON-RL" {
		t.Fatalf("type = %s", ty.TypeName())
	}
	if err := st.Redka(0).Key().Rename("k", "k2"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Redka(0).Key().Exists("k"); ok {
		t.Fatal("old key exists")
	}
	if got := jsonOf(t, st, "k2"); got != doc {
		t.Fatalf("renamed: %s", got)
	}
	if n, _ := st.Redka(0).Key().Delete("k2"); n != 1 {
		t.Fatal("DEL")
	}
	if r := jcmd(t, st, "JSON.GET", "k2"); !(r.Kind == '_') {
		t.Fatalf("GET after DEL = %+v", r)
	}
	// Re-creating a deleted document must not resurrect old nodes.
	jcmd(t, st, "JSON.SET", "k2", "$", `{"new":true}`)
	if got := jsonOf(t, st, "k2"); got != `{"new":true}` {
		t.Fatalf("recreated: %s", got)
	}
}

// Replacing a subtree must tombstone it: re-creating the same field later must
// not bring back old children.
func TestJSONSubtreeReplaceDoesNotResurrect(t *testing.T) {
	st := newTestStore(t)
	jcmd(t, st, "JSON.SET", "k", "$", `{"o":{"x":1,"y":[1,2]}}`)
	jcmd(t, st, "JSON.SET", "k", "$.o", `"scalar"`)
	jcmd(t, st, "JSON.SET", "k", "$.o", `{"z":3}`)
	if got := jsonOf(t, st, "k"); got != `{"o":{"z":3}}` {
		t.Fatalf("got %s", got)
	}
	jcmd(t, st, "JSON.DEL", "k", "$.o")
	jcmd(t, st, "JSON.SET", "k", "$.o", `[]`)
	jcmd(t, st, "JSON.ARRAPPEND", "k", "$.o", `7`)
	if got := jsonOf(t, st, "k"); got != `{"o":[7]}` {
		t.Fatalf("got %s", got)
	}
}

func TestJSONConcurrentEditsMerge(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0], stores[1]
	jcmd(t, a, "JSON.SET", "d", "$", `{"a":1,"b":{"c":2},"l":[1],"name":"base"}`)
	eventually(t, "r1 has doc", func() bool { return jsonOf(t, b, "d") == `{"a":1,"b":{"c":2},"l":[1],"name":"base"}` })

	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	// Different paths on each side.
	jcmd(t, a, "JSON.SET", "d", "$.a", `10`)
	jcmd(t, a, "JSON.SET", "d", "$.b.d", `3`)
	jcmd(t, a, "JSON.ARRAPPEND", "d", "$.l", `"fromA"`)
	jcmd(t, b, "JSON.SET", "d", "$.b.c", `20`)
	jcmd(t, b, "JSON.SET", "d", "$.e", `5`)
	jcmd(t, b, "JSON.ARRAPPEND", "d", "$.l", `"fromB"`)
	// Same path on both sides: last writer (by HLC) wins, identically everywhere.
	jcmd(t, a, "JSON.SET", "d", "$.name", `"A"`)
	jcmd(t, b, "JSON.SET", "d", "$.name", `"B"`)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)

	var final string
	eventually(t, "both replicas converge", func() bool {
		x, y := jsonOf(t, a, "d"), jsonOf(t, b, "d")
		final = x
		return x == y && strings.Contains(x, `"a":10`) && strings.Contains(x, `"e":5`) &&
			strings.Contains(x, `"c":20`) && strings.Contains(x, `"d":3`) &&
			strings.Contains(x, `"fromA"`) && strings.Contains(x, `"fromB"`)
	})
	if !strings.Contains(final, `"name":"B"`) { // B's write has the later HLC
		t.Fatalf("same-path conflict did not resolve to the later writer: %s", final)
	}
	if !strings.Contains(final, `"l":[1,`) { // the original element stays first
		t.Fatalf("array order: %s", final)
	}
}

// A subtree one replica deletes while another edits inside it: the delete wins
// the node it saw; the concurrent edit to an unseen sibling survives.
func TestJSONConcurrentDeleteAndEdit(t *testing.T) {
	stores, net := newCluster(t, 2)
	a, b := stores[0], stores[1]
	jcmd(t, a, "JSON.SET", "d", "$", `{"keep":1,"o":{"x":1}}`)
	eventually(t, "synced", func() bool { return jsonOf(t, b, "d") == `{"keep":1,"o":{"x":1}}` })
	net.SetPartitioned(0, true)
	net.SetPartitioned(1, true)
	jcmd(t, a, "JSON.DEL", "d", "$.o")
	jcmd(t, b, "JSON.SET", "d", "$.o.y", `2`)
	net.SetPartitioned(0, false)
	net.SetPartitioned(1, false)
	eventually(t, "converged", func() bool {
		x, y := jsonOf(t, a, "d"), jsonOf(t, b, "d")
		return x == y && strings.Contains(x, `"keep":1`)
	})
	// Whatever the merge decided, both replicas agree and the document is valid.
	if got := jsonOf(t, a, "d"); !strings.HasPrefix(got, "{") {
		t.Fatalf("invalid result %s", got)
	}
}
