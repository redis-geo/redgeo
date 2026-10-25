package script

import (
	"reflect"
	"testing"
)

func TestRecorderNesting(t *testing.T) {
	var w Recorder
	w.WriteArray(3)
	w.WriteInt(1)
	w.WriteArray(2) // last-but-one child opens a nested array
	w.WriteBulkString("a")
	w.WriteArray(1)
	w.WriteNull()
	w.WriteArray(0)
	want := Reply{Kind: '*', Arr: []Reply{
		{Kind: ':', Int: 1},
		{Kind: '*', Arr: []Reply{{Kind: '$', Str: "a"}, {Kind: '*', Arr: []Reply{{Kind: '_'}}}}},
		{Kind: '*'},
	}}
	if got := w.Reply(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}

	// A nested array that is the parent's final child.
	var w2 Recorder
	w2.WriteArray(1)
	w2.WriteArray(2)
	w2.WriteInt(7)
	w2.WriteString("ok")
	if r := w2.Reply(); len(r.Arr) != 1 || len(r.Arr[0].Arr) != 2 {
		t.Fatalf("final-child nesting = %+v", r)
	}
	if r := (&Recorder{}).Reply(); r.Kind != '_' {
		t.Fatalf("empty recorder = %+v", r)
	}
}

func TestEngineLoadAndErrors(t *testing.T) {
	e := NewEngine()
	sha, err := e.Load("return KEYS[1]")
	if err != nil || sha != SHA1("return KEYS[1]") || !e.Exists(sha) {
		t.Fatalf("load: %v %v", sha, err)
	}
	if _, err := e.Load("return +"); err == nil {
		t.Fatal("syntax error accepted")
	}
	r, err := e.Run(sha, [][]byte{[]byte("k")}, nil, nil)
	if err != nil || r.Kind != '$' || r.Str != "k" {
		t.Fatalf("run = %+v %v", r, err)
	}
	if _, err := e.Run("0000", nil, nil, nil); err != ErrNoScript {
		t.Fatalf("unknown sha: %v", err)
	}
	e.Flush()
	if e.Exists(sha) {
		t.Fatal("flush kept script")
	}
}
