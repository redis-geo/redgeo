package jsondoc

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// testPos mints fixed-width decimal positions so they sort as strings.
func testPos(prev, next string) string {
	f := func(s string) (float64, bool) {
		v, err := strconv.ParseFloat(s, 64)
		return v, err == nil && s != ""
	}
	lo, hasLo := f(prev)
	hi, hasHi := f(next)
	var p float64
	switch {
	case hasLo && hasHi:
		p = (lo + hi) / 2
	case hasLo:
		p = lo + 1
	case hasHi:
		p = hi - 1
	default:
		p = 1000
	}
	return fmt.Sprintf("%020.8f", p)
}

func newDoc(t *testing.T, text string) *Doc {
	t.Helper()
	d := &Doc{NewPos: testPos}
	n, err := d.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	d.Root = n
	return d
}

func sel(t *testing.T, d *Doc, path string) []string {
	t.Helper()
	p, err := ParsePath(path)
	if err != nil {
		t.Fatalf("ParsePath(%q): %v", path, err)
	}
	var out []string
	for _, r := range d.Select(p) {
		out = append(out, Encode(r.Node))
	}
	return out
}

func TestParseEncode(t *testing.T) {
	in := `{"b":[1,2.50,"x\"y",null,true,{"z":false}],"a":{"n":-1e3},"s":"héllo <&>"}`
	d := newDoc(t, in)
	want := `{"a":{"n":-1e3},"b":[1,2.50,"x\"y",null,true,{"z":false}],"s":"héllo <&>"}` // keys sorted, numbers verbatim, no HTML escaping
	if got := Encode(d.Root); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	for _, bad := range []string{``, `{`, `[1,`, `{"a":1} x`, `nope`, `{"a" 1}`} {
		if _, err := (&Doc{NewPos: testPos}).Parse([]byte(bad)); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	for text, typ := range map[string]string{`1`: "integer", `1.5`: "number", `1e2`: "number", `"s"`: "string", `[]`: "array", `{}`: "object", `null`: "null", `true`: "boolean"} {
		if got := newDoc(t, text).Root.TypeName(); got != typ {
			t.Fatalf("%s type = %s want %s", text, got, typ)
		}
	}
}

func TestSelect(t *testing.T) {
	d := newDoc(t, `{"a":{"b":[10,20,30],"c":"x"},"d":[{"b":1},{"b":2}],"e":null}`)
	for path, want := range map[string]string{
		`$`:            `{"a":{"b":[10,20,30],"c":"x"},"d":[{"b":1},{"b":2}],"e":null}`,
		`$.a.c`:        `"x"`,
		`$.a.b[1]`:     `20`,
		`$.a.b[-1]`:    `30`,
		`$['a']['c']`:  `"x"`,
		`$.a.b[*]`:     `10 20 30`,
		`$.d[*].b`:     `1 2`,
		`$..b`:         `[10,20,30] 1 2`,
		`$.d[0]`:       `{"b":1}`,
		`$.a.*`:        `[10,20,30] "x"`,
		`$.nope`:       ``,
		`$.a.b[9]`:     ``,
		`$.a.c.deeper`: ``,
		`.a.c`:         `"x"`, // legacy forms
		`a.c`:          `"x"`,
		`.`:            `{"a":{"b":[10,20,30],"c":"x"},"d":[{"b":1},{"b":2}],"e":null}`,
	} {
		if got := strings.Join(sel(t, d, path), " "); got != want {
			t.Errorf("%s = %q want %q", path, got, want)
		}
	}
	for _, bad := range []string{`$.a[`, `$.a[?(@.b>1)]`, `$.a[1:2]`, `$.`} {
		if _, err := ParsePath(bad); err == nil {
			t.Errorf("ParsePath(%q) accepted", bad)
		}
	}
	if p, _ := ParsePath("$.a"); p.Legacy {
		t.Error("$ path flagged legacy")
	}
	if p, _ := ParsePath(".a"); !p.Legacy {
		t.Error(".a not flagged legacy")
	}
}

func TestMutations(t *testing.T) {
	d := newDoc(t, `{"a":{"x":1},"l":[1,2,3]}`)
	set := func(path, val string) int {
		p, _ := ParsePath(path)
		n, err := d.Parse([]byte(val))
		if err != nil {
			t.Fatal(err)
		}
		return d.Set(p, n)
	}
	if set("$.a.y", `2`) != 1 || set("$.l[1]", `"two"`) != 1 {
		t.Fatal("set")
	}
	if set("$.missing.deep", `1`) != 0 || set("$.l[9]", `1`) != 0 {
		t.Fatal("set created something it should not")
	}
	if set("$..x", `0`) != 1 { // recursive selector replaces matches
		t.Fatal("recursive set")
	}
	want := `{"a":{"x":0,"y":2},"l":[1,"two",3]}`
	if got := Encode(d.Root); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	// Set copies per target: mutating one must not alias another.
	d2 := newDoc(t, `{"a":[0,0]}`)
	p, _ := ParsePath("$.a[*]")
	n, _ := d2.Parse([]byte(`{"k":1}`))
	d2.Set(p, n)
	d2.Root.Fields["a"].Elems[0].Node.Fields["k"].Raw = "9"
	if got := Encode(d2.Root); got != `{"a":[{"k":9},{"k":1}]}` {
		t.Fatalf("aliased copies: %s", got)
	}

	// Delete.
	p, _ = ParsePath("$.l[0]")
	for _, r := range d.Select(p) {
		d.Delete(r)
	}
	p, _ = ParsePath("$.a.y")
	for _, r := range d.Select(p) {
		d.Delete(r)
	}
	if got := Encode(d.Root); got != `{"a":{"x":0},"l":["two",3]}` {
		t.Fatalf("after delete: %s", got)
	}
	p, _ = ParsePath("$")
	d.Delete(d.Select(p)[0])
	if d.Root != nil {
		t.Fatal("root delete")
	}
}

func TestArrayInsertKeepsOrderAndIdentity(t *testing.T) {
	d := newDoc(t, `[1,2,5]`)
	before := []string{d.Root.Elems[0].Pos, d.Root.Elems[1].Pos, d.Root.Elems[2].Pos}
	mk := func(s string) *Node { n, _ := d.Parse([]byte(s)); return n }
	d.InsertElems(d.Root, 2, []*Node{mk("3"), mk("4")})
	d.InsertElems(d.Root, 0, []*Node{mk("0")})
	d.AppendElem(d.Root, mk("6"))
	if got := Encode(d.Root); got != `[0,1,2,3,4,5,6]` {
		t.Fatalf("got %s", got)
	}
	// Positions are strictly increasing and the originals kept their identity.
	for i := 1; i < len(d.Root.Elems); i++ {
		if d.Root.Elems[i].Pos <= d.Root.Elems[i-1].Pos {
			t.Fatalf("positions not increasing at %d: %v", i, d.Root.Elems[i-1].Pos)
		}
	}
	if d.Root.Elems[1].Pos != before[0] || d.Root.Elems[2].Pos != before[1] || d.Root.Elems[5].Pos != before[2] {
		t.Fatal("existing elements changed identity")
	}
}

func TestClone(t *testing.T) {
	d := newDoc(t, `{"a":[1,{"b":2}]}`)
	c := d.Root.Clone()
	c.Fields["a"].Elems[1].Node.Fields["b"].Raw = "9"
	if Encode(d.Root) != `{"a":[1,{"b":2}]}` || Encode(c) != `{"a":[1,{"b":9}]}` {
		t.Fatalf("clone shares state: %s / %s", Encode(d.Root), Encode(c))
	}
}
