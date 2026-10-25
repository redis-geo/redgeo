// Package jsondoc is an in-memory JSON document tree with JSONPath selection
// and mutation, independent of storage. The crdtstore backend loads a stored
// document into a Doc, lets a command mutate it, and persists the difference.
//
// Every node has a stable identity: an object field is identified by its name
// and an array element by its Pos key (an order-preserving string minted by
// Doc.NewPos). Identity is what lets two replicas edit different parts of one
// document and merge.
package jsondoc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Kind uint8

const (
	Null Kind = iota + 1
	Bool
	Number
	String
	Object
	Array
)

// Node is one JSON value. Scalars keep their text in Raw: the literal for a
// Number (preserved exactly), "true"/"false" for Bool, the decoded text for a
// String.
type Node struct {
	Kind   Kind
	Raw    string
	Fields map[string]*Node // Object
	Elems  []*Elem          // Array, ordered by Pos
}

// Elem is an array element with its identity.
type Elem struct {
	Pos  string
	Node *Node
}

// Doc is a document. Root is nil when the document does not exist yet.
type Doc struct {
	Root *Node
	// NewPos returns a position key strictly between prev and next ("" = open
	// end). The backend supplies a replica-unique implementation.
	NewPos func(prev, next string) string
}

// ---- parsing / encoding ----

// Parse decodes JSON text into a Node, minting array positions via d.NewPos.
func (d *Doc) Parse(text []byte) (*Node, error) {
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	n, err := d.parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("unexpected data after JSON value")
	}
	return n, nil
}

func (d *Doc) parseValue(dec *json.Decoder) (*Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &Node{Kind: Object, Fields: map[string]*Node{}}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := d.parseValue(dec)
				if err != nil {
					return nil, err
				}
				n.Fields[k.(string)] = v
			}
			_, err := dec.Token()
			return n, err
		case '[':
			n := &Node{Kind: Array}
			prev := ""
			for dec.More() {
				v, err := d.parseValue(dec)
				if err != nil {
					return nil, err
				}
				pos := d.NewPos(prev, "")
				n.Elems = append(n.Elems, &Elem{Pos: pos, Node: v})
				prev = pos
			}
			_, err := dec.Token()
			return n, err
		}
	case string:
		return &Node{Kind: String, Raw: t}, nil
	case json.Number:
		return &Node{Kind: Number, Raw: t.String()}, nil
	case bool:
		return &Node{Kind: Bool, Raw: strconv.FormatBool(t)}, nil
	case nil:
		return &Node{Kind: Null}, nil
	}
	return nil, errors.New("invalid JSON")
}

// Encode serializes n as compact JSON; object fields are sorted by name.
func Encode(n *Node) string {
	var b strings.Builder
	encode(&b, n)
	return b.String()
}

func encode(b *strings.Builder, n *Node) {
	switch n.Kind {
	case Null:
		b.WriteString("null")
	case Bool, Number:
		b.WriteString(n.Raw)
	case String:
		b.WriteString(quote(n.Raw))
	case Object:
		names := make([]string, 0, len(n.Fields))
		for k := range n.Fields {
			names = append(names, k)
		}
		sort.Strings(names)
		b.WriteByte('{')
		for i, k := range names {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(quote(k))
			b.WriteByte(':')
			encode(b, n.Fields[k])
		}
		b.WriteByte('}')
	case Array:
		b.WriteByte('[')
		for i, e := range n.Elems {
			if i > 0 {
				b.WriteByte(',')
			}
			encode(b, e.Node)
		}
		b.WriteByte(']')
	}
}

func quote(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimRight(buf.String(), "\n")
}

// TypeName is the RedisJSON type name of a node.
func (n *Node) TypeName() string {
	switch n.Kind {
	case Null:
		return "null"
	case Bool:
		return "boolean"
	case Number:
		if strings.ContainsAny(n.Raw, ".eE") {
			return "number"
		}
		return "integer"
	case String:
		return "string"
	case Object:
		return "object"
	}
	return "array"
}

// Clone deep-copies a node (keeping array positions).
func (n *Node) Clone() *Node {
	c := &Node{Kind: n.Kind, Raw: n.Raw}
	if n.Fields != nil {
		c.Fields = make(map[string]*Node, len(n.Fields))
		for k, v := range n.Fields {
			c.Fields[k] = v.Clone()
		}
	}
	for _, e := range n.Elems {
		c.Elems = append(c.Elems, &Elem{Pos: e.Pos, Node: e.Node.Clone()})
	}
	return c
}

// FieldNames returns an object's field names, sorted.
func (n *Node) FieldNames() []string {
	names := make([]string, 0, len(n.Fields))
	for k := range n.Fields {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ---- paths ----

type stepKind uint8

const (
	stField stepKind = iota + 1
	stIndex
	stWildcard
	stRecField    // ..name
	stRecWildcard // ..*
)

type step struct {
	kind stepKind
	name string
	idx  int
}

// Path is a parsed JSONPath (the common subset: $, .name, ['name'], [n], [-n],
// .*, [*], ..name, ..*). Legacy paths (".a.b", "a.b") are accepted and flagged.
type Path struct {
	steps  []step
	Legacy bool // not rooted at "$": replies are single values, not arrays
	Text   string
}

// ParsePath parses a RedisJSON path.
func ParsePath(s string) (*Path, error) {
	p := &Path{Text: s}
	switch {
	case s == "$" || strings.HasPrefix(s, "$.") || strings.HasPrefix(s, "$["):
		s = s[1:]
	case s == "." || s == "":
		p.Legacy, s = true, ""
	case strings.HasPrefix(s, "."):
		p.Legacy = true
	default:
		p.Legacy, s = true, "."+s
	}
	for len(s) > 0 {
		switch s[0] {
		case '.':
			rec := strings.HasPrefix(s, "..")
			s = strings.TrimPrefix(s, ".")
			if rec {
				s = s[1:]
			}
			if s == "" {
				return nil, errors.New("path ends with '.'")
			}
			if s[0] == '[' { // "..[...]": only the wildcard form is useful
				if !strings.HasPrefix(s, "[*]") {
					return nil, fmt.Errorf("unsupported path %q", p.Text)
				}
				p.steps = append(p.steps, step{kind: stRecWildcard})
				s = s[3:]
				continue
			}
			end := strings.IndexAny(s, ".[")
			if end < 0 {
				end = len(s)
			}
			name := s[:end]
			s = s[end:]
			switch {
			case name == "*" && rec:
				p.steps = append(p.steps, step{kind: stRecWildcard})
			case name == "*":
				p.steps = append(p.steps, step{kind: stWildcard})
			case rec:
				p.steps = append(p.steps, step{kind: stRecField, name: name})
			default:
				p.steps = append(p.steps, step{kind: stField, name: name})
			}
		case '[':
			end := strings.IndexByte(s, ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated '[' in path %q", p.Text)
			}
			inner := s[1:end]
			s = s[end+1:]
			switch {
			case inner == "*":
				p.steps = append(p.steps, step{kind: stWildcard})
			case len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0]:
				p.steps = append(p.steps, step{kind: stField, name: inner[1 : len(inner)-1]})
			default:
				i, err := strconv.Atoi(inner)
				if err != nil {
					return nil, fmt.Errorf("unsupported path expression [%s]", inner)
				}
				p.steps = append(p.steps, step{kind: stIndex, idx: i})
			}
		default:
			return nil, fmt.Errorf("invalid path %q", p.Text)
		}
	}
	return p, nil
}

// IsRoot reports whether the path selects only the document root.
func (p *Path) IsRoot() bool { return len(p.steps) == 0 }

// Ref is a selected node along with how to reach and replace it.
type Ref struct {
	Node   *Node
	Parent *Node
	Field  string // when Parent is an Object
	Elem   *Elem  // when Parent is an Array
	Path   string // canonical JSONPath of this node
}

// Select returns every node the path matches, in document order.
func (d *Doc) Select(p *Path) []Ref {
	if d.Root == nil {
		return nil
	}
	cur := []Ref{{Node: d.Root, Path: "$"}}
	for _, st := range p.steps {
		var next []Ref
		for _, r := range cur {
			next = append(next, expand(r, st)...)
		}
		cur = next
	}
	return cur
}

func fieldRef(r Ref, name string) (Ref, bool) {
	if r.Node.Kind != Object {
		return Ref{}, false
	}
	c, ok := r.Node.Fields[name]
	if !ok {
		return Ref{}, false
	}
	return Ref{Node: c, Parent: r.Node, Field: name, Path: r.Path + "['" + name + "']"}, true
}

func elemRefs(r Ref) []Ref {
	if r.Node.Kind != Array {
		return nil
	}
	out := make([]Ref, len(r.Node.Elems))
	for i, e := range r.Node.Elems {
		out[i] = Ref{Node: e.Node, Parent: r.Node, Elem: e, Path: r.Path + "[" + strconv.Itoa(i) + "]"}
	}
	return out
}

func children(r Ref) []Ref {
	switch r.Node.Kind {
	case Object:
		var out []Ref
		for _, k := range r.Node.FieldNames() {
			c, _ := fieldRef(r, k)
			out = append(out, c)
		}
		return out
	case Array:
		return elemRefs(r)
	}
	return nil
}

func expand(r Ref, st step) []Ref {
	switch st.kind {
	case stField:
		if c, ok := fieldRef(r, st.name); ok {
			return []Ref{c}
		}
	case stIndex:
		els := elemRefs(r)
		i := st.idx
		if i < 0 {
			i += len(els)
		}
		if i >= 0 && i < len(els) {
			return []Ref{els[i]}
		}
	case stWildcard:
		return children(r)
	case stRecField, stRecWildcard:
		var out []Ref
		var walk func(Ref)
		walk = func(x Ref) {
			if st.kind == stRecField {
				if c, ok := fieldRef(x, st.name); ok {
					out = append(out, c)
				}
			}
			for _, c := range children(x) {
				if st.kind == stRecWildcard {
					out = append(out, c)
				}
				walk(c)
			}
		}
		walk(r)
		return out
	}
	return nil
}

// ---- mutation ----

// Replace overwrites the selected node in place (so parent links stay valid).
func (r Ref) Replace(n *Node) { *r.Node = *n }

// Delete removes the selected node from its parent (or the whole document for
// the root). It reports whether anything was removed.
func (d *Doc) Delete(r Ref) bool {
	switch {
	case r.Parent == nil:
		d.Root = nil
	case r.Elem != nil:
		for i, e := range r.Parent.Elems {
			if e == r.Elem {
				r.Parent.Elems = append(r.Parent.Elems[:i], r.Parent.Elems[i+1:]...)
				return true
			}
		}
		return false
	default:
		delete(r.Parent.Fields, r.Field)
	}
	return true
}

// Set stores n at every node the path matches; when nothing matches and the
// path ends in an object field, the field is created on each matching parent
// object. Each target gets its own copy. It returns how many nodes were set.
func (d *Doc) Set(p *Path, n *Node) int {
	if p.IsRoot() {
		d.Root = n
		return 1
	}
	if refs := d.Select(p); len(refs) > 0 {
		for _, r := range refs {
			r.Replace(n.Clone())
		}
		return len(refs)
	}
	last := p.steps[len(p.steps)-1]
	if last.kind != stField {
		return 0
	}
	count := 0
	for _, par := range d.Select(&Path{steps: p.steps[:len(p.steps)-1]}) {
		if par.Node.Kind == Object {
			par.Node.Fields[last.name] = n.Clone()
			count++
		}
	}
	return count
}

// AppendElem adds n at the end of arr.
func (d *Doc) AppendElem(arr *Node, n *Node) {
	prev := ""
	if k := len(arr.Elems); k > 0 {
		prev = arr.Elems[k-1].Pos
	}
	arr.Elems = append(arr.Elems, &Elem{Pos: d.NewPos(prev, ""), Node: n})
}

// InsertElems inserts nodes before index i (0..len) of arr.
func (d *Doc) InsertElems(arr *Node, i int, nodes []*Node) {
	prev := ""
	if i > 0 {
		prev = arr.Elems[i-1].Pos
	}
	next := ""
	if i < len(arr.Elems) {
		next = arr.Elems[i].Pos
	}
	added := make([]*Elem, 0, len(nodes))
	for _, n := range nodes {
		pos := d.NewPos(prev, next)
		added = append(added, &Elem{Pos: pos, Node: n})
		prev = pos
	}
	tail := append([]*Elem(nil), arr.Elems[i:]...)
	arr.Elems = append(append(arr.Elems[:i], added...), tail...)
}
