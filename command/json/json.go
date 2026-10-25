// Package json implements the RedisJSON command subset (JSON.SET, JSON.GET, …)
// on the RJSON storage interface. Paths use the common JSONPath subset in
// package jsondoc; a path starting with "$" yields array-of-matches replies
// (RedisJSON v2), any other path yields a single value (legacy).
package json

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/jsondoc"
	redis "github.com/redis-geo/redgeo/redisapi"
)

var (
	errNoKey     = errors.New("ERR could not perform this operation on a key that doesn't exist")
	errWrongType = errors.New("ERR wrong type of path value")
	errRange     = errors.New("ERR index out of range")
)

func errPath(p *jsondoc.Path) error { return fmt.Errorf("ERR Path '%s' does not exist", p.Text) }

func parsePath(s string) (*jsondoc.Path, error) {
	p, err := jsondoc.ParsePath(s)
	if err != nil {
		return nil, fmt.Errorf("ERR %v", err)
	}
	return p, nil
}

// item is one per-match result.
type item struct {
	null bool
	i    int64
	s    string
	str  bool // s is a bulk string
	arr  []string
	isAr bool
}

func nullItem() item          { return item{null: true} }
func intItem(n int) item      { return item{i: int64(n)} }
func strItem(s string) item   { return item{s: s, str: true} }
func arrItem(a []string) item { return item{arr: a, isAr: true} }

func writeItem(w redis.Writer, it item) {
	switch {
	case it.null:
		w.WriteNull()
	case it.isAr:
		w.WriteArray(len(it.arr))
		for _, s := range it.arr {
			w.WriteBulkString(s)
		}
	case it.str:
		w.WriteBulkString(it.s)
	default:
		w.WriteInt64(it.i)
	}
}

// writeItems replies with one value per match ($ paths) or the first match
// (legacy paths).
func writeItems(w redis.Writer, p *jsondoc.Path, items []item) error {
	if p.Legacy {
		if len(items) == 0 {
			return errPath(p)
		}
		if items[0].null {
			return errWrongType
		}
		writeItem(w, items[0])
		return nil
	}
	w.WriteArray(len(items))
	for _, it := range items {
		writeItem(w, it)
	}
	return nil
}

// jsonArray renders per-match JSON texts ("" = null) as a JSON array.
func jsonArray(texts []string) string {
	parts := make([]string, len(texts))
	for i, t := range texts {
		if t == "" {
			t = "null"
		}
		parts[i] = t
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// read runs a read-only per-match query. A missing key replies nil.
func read(w redis.Writer, red redis.Redka, name, key string, p *jsondoc.Path, per func(r jsondoc.Ref) item) (any, error) {
	var items []item
	err := red.JSON().View(key, func(d *jsondoc.Doc) error {
		for _, r := range d.Select(p) {
			items = append(items, per(r))
		}
		return nil
	})
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err == nil {
		err = writeItems(w, p, items)
	}
	return nil, err
}

// write runs a mutating per-match operation and replies with its items.
func write(w redis.Writer, red redis.Redka, key string, p *jsondoc.Path, per func(d *jsondoc.Doc, r jsondoc.Ref) (item, error)) (any, error) {
	var items []item
	err := red.JSON().Update(key, false, func(d *jsondoc.Doc) error {
		for _, r := range d.Select(p) {
			it, err := per(d, r)
			if err != nil {
				return err
			}
			items = append(items, it)
		}
		return nil
	})
	if err == core.ErrNotFound {
		err = errNoKey
	}
	if err == nil {
		err = writeItems(w, p, items)
	}
	return nil, err
}

// base holds the plumbing every command shares.
type base struct {
	redis.BaseCmd
	key  string
	path *jsondoc.Path
}

func (b *base) fail(w redis.Writer, err error) (any, error) {
	w.WriteError(b.Error(err))
	return nil, err
}

func newBase(b redis.BaseCmd, minArgs int, defPath bool) (base, error) {
	c := base{BaseCmd: b}
	if len(b.Args()) < minArgs {
		return c, redis.ErrInvalidArgNum
	}
	c.key = string(b.Args()[0])
	ps := "$"
	if !defPath || len(b.Args()) > 1 {
		if len(b.Args()) > 1 {
			ps = string(b.Args()[1])
		}
	}
	var err error
	c.path, err = parsePath(ps)
	return c, err
}

// ---- JSON.SET ----

type Set struct {
	base
	value  []byte
	nx, xx bool
}

func ParseSet(b redis.BaseCmd) (Set, error) {
	if len(b.Args()) < 3 || len(b.Args()) > 4 {
		return Set{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 3, false)
	if err != nil {
		return Set{}, err
	}
	c := Set{base: bs, value: b.Args()[2]}
	if len(b.Args()) == 4 {
		switch strings.ToLower(string(b.Args()[3])) {
		case "nx":
			c.nx = true
		case "xx":
			c.xx = true
		default:
			return Set{}, redis.ErrSyntaxError
		}
	}
	return c, nil
}

// applySet is shared with JSON.MSET. It reports whether the value was stored.
func applySet(red redis.Redka, key string, p *jsondoc.Path, value []byte, nx, xx bool) (bool, error) {
	stored := false
	err := red.JSON().Update(key, true, func(d *jsondoc.Doc) error {
		n, err := d.Parse(value)
		if err != nil {
			return fmt.Errorf("ERR invalid JSON: %v", err)
		}
		if d.Root == nil {
			if !p.IsRoot() {
				return errors.New("ERR new objects must be created at the root")
			}
			if xx {
				return nil
			}
			d.Root, stored = n, true
			return nil
		}
		matches := len(d.Select(p))
		if (nx && matches > 0) || (xx && matches == 0) {
			return nil
		}
		stored = d.Set(p, n) > 0
		return nil
	})
	return stored, err
}

func (c Set) Run(w redis.Writer, red redis.Redka) (any, error) {
	ok, err := applySet(red, c.key, c.path, c.value, c.nx, c.xx)
	if err != nil {
		return c.fail(w, err)
	}
	if !ok {
		w.WriteNull()
		return nil, nil
	}
	w.WriteString("OK")
	return true, nil
}

// ---- JSON.MSET ----

type MSet struct {
	redis.BaseCmd
	keys   []string
	paths  []*jsondoc.Path
	values [][]byte
}

func ParseMSet(b redis.BaseCmd) (MSet, error) {
	a := b.Args()
	if len(a) == 0 || len(a)%3 != 0 {
		return MSet{}, redis.ErrInvalidArgNum
	}
	c := MSet{BaseCmd: b}
	for i := 0; i < len(a); i += 3 {
		p, err := parsePath(string(a[i+1]))
		if err != nil {
			return MSet{}, err
		}
		c.keys, c.paths, c.values = append(c.keys, string(a[i])), append(c.paths, p), append(c.values, a[i+2])
	}
	return c, nil
}

func (c MSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	for i := range c.keys {
		if _, err := applySet(red, c.keys[i], c.paths[i], c.values[i], false, false); err != nil {
			w.WriteError(c.Error(err))
			return nil, err
		}
	}
	w.WriteString("OK")
	return true, nil
}

// ---- JSON.GET / JSON.MGET ----

// textFor renders the matches of p: a JSON array for $ paths, the first match
// for legacy ones (found=false if a legacy path matches nothing).
func textFor(d *jsondoc.Doc, p *jsondoc.Path) (string, bool) {
	refs := d.Select(p)
	if p.Legacy {
		if len(refs) == 0 {
			return "", false
		}
		return jsondoc.Encode(refs[0].Node), true
	}
	texts := make([]string, len(refs))
	for i, r := range refs {
		texts[i] = jsondoc.Encode(r.Node)
	}
	return jsonArray(texts), true
}

type Get struct {
	redis.BaseCmd
	key   string
	paths []*jsondoc.Path
}

func ParseGet(b redis.BaseCmd) (Get, error) {
	a := b.Args()
	if len(a) < 1 {
		return Get{}, redis.ErrInvalidArgNum
	}
	c := Get{BaseCmd: b, key: string(a[0])}
	rest := a[1:]
	for len(rest) > 0 { // formatting options are accepted and ignored (output is compact)
		switch strings.ToLower(string(rest[0])) {
		case "indent", "newline", "space":
			if len(rest) < 2 {
				return Get{}, redis.ErrSyntaxError
			}
			rest = rest[2:]
			continue
		case "noescape":
			rest = rest[1:]
			continue
		}
		break
	}
	if len(rest) == 0 {
		rest = [][]byte{[]byte("$")}
	}
	for _, ps := range rest {
		p, err := parsePath(string(ps))
		if err != nil {
			return Get{}, err
		}
		c.paths = append(c.paths, p)
	}
	return c, nil
}

func (c Get) Run(w redis.Writer, red redis.Redka) (any, error) {
	var out string
	err := red.JSON().View(c.key, func(d *jsondoc.Doc) error {
		if len(c.paths) == 1 {
			t, ok := textFor(d, c.paths[0])
			if !ok {
				return errPath(c.paths[0])
			}
			out = t
			return nil
		}
		parts := make([]string, len(c.paths))
		for i, p := range c.paths {
			t, ok := textFor(d, p)
			if !ok {
				return errPath(p)
			}
			parts[i] = strconv.Quote(p.Text) + ":" + t
		}
		out = "{" + strings.Join(parts, ",") + "}"
		return nil
	})
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteBulkString(out)
	return out, nil
}

type MGet struct {
	redis.BaseCmd
	keys []string
	path *jsondoc.Path
}

func ParseMGet(b redis.BaseCmd) (MGet, error) {
	a := b.Args()
	if len(a) < 2 {
		return MGet{}, redis.ErrInvalidArgNum
	}
	p, err := parsePath(string(a[len(a)-1]))
	if err != nil {
		return MGet{}, err
	}
	c := MGet{BaseCmd: b, path: p}
	for _, k := range a[:len(a)-1] {
		c.keys = append(c.keys, string(k))
	}
	return c, nil
}

func (c MGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	w.WriteArray(len(c.keys))
	for _, k := range c.keys {
		var out string
		found := false
		err := red.JSON().View(k, func(d *jsondoc.Doc) error {
			out, found = textFor(d, c.path)
			return nil
		})
		if err != nil || !found {
			w.WriteNull() // missing key, wrong type, or no match
			continue
		}
		w.WriteBulkString(out)
	}
	return nil, nil
}

// ---- JSON.DEL / JSON.FORGET ----

type Del struct{ base }

func ParseDel(b redis.BaseCmd) (Del, error) {
	bs, err := newBase(b, 1, true)
	return Del{bs}, err
}

func (c Del) Run(w redis.Writer, red redis.Redka) (any, error) {
	n := 0
	err := red.JSON().Update(c.key, false, func(d *jsondoc.Doc) error {
		for _, r := range d.Select(c.path) {
			if d.Delete(r) {
				n++
			}
		}
		return nil
	})
	if err != nil && err != core.ErrNotFound {
		return c.fail(w, err)
	}
	w.WriteInt(n)
	return n, nil
}

// ---- type / length queries ----

type Query struct {
	base
	kind string
}

func ParseQuery(b redis.BaseCmd, kind string) (Query, error) {
	bs, err := newBase(b, 1, true)
	return Query{bs, kind}, err
}

func (c Query) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := read(w, red, c.Name(), c.key, c.path, func(r jsondoc.Ref) item {
		n := r.Node
		switch c.kind {
		case "type":
			return strItem(n.TypeName())
		case "strlen":
			if n.Kind == jsondoc.String {
				return intItem(len(n.Raw))
			}
		case "arrlen":
			if n.Kind == jsondoc.Array {
				return intItem(len(n.Elems))
			}
		case "objlen":
			if n.Kind == jsondoc.Object {
				return intItem(len(n.Fields))
			}
		case "objkeys":
			if n.Kind == jsondoc.Object {
				return arrItem(n.FieldNames())
			}
		}
		return nullItem()
	})
	if err != nil {
		return c.fail(w, err)
	}
	return res, nil
}

// ---- JSON.NUMINCRBY / JSON.NUMMULTBY ----

type NumOp struct {
	base
	operand string
	mul     bool
}

func ParseNumOp(b redis.BaseCmd, mul bool) (NumOp, error) {
	if len(b.Args()) != 3 {
		return NumOp{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 3, false)
	if err != nil {
		return NumOp{}, err
	}
	if _, err := strconv.ParseFloat(string(b.Args()[2]), 64); err != nil {
		return NumOp{}, redis.ErrInvalidFloat
	}
	return NumOp{bs, string(b.Args()[2]), mul}, nil
}

// applyNum computes raw op operand, using integer arithmetic when both are
// integers that do not overflow.
func applyNum(raw, operand string, mul bool) (string, bool) {
	a, ea := strconv.ParseInt(raw, 10, 64)
	b, eb := strconv.ParseInt(operand, 10, 64)
	if ea == nil && eb == nil {
		var r int64
		if mul {
			r = a * b
			if a != 0 && (r/a != b) {
				goto float
			}
		} else {
			r = a + b
			if (r > a) != (b > 0) {
				goto float
			}
		}
		return strconv.FormatInt(r, 10), true
	}
float:
	x, ex := strconv.ParseFloat(raw, 64)
	y, ey := strconv.ParseFloat(operand, 64)
	if ex != nil || ey != nil {
		return "", false
	}
	r := x + y
	if mul {
		r = x * y
	}
	if r != r || r > 1.7976931348623157e308 || r < -1.7976931348623157e308 {
		return "", false
	}
	return strconv.FormatFloat(r, 'g', -1, 64), true
}

func (c NumOp) Run(w redis.Writer, red redis.Redka) (any, error) {
	var texts []string
	var failed bool
	err := red.JSON().Update(c.key, false, func(d *jsondoc.Doc) error {
		for _, r := range d.Select(c.path) {
			if r.Node.Kind != jsondoc.Number {
				texts = append(texts, "")
				continue
			}
			res, ok := applyNum(r.Node.Raw, c.operand, c.mul)
			if !ok {
				failed = true
				return errors.New("ERR result is not a number")
			}
			r.Node.Raw = res
			texts = append(texts, res)
		}
		return nil
	})
	if err == core.ErrNotFound {
		err = errNoKey
	}
	if err == nil && failed {
		err = errWrongType
	}
	if err == nil && c.path.Legacy {
		if len(texts) == 0 || texts[0] == "" {
			err = errWrongType
		} else {
			w.WriteBulkString(texts[0])
			return texts[0], nil
		}
	}
	if err != nil {
		return c.fail(w, err)
	}
	out := jsonArray(texts)
	w.WriteBulkString(out)
	return out, nil
}

// ---- JSON.TOGGLE / JSON.CLEAR ----

type Toggle struct{ base }

func ParseToggle(b redis.BaseCmd) (Toggle, error) {
	if len(b.Args()) != 2 {
		return Toggle{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 2, false)
	return Toggle{bs}, err
}

func (c Toggle) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		if r.Node.Kind != jsondoc.Bool {
			return nullItem(), nil
		}
		if r.Node.Raw == "true" {
			r.Node.Raw = "false"
			return intItem(0), nil
		}
		r.Node.Raw = "true"
		return intItem(1), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

type Clear struct{ base }

func ParseClear(b redis.BaseCmd) (Clear, error) {
	bs, err := newBase(b, 1, true)
	return Clear{bs}, err
}

func (c Clear) Run(w redis.Writer, red redis.Redka) (any, error) {
	n := 0
	err := red.JSON().Update(c.key, false, func(d *jsondoc.Doc) error {
		for _, r := range d.Select(c.path) {
			switch r.Node.Kind {
			case jsondoc.Object:
				r.Node.Fields = map[string]*jsondoc.Node{}
			case jsondoc.Array:
				r.Node.Elems = nil
			case jsondoc.Number:
				r.Node.Raw = "0"
			default:
				continue
			}
			n++
		}
		return nil
	})
	if err == core.ErrNotFound {
		err = errNoKey
	}
	if err != nil {
		return c.fail(w, err)
	}
	w.WriteInt(n)
	return n, nil
}

// ---- JSON.STRAPPEND ----

type StrAppend struct {
	base
	value []byte
}

func ParseStrAppend(b redis.BaseCmd) (StrAppend, error) {
	a := b.Args()
	if len(a) != 2 && len(a) != 3 {
		return StrAppend{}, redis.ErrInvalidArgNum
	}
	bs := base{BaseCmd: b, key: string(a[0])}
	ps := "$"
	if len(a) == 3 {
		ps = string(a[1])
	}
	var err error
	bs.path, err = parsePath(ps)
	return StrAppend{bs, a[len(a)-1]}, err
}

func (c StrAppend) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		add, err := d.Parse(c.value)
		if err != nil || add.Kind != jsondoc.String {
			return item{}, errors.New("ERR expected a JSON string value to append")
		}
		if r.Node.Kind != jsondoc.String {
			return nullItem(), nil
		}
		r.Node.Raw += add.Raw
		return intItem(len(r.Node.Raw)), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

// ---- arrays ----

// normIndex resolves a possibly negative index; ok=false if outside [0,n].
func normIndex(i, n int) (int, bool) {
	if i < 0 {
		i += n
	}
	return i, i >= 0 && i <= n
}

type ArrAppend struct {
	base
	values [][]byte
}

func ParseArrAppend(b redis.BaseCmd) (ArrAppend, error) {
	if len(b.Args()) < 3 {
		return ArrAppend{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 3, false)
	return ArrAppend{bs, b.Args()[2:]}, err
}

func (c ArrAppend) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		if r.Node.Kind != jsondoc.Array {
			return nullItem(), nil
		}
		for _, v := range c.values {
			n, err := d.Parse(v)
			if err != nil {
				return item{}, fmt.Errorf("ERR invalid JSON: %v", err)
			}
			d.AppendElem(r.Node, n)
		}
		return intItem(len(r.Node.Elems)), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

type ArrInsert struct {
	base
	index  int
	values [][]byte
}

func ParseArrInsert(b redis.BaseCmd) (ArrInsert, error) {
	if len(b.Args()) < 4 {
		return ArrInsert{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 4, false)
	if err != nil {
		return ArrInsert{}, err
	}
	idx, err := strconv.Atoi(string(b.Args()[2]))
	if err != nil {
		return ArrInsert{}, redis.ErrInvalidInt
	}
	return ArrInsert{bs, idx, b.Args()[3:]}, nil
}

func (c ArrInsert) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		if r.Node.Kind != jsondoc.Array {
			return nullItem(), nil
		}
		i, ok := normIndex(c.index, len(r.Node.Elems))
		if !ok {
			return item{}, errRange
		}
		nodes := make([]*jsondoc.Node, 0, len(c.values))
		for _, v := range c.values {
			n, err := d.Parse(v)
			if err != nil {
				return item{}, fmt.Errorf("ERR invalid JSON: %v", err)
			}
			nodes = append(nodes, n)
		}
		d.InsertElems(r.Node, i, nodes)
		return intItem(len(r.Node.Elems)), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

type ArrPop struct {
	base
	index int
}

func ParseArrPop(b redis.BaseCmd) (ArrPop, error) {
	a := b.Args()
	if len(a) < 1 || len(a) > 3 {
		return ArrPop{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 1, true)
	c := ArrPop{bs, -1}
	if err == nil && len(a) == 3 {
		if c.index, err = strconv.Atoi(string(a[2])); err != nil {
			err = redis.ErrInvalidInt
		}
	}
	return c, err
}

func (c ArrPop) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		els := r.Node.Elems
		if r.Node.Kind != jsondoc.Array || len(els) == 0 {
			return nullItem(), nil
		}
		i := c.index
		if i < 0 {
			i += len(els)
		}
		i = min(max(i, 0), len(els)-1) // out of range clamps, as in RedisJSON
		popped := els[i].Node
		r.Node.Elems = append(els[:i:i], els[i+1:]...)
		return strItem(jsondoc.Encode(popped)), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

type ArrTrim struct {
	base
	start, stop int
}

func ParseArrTrim(b redis.BaseCmd) (ArrTrim, error) {
	if len(b.Args()) != 4 {
		return ArrTrim{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 4, false)
	if err != nil {
		return ArrTrim{}, err
	}
	s, e1 := strconv.Atoi(string(b.Args()[2]))
	t, e2 := strconv.Atoi(string(b.Args()[3]))
	if e1 != nil || e2 != nil {
		return ArrTrim{}, redis.ErrInvalidInt
	}
	return ArrTrim{bs, s, t}, nil
}

func (c ArrTrim) Run(w redis.Writer, red redis.Redka) (any, error) {
	_, err := write(w, red, c.key, c.path, func(d *jsondoc.Doc, r jsondoc.Ref) (item, error) {
		if r.Node.Kind != jsondoc.Array {
			return nullItem(), nil
		}
		n := len(r.Node.Elems)
		s, e := c.start, c.stop
		if s < 0 {
			s += n
		}
		if e < 0 {
			e += n
		}
		s, e = max(s, 0), min(e, n-1)
		if s > e || s >= n {
			r.Node.Elems = nil
		} else {
			r.Node.Elems = append([]*jsondoc.Elem(nil), r.Node.Elems[s:e+1]...)
		}
		return intItem(len(r.Node.Elems)), nil
	})
	if err != nil {
		return c.fail(w, err)
	}
	return nil, nil
}

type ArrIndex struct {
	base
	value       []byte
	start, stop int
}

func ParseArrIndex(b redis.BaseCmd) (ArrIndex, error) {
	a := b.Args()
	if len(a) < 3 || len(a) > 5 {
		return ArrIndex{}, redis.ErrInvalidArgNum
	}
	bs, err := newBase(b, 3, false)
	if err != nil {
		return ArrIndex{}, err
	}
	c := ArrIndex{base: bs, value: a[2]}
	if len(a) > 3 {
		if c.start, err = strconv.Atoi(string(a[3])); err != nil {
			return ArrIndex{}, redis.ErrInvalidInt
		}
	}
	if len(a) > 4 {
		if c.stop, err = strconv.Atoi(string(a[4])); err != nil {
			return ArrIndex{}, redis.ErrInvalidInt
		}
	}
	return c, nil
}

func (c ArrIndex) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Parse the search value first, so a bad value is an error rather than a
	// reply of nulls.
	want, err := (&jsondoc.Doc{NewPos: func(string, string) string { return "" }}).Parse(c.value)
	if err != nil {
		return c.fail(w, fmt.Errorf("ERR invalid JSON: %v", err))
	}
	wt := jsondoc.Encode(want)
	res, err := read(w, red, c.Name(), c.key, c.path, func(r jsondoc.Ref) item {
		if r.Node.Kind != jsondoc.Array {
			return nullItem()
		}
		n := len(r.Node.Elems)
		s, e := c.start, c.stop
		if s < 0 {
			s = max(s+n, 0)
		}
		if e <= 0 {
			e += n // 0 means "through the last element"
		}
		for i := s; i < min(e, n); i++ {
			if jsondoc.Encode(r.Node.Elems[i].Node) == wt {
				return intItem(i)
			}
		}
		return intItem(-1)
	})
	if err != nil {
		return c.fail(w, err)
	}
	return res, nil
}
