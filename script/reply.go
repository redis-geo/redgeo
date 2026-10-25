// Package script runs Redis Lua scripts (EVAL/EVALSHA) in a sandboxed
// gopher-lua VM. Scripts execute on the local node against whatever store the
// calling command was handed (a transaction-bound one under MULTI/EXEC or an
// EVAL), so their writes replicate as ordinary CRDT writes: scripts are never
// replayed on other nodes, hence non-deterministic functions are allowed.
package script

import (
	"fmt"
	"strconv"
)

// Reply is a recorded RESP reply tree.
type Reply struct {
	Kind byte // '+' status, '-' error, ':' integer, '$' bulk, '_' nil, '*' array
	Str  string
	Int  int64
	Arr  []Reply
}

// Recorder is a redis.Writer that builds a Reply instead of writing to a
// connection. RESP3 types are folded into their RESP2 forms (as Lua sees them).
type Recorder struct {
	root  Reply
	set   bool
	stack []*frame
}

type frame struct {
	r    *Reply
	left int
}

// Reply returns the recorded reply (nil if nothing was written).
func (w *Recorder) Reply() Reply {
	if !w.set {
		return Reply{Kind: '_'}
	}
	return w.root
}

func (w *Recorder) put(r Reply) {
	if len(w.stack) == 0 {
		w.root, w.set = r, true
		return
	}
	f := w.stack[len(w.stack)-1]
	f.r.Arr = append(f.r.Arr, r)
	f.left--
	w.sweep()
}

// sweep pops every completed frame off the top of the stack.
func (w *Recorder) sweep() {
	for len(w.stack) > 0 && w.stack[len(w.stack)-1].left == 0 {
		w.stack = w.stack[:len(w.stack)-1]
	}
}

// WriteArray starts an array of n children. Arrays are preallocated to n, so
// pointers into a parent's Arr stay valid while the child is being filled; a
// parent whose last child is still open stays below it on the stack.
func (w *Recorder) WriteArray(n int) {
	if n <= 0 {
		w.put(Reply{Kind: '*'})
		return
	}
	arr := Reply{Kind: '*', Arr: make([]Reply, 0, n)}
	var slot *Reply
	if len(w.stack) == 0 {
		w.root, w.set = arr, true
		slot = &w.root
	} else {
		p := w.stack[len(w.stack)-1]
		p.r.Arr = append(p.r.Arr, arr)
		p.left--
		slot = &p.r.Arr[len(p.r.Arr)-1]
	}
	w.stack = append(w.stack, &frame{r: slot, left: n})
}

func (w *Recorder) WriteMap(pairs int)       { w.WriteArray(pairs * 2) }
func (w *Recorder) WriteBulk(b []byte)       { w.put(Reply{Kind: '$', Str: string(b)}) }
func (w *Recorder) WriteBulkString(s string) { w.put(Reply{Kind: '$', Str: s}) }
func (w *Recorder) WriteError(msg string)    { w.put(Reply{Kind: '-', Str: msg}) }
func (w *Recorder) WriteInt(n int)           { w.put(Reply{Kind: ':', Int: int64(n)}) }
func (w *Recorder) WriteInt64(n int64)       { w.put(Reply{Kind: ':', Int: n}) }
func (w *Recorder) WriteUint64(n uint64)     { w.put(Reply{Kind: ':', Int: int64(n)}) }
func (w *Recorder) WriteNull()               { w.put(Reply{Kind: '_'}) }
func (w *Recorder) WriteString(s string)     { w.put(Reply{Kind: '+', Str: s}) }
func (w *Recorder) WriteRaw(b []byte)        { w.put(Reply{Kind: '+', Str: string(b)}) }
func (w *Recorder) WriteDouble(f float64) {
	w.put(Reply{Kind: '$', Str: strconv.FormatFloat(f, 'g', -1, 64)})
}
func (w *Recorder) WriteBool(b bool) {
	if b {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
}

func (w *Recorder) WriteAny(v any) {
	switch x := v.(type) {
	case nil:
		w.WriteNull()
	case string:
		w.WriteBulkString(x)
	case []byte:
		w.WriteBulk(x)
	case int:
		w.WriteInt(x)
	case int64:
		w.WriteInt64(x)
	case bool:
		w.WriteBool(x)
	case float64:
		w.WriteDouble(x)
	default:
		w.WriteBulkString(fmt.Sprint(x))
	}
}
