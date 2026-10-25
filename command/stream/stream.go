// Package stream implements the Redis stream commands (XADD, XRANGE, XREAD, …)
// on top of the RStream storage interface. Consumer groups are not implemented.
package stream

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

var (
	errBadID    = errors.New("ERR Invalid stream ID specified as stream command argument")
	errArgs     = redis.ErrInvalidArgNum
	errTrimOpt  = errors.New("ERR The MAXLEN/MINID argument must be >= 0")
	errUnbalXR  = errors.New("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.")
	errXReadOpt = errors.New("ERR timeout is not an integer or out of range")
)

// parseID parses "ms" or "ms-seq"; a bare ms takes defSeq as its seq.
func parseID(s string, defSeq uint64) (restypes.StreamID, error) {
	msS, seqS, hasSeq := strings.Cut(s, "-")
	ms, err := strconv.ParseUint(msS, 10, 64)
	if err != nil {
		return restypes.StreamID{}, errBadID
	}
	if !hasSeq {
		return restypes.StreamID{Ms: ms, Seq: defSeq}, nil
	}
	seq, err := strconv.ParseUint(seqS, 10, 64)
	if err != nil {
		return restypes.StreamID{}, errBadID
	}
	return restypes.StreamID{Ms: ms, Seq: seq}, nil
}

// parseBound parses an XRANGE endpoint: "-", "+", "(id" (exclusive) or id.
// A bare ms is the smallest seq for a start and the largest for an end.
func parseBound(s string, isEnd bool) (restypes.StreamBound, error) {
	switch s {
	case "-":
		return restypes.StreamBound{}, nil
	case "+":
		return restypes.StreamBound{ID: restypes.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}}, nil
	}
	excl := strings.HasPrefix(s, "(")
	defSeq := uint64(0)
	if isEnd {
		defSeq = math.MaxUint64
	}
	id, err := parseID(strings.TrimPrefix(s, "("), defSeq)
	return restypes.StreamBound{ID: id, Excl: excl}, err
}

func writeEntries(w redis.Writer, es []restypes.StreamEntry) {
	w.WriteArray(len(es))
	for _, e := range es {
		writeEntry(w, e)
	}
}

// writeEntry writes [id, [field value ...]]. An entry with no fields was
// deleted from the stream while still pending in a group: [id, nil].
func writeEntry(w redis.Writer, e restypes.StreamEntry) {
	w.WriteArray(2)
	w.WriteBulkString(e.ID.String())
	if e.Fields == nil {
		w.WriteNull()
		return
	}
	w.WriteArray(len(e.Fields))
	for _, f := range e.Fields {
		w.WriteBulk(f)
	}
}

// parseTrim parses `MAXLEN|MINID [=|~] threshold [LIMIT n]` starting at args[0]
// (which is the strategy keyword) and returns the trim and args consumed.
func parseTrim(args [][]byte) (restypes.StreamTrim, int, error) {
	var t restypes.StreamTrim
	n := 1
	if n < len(args) && (string(args[n]) == "=" || string(args[n]) == "~") {
		n++
	}
	if n >= len(args) {
		return t, 0, redis.ErrSyntaxError
	}
	switch strings.ToLower(string(args[0])) {
	case "maxlen":
		v, err := strconv.ParseInt(string(args[n]), 10, 64)
		if err != nil {
			return t, 0, redis.ErrInvalidInt
		}
		if v < 0 {
			return t, 0, errTrimOpt
		}
		t.ByMaxLen, t.MaxLen = true, v
	case "minid":
		id, err := parseID(string(args[n]), 0)
		if err != nil {
			return t, 0, err
		}
		t.ByMinID, t.MinID = true, id
	}
	n++
	if n+1 < len(args) && strings.EqualFold(string(args[n]), "limit") {
		if _, err := strconv.ParseInt(string(args[n+1]), 10, 64); err != nil {
			return t, 0, redis.ErrInvalidInt
		}
		n += 2 // LIMIT only bounds work for "~"; trimming is exact here
	}
	return t, n, nil
}

// XAdd appends an entry to a stream.
// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] <* | id> field value [field value ...]
// https://redis.io/commands/xadd
type XAdd struct {
	redis.BaseCmd
	key    string
	opts   restypes.StreamAddOpts
	fields [][]byte
}

func ParseXAdd(b redis.BaseCmd) (XAdd, error) {
	cmd := XAdd{BaseCmd: b}
	args := cmd.Args()
	if len(args) < 4 {
		return XAdd{}, errArgs
	}
	cmd.key = string(args[0])
	args = args[1:]
	for len(args) > 0 {
		kw := strings.ToLower(string(args[0]))
		if kw == "nomkstream" {
			cmd.opts.NoMkStream = true
			args = args[1:]
		} else if kw == "maxlen" || kw == "minid" {
			t, n, err := parseTrim(args)
			if err != nil {
				return XAdd{}, err
			}
			cmd.opts.Trim = &t
			args = args[n:]
		} else {
			break
		}
	}
	if len(args) < 3 || len(args)%2 != 1 {
		return XAdd{}, errArgs
	}
	switch id := string(args[0]); {
	case id == "*":
		cmd.opts.Mode = restypes.IDAuto
	case strings.HasSuffix(id, "-*"):
		ms, err := strconv.ParseUint(strings.TrimSuffix(id, "-*"), 10, 64)
		if err != nil {
			return XAdd{}, errBadID
		}
		cmd.opts.Mode, cmd.opts.ID = restypes.IDAutoSeq, restypes.StreamID{Ms: ms}
	default:
		sid, err := parseID(id, 0)
		if err != nil {
			return XAdd{}, err
		}
		cmd.opts.ID = sid
	}
	cmd.fields = args[1:]
	return cmd, nil
}

func (cmd XAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	id, ok, err := red.Stream().Add(cmd.key, cmd.opts, cmd.fields)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !ok {
		w.WriteNull()
		return nil, nil
	}
	w.WriteBulkString(id.String())
	return id, nil
}

// XLen returns the number of entries in a stream.
// XLEN key
// https://redis.io/commands/xlen
type XLen struct {
	redis.BaseCmd
	key string
}

func ParseXLen(b redis.BaseCmd) (XLen, error) {
	cmd := XLen{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return XLen{}, errArgs
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd XLen) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Stream().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// XRange returns entries within an ID range (XREVRANGE: reversed, end first).
// XRANGE key start end [COUNT count]
// XREVRANGE key end start [COUNT count]
// https://redis.io/commands/xrange
type XRange struct {
	redis.BaseCmd
	key    string
	lo, hi restypes.StreamBound
	count  int
	rev    bool
}

func ParseXRange(b redis.BaseCmd, rev bool) (XRange, error) {
	cmd := XRange{BaseCmd: b, rev: rev}
	args := cmd.Args()
	if len(args) != 3 && len(args) != 5 {
		return XRange{}, errArgs
	}
	cmd.key = string(args[0])
	first, second := string(args[1]), string(args[2])
	if rev {
		first, second = second, first // XREVRANGE key end start
	}
	var err error
	if cmd.lo, err = parseBound(first, false); err != nil {
		return XRange{}, err
	}
	if cmd.hi, err = parseBound(second, true); err != nil {
		return XRange{}, err
	}
	if len(args) == 5 {
		if !strings.EqualFold(string(args[3]), "count") {
			return XRange{}, redis.ErrSyntaxError
		}
		if cmd.count, err = strconv.Atoi(string(args[4])); err != nil {
			return XRange{}, redis.ErrInvalidInt
		}
		if cmd.count <= 0 {
			cmd.count = -1 // COUNT 0 / negative: empty result (below)
		}
	}
	return cmd, nil
}

func (cmd XRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	if cmd.count < 0 {
		w.WriteArray(0)
		return nil, nil
	}
	es, err := red.Stream().Range(cmd.key, cmd.lo, cmd.hi, cmd.count, cmd.rev)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeEntries(w, es)
	return es, nil
}

// XDel removes entries by ID.
// XDEL key id [id ...]
// https://redis.io/commands/xdel
type XDel struct {
	redis.BaseCmd
	key string
	ids []restypes.StreamID
}

func ParseXDel(b redis.BaseCmd) (XDel, error) {
	cmd := XDel{BaseCmd: b}
	if len(cmd.Args()) < 2 {
		return XDel{}, errArgs
	}
	cmd.key = string(cmd.Args()[0])
	for _, a := range cmd.Args()[1:] {
		id, err := parseID(string(a), 0)
		if err != nil {
			return XDel{}, err
		}
		cmd.ids = append(cmd.ids, id)
	}
	return cmd, nil
}

func (cmd XDel) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Stream().Del(cmd.key, cmd.ids...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// XTrim trims a stream by length or minimum ID.
// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
// https://redis.io/commands/xtrim
type XTrim struct {
	redis.BaseCmd
	key  string
	trim restypes.StreamTrim
}

func ParseXTrim(b redis.BaseCmd) (XTrim, error) {
	cmd := XTrim{BaseCmd: b}
	args := cmd.Args()
	if len(args) < 3 {
		return XTrim{}, errArgs
	}
	cmd.key = string(args[0])
	kw := strings.ToLower(string(args[1]))
	if kw != "maxlen" && kw != "minid" {
		return XTrim{}, redis.ErrSyntaxError
	}
	t, n, err := parseTrim(args[1:])
	if err != nil {
		return XTrim{}, err
	}
	if n != len(args)-1 {
		return XTrim{}, redis.ErrSyntaxError
	}
	cmd.trim = t
	return cmd, nil
}

func (cmd XTrim) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Stream().Trim(cmd.key, cmd.trim)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// XRead reads entries newer than the given IDs from one or more streams.
// XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
// https://redis.io/commands/xread
//
// BLOCK polls the store until data arrives or the timeout passes (entries from
// other replicas arrive via replication, so a poll sees them too). BLOCK 0
// waits at most blockForever, not indefinitely.
type XRead struct {
	redis.BaseCmd
	count  int
	block  time.Duration
	blocks bool
	keys   []string
	ids    []string // raw; "$" resolved when the command starts
}

const (
	blockForever = time.Minute
	blockPoll    = 20 * time.Millisecond
)

func ParseXRead(b redis.BaseCmd) (XRead, error) {
	cmd := XRead{BaseCmd: b}
	args := cmd.Args()
	i := 0
	for ; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "count":
			if i+1 >= len(args) {
				return XRead{}, redis.ErrSyntaxError
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return XRead{}, redis.ErrInvalidInt
			}
			cmd.count = n
			i++
		case "block":
			if i+1 >= len(args) {
				return XRead{}, redis.ErrSyntaxError
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return XRead{}, errXReadOpt
			}
			cmd.blocks = true
			cmd.block = time.Duration(ms) * time.Millisecond
			if ms == 0 {
				cmd.block = blockForever
			}
			i++
		case "streams":
			rest := args[i+1:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				return XRead{}, errUnbalXR
			}
			half := len(rest) / 2
			for j := 0; j < half; j++ {
				cmd.keys = append(cmd.keys, string(rest[j]))
				cmd.ids = append(cmd.ids, string(rest[half+j]))
			}
			return cmd, nil
		default:
			return XRead{}, redis.ErrSyntaxError
		}
	}
	return XRead{}, redis.ErrSyntaxError
}

func (cmd XRead) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Resolve each start ID once: "$" means "entries added from now on".
	after := make([]restypes.StreamID, len(cmd.keys))
	for i, raw := range cmd.ids {
		if raw == "$" {
			id, err := red.Stream().LastID(cmd.keys[i])
			if err != nil {
				w.WriteError(cmd.Error(err))
				return nil, err
			}
			after[i] = id
			continue
		}
		id, err := parseID(raw, 0)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		after[i] = id
	}
	max := restypes.StreamBound{ID: restypes.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}}
	deadline := time.Now().Add(cmd.block)
	for {
		type result struct {
			key string
			es  []restypes.StreamEntry
		}
		var got []result
		for i, key := range cmd.keys {
			es, err := red.Stream().Range(key, restypes.StreamBound{ID: after[i], Excl: true}, max, cmd.count, false)
			if err != nil {
				w.WriteError(cmd.Error(err))
				return nil, err
			}
			if len(es) > 0 {
				got = append(got, result{key, es})
			}
		}
		if len(got) > 0 {
			w.WriteArray(len(got))
			for _, g := range got {
				w.WriteArray(2)
				w.WriteBulkString(g.key)
				writeEntries(w, g.es)
			}
			return got, nil
		}
		if !cmd.blocks || !time.Now().Before(deadline) {
			w.WriteNull()
			return nil, nil
		}
		time.Sleep(blockPoll)
	}
}
