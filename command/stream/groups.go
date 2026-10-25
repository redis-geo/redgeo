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

// nowMS is indirected for tests.
var nowMS = func() int64 { return time.Now().UnixMilli() }

var errGroupOnly = errors.New("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")

func maxBound() restypes.StreamBound {
	return restypes.StreamBound{ID: restypes.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}}
}

// resolveID parses an ID argument where "$" means the stream's last ID.
func resolveID(red redis.Redka, key, raw string) (restypes.StreamID, error) {
	if raw == "$" {
		return red.Stream().LastID(key)
	}
	return parseID(raw, 0)
}

// ---- XGROUP ----

// XGroup manages consumer groups.
// XGROUP CREATE key group <id | $> [MKSTREAM] [ENTRIESREAD n]
// XGROUP DESTROY key group
// XGROUP CREATECONSUMER key group consumer
// XGROUP DELCONSUMER key group consumer
// XGROUP SETID key group <id | $> [ENTRIESREAD n]
// https://redis.io/commands/xgroup
type XGroup struct {
	redis.BaseCmd
	sub      string
	key      string
	group    string
	arg      string // id (CREATE/SETID) or consumer
	mkstream bool
}

func ParseXGroup(b redis.BaseCmd) (XGroup, error) {
	a := b.Args()
	if len(a) < 1 {
		return XGroup{}, errArgs
	}
	c := XGroup{BaseCmd: b, sub: strings.ToLower(string(a[0]))}
	need := map[string]int{"create": 4, "destroy": 3, "createconsumer": 4, "delconsumer": 4, "setid": 4}
	n, ok := need[c.sub]
	if !ok {
		return XGroup{}, redis.ErrUnknownSubcmd
	}
	if len(a) < n {
		return XGroup{}, errArgs
	}
	c.key, c.group = string(a[1]), string(a[2])
	if n == 4 {
		c.arg = string(a[3])
	}
	rest := a[n:]
	for len(rest) > 0 {
		switch kw := strings.ToLower(string(rest[0])); {
		case kw == "mkstream" && c.sub == "create":
			c.mkstream = true
			rest = rest[1:]
		case kw == "entriesread" && (c.sub == "create" || c.sub == "setid") && len(rest) >= 2:
			rest = rest[2:] // accepted, not tracked
		default:
			return XGroup{}, redis.ErrSyntaxError
		}
	}
	if (c.sub == "create" || c.sub == "setid") && c.arg != "$" {
		if _, err := parseID(c.arg, 0); err != nil {
			return XGroup{}, err
		}
	}
	return c, nil
}

func (c XGroup) Run(w redis.Writer, red redis.Redka) (any, error) {
	rs := red.Stream()
	var err error
	switch c.sub {
	case "create":
		var id restypes.StreamID
		if id, err = resolveID(red, c.key, c.arg); err == nil {
			if err = rs.GroupCreate(c.key, c.group, id, c.mkstream); err == redis.ErrNoGroup {
				err = errGroupOnly
			}
			if err == nil {
				w.WriteString("OK")
			}
		}
	case "setid":
		var id restypes.StreamID
		if id, err = resolveID(red, c.key, c.arg); err == nil {
			if err = rs.GroupSetID(c.key, c.group, id); err == nil {
				w.WriteString("OK")
			}
		}
	case "destroy":
		var ok bool
		if ok, err = rs.GroupDestroy(c.key, c.group); err == nil {
			if ok {
				w.WriteInt(1)
			} else {
				w.WriteInt(0)
			}
		}
	case "createconsumer":
		var isNew bool
		if isNew, err = rs.ConsumerCreate(c.key, c.group, c.arg); err == nil {
			if isNew {
				w.WriteInt(1)
			} else {
				w.WriteInt(0)
			}
		}
	case "delconsumer":
		var n int
		if n, err = rs.ConsumerDelete(c.key, c.group, c.arg); err == nil {
			w.WriteInt(n)
		}
	}
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	return nil, nil
}

// ---- XREADGROUP ----

// XReadGroup reads entries on behalf of a consumer in a group.
// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]
// https://redis.io/commands/xreadgroup
//
// With id ">" it delivers new entries and records them as pending; with any
// other id it replays the consumer's own pending entries after that id. Across
// replicas delivery is at-least-once (see rstream_group.go).
type XReadGroup struct {
	redis.BaseCmd
	group, consumer string
	count           int
	block           time.Duration
	blocks, noack   bool
	keys, ids       []string
}

func ParseXReadGroup(b redis.BaseCmd) (XReadGroup, error) {
	c := XReadGroup{BaseCmd: b}
	a := b.Args()
	if len(a) < 6 || !strings.EqualFold(string(a[0]), "group") {
		return XReadGroup{}, redis.ErrSyntaxError
	}
	c.group, c.consumer = string(a[1]), string(a[2])
	for i := 3; i < len(a); i++ {
		switch strings.ToLower(string(a[i])) {
		case "count":
			if i+1 >= len(a) {
				return XReadGroup{}, redis.ErrSyntaxError
			}
			n, err := strconv.Atoi(string(a[i+1]))
			if err != nil {
				return XReadGroup{}, redis.ErrInvalidInt
			}
			c.count = n
			i++
		case "block":
			if i+1 >= len(a) {
				return XReadGroup{}, redis.ErrSyntaxError
			}
			ms, err := strconv.ParseInt(string(a[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return XReadGroup{}, errXReadOpt
			}
			c.blocks, c.block = true, time.Duration(ms)*time.Millisecond
			if ms == 0 {
				c.block = blockForever
			}
			i++
		case "noack":
			c.noack = true
		case "streams":
			rest := a[i+1:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				return XReadGroup{}, errUnbalXR
			}
			half := len(rest) / 2
			for j := 0; j < half; j++ {
				c.keys = append(c.keys, string(rest[j]))
				c.ids = append(c.ids, string(rest[half+j]))
			}
			return c, nil
		default:
			return XReadGroup{}, redis.ErrSyntaxError
		}
	}
	return XReadGroup{}, redis.ErrSyntaxError
}

func (c XReadGroup) Run(w redis.Writer, red redis.Redka) (any, error) {
	rs := red.Stream()
	history := make([]bool, len(c.keys))
	after := make([]restypes.StreamID, len(c.keys))
	anyNew := false
	for i, raw := range c.ids {
		if raw == ">" {
			anyNew = true
			continue
		}
		id, err := parseID(raw, 0)
		if err != nil {
			w.WriteError(c.Error(err))
			return nil, err
		}
		history[i], after[i] = true, id
	}
	deadline := time.Now().Add(c.block)
	for {
		type result struct {
			key string
			es  []restypes.StreamEntry
		}
		var got []result
		for i, key := range c.keys {
			var es []restypes.StreamEntry
			var err error
			if history[i] {
				es, err = rs.ReadPending(key, c.group, c.consumer, after[i], c.count)
			} else {
				es, err = rs.ReadNew(key, c.group, c.consumer, c.count, c.noack)
			}
			if err != nil {
				w.WriteError(c.Error(err))
				return nil, err
			}
			if len(es) > 0 || history[i] {
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
		if !anyNew || !c.blocks || !time.Now().Before(deadline) {
			w.WriteNull()
			return nil, nil
		}
		time.Sleep(blockPoll)
	}
}

// ---- XACK ----

// XAck acknowledges pending entries.
// XACK key group id [id ...]
// https://redis.io/commands/xack
type XAck struct {
	redis.BaseCmd
	key, group string
	ids        []restypes.StreamID
}

func ParseXAck(b redis.BaseCmd) (XAck, error) {
	a := b.Args()
	if len(a) < 3 {
		return XAck{}, errArgs
	}
	c := XAck{BaseCmd: b, key: string(a[0]), group: string(a[1])}
	for _, x := range a[2:] {
		id, err := parseID(string(x), 0)
		if err != nil {
			return XAck{}, err
		}
		c.ids = append(c.ids, id)
	}
	return c, nil
}

func (c XAck) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Stream().Ack(c.key, c.group, c.ids...)
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// ---- XPENDING ----

// XPending inspects a group's pending entries.
// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
// https://redis.io/commands/xpending
type XPending struct {
	redis.BaseCmd
	key, group string
	extended   bool
	minIdle    int64
	lo, hi     restypes.StreamBound
	count      int
	consumer   string
}

func ParseXPending(b redis.BaseCmd) (XPending, error) {
	a := b.Args()
	if len(a) < 2 {
		return XPending{}, errArgs
	}
	c := XPending{BaseCmd: b, key: string(a[0]), group: string(a[1])}
	rest := a[2:]
	if len(rest) == 0 {
		return c, nil
	}
	if len(rest) >= 2 && strings.EqualFold(string(rest[0]), "idle") {
		v, err := strconv.ParseInt(string(rest[1]), 10, 64)
		if err != nil {
			return XPending{}, redis.ErrInvalidInt
		}
		c.minIdle, rest = v, rest[2:]
	}
	if len(rest) != 3 && len(rest) != 4 {
		return XPending{}, redis.ErrSyntaxError
	}
	var err error
	if c.lo, err = parseBound(string(rest[0]), false); err != nil {
		return XPending{}, err
	}
	if c.hi, err = parseBound(string(rest[1]), true); err != nil {
		return XPending{}, err
	}
	if c.count, err = strconv.Atoi(string(rest[2])); err != nil {
		return XPending{}, redis.ErrInvalidInt
	}
	if len(rest) == 4 {
		c.consumer = string(rest[3])
	}
	c.extended = true
	return c, nil
}

func (c XPending) Run(w redis.Writer, red redis.Redka) (any, error) {
	g, err := red.Stream().Group(c.key, c.group)
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	if !c.extended {
		if len(g.Pending) == 0 {
			w.WriteArray(4)
			w.WriteInt(0)
			w.WriteNull()
			w.WriteNull()
			w.WriteNull()
			return nil, nil
		}
		per := map[string]int{}
		var names []string
		for _, p := range g.Pending {
			if per[p.Consumer] == 0 {
				names = append(names, p.Consumer)
			}
			per[p.Consumer]++
		}
		sortStrings(names)
		w.WriteArray(4)
		w.WriteInt(len(g.Pending))
		w.WriteBulkString(g.Pending[0].ID.String())
		w.WriteBulkString(g.Pending[len(g.Pending)-1].ID.String())
		w.WriteArray(len(names))
		for _, n := range names {
			w.WriteArray(2)
			w.WriteBulkString(n)
			w.WriteBulkString(strconv.Itoa(per[n]))
		}
		return nil, nil
	}
	now := nowMS()
	var out []restypes.PendingEntry
	for _, p := range g.Pending {
		if cmpLo := p.ID.Cmp(c.lo.ID); cmpLo < 0 || (cmpLo == 0 && c.lo.Excl) {
			continue
		}
		if cmpHi := p.ID.Cmp(c.hi.ID); cmpHi > 0 || (cmpHi == 0 && c.hi.Excl) {
			continue
		}
		if c.consumer != "" && p.Consumer != c.consumer {
			continue
		}
		if now-p.DeliveredMS < c.minIdle {
			continue
		}
		out = append(out, p)
		if c.count > 0 && len(out) == c.count {
			break
		}
	}
	if c.count <= 0 {
		out = nil
	}
	w.WriteArray(len(out))
	for _, p := range out {
		w.WriteArray(4)
		w.WriteBulkString(p.ID.String())
		w.WriteBulkString(p.Consumer)
		w.WriteInt64(now - p.DeliveredMS)
		w.WriteInt(p.Count)
	}
	return out, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// ---- XCLAIM / XAUTOCLAIM ----

// XClaim takes over pending entries idle for at least min-idle-time.
// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix]
//
//	[RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]
//
// https://redis.io/commands/xclaim
type XClaim struct {
	redis.BaseCmd
	key, group, consumer string
	ids                  []restypes.StreamID
	opts                 restypes.ClaimOpts
}

func ParseXClaim(b redis.BaseCmd) (XClaim, error) {
	a := b.Args()
	if len(a) < 5 {
		return XClaim{}, errArgs
	}
	c := XClaim{BaseCmd: b, key: string(a[0]), group: string(a[1]), consumer: string(a[2])}
	c.opts.RetryCount = -1
	idle, err := strconv.ParseInt(string(a[3]), 10, 64)
	if err != nil {
		return XClaim{}, errors.New("ERR Invalid min-idle-time argument for XCLAIM")
	}
	c.opts.MinIdleMS = max(idle, 0)
	rest := a[4:]
	for len(rest) > 0 { // IDs run until the first option keyword
		id, err := parseID(string(rest[0]), 0)
		if err != nil {
			break
		}
		c.ids = append(c.ids, id)
		rest = rest[1:]
	}
	if len(c.ids) == 0 {
		return XClaim{}, errBadID
	}
	for len(rest) > 0 {
		kw := strings.ToLower(string(rest[0]))
		switch {
		case kw == "force":
			c.opts.Force, rest = true, rest[1:]
		case kw == "justid":
			c.opts.JustID, rest = true, rest[1:]
		case (kw == "idle" || kw == "time" || kw == "retrycount" || kw == "lastid") && len(rest) >= 2:
			if kw != "lastid" {
				v, err := strconv.ParseInt(string(rest[1]), 10, 64)
				if err != nil {
					return XClaim{}, redis.ErrInvalidInt
				}
				switch kw {
				case "idle":
					c.opts.IdleMS = v
				case "time":
					c.opts.TimeMS = v
				case "retrycount":
					c.opts.RetryCount = int(v)
				}
			}
			rest = rest[2:]
		default:
			return XClaim{}, redis.ErrSyntaxError
		}
	}
	return c, nil
}

func (c XClaim) Run(w redis.Writer, red redis.Redka) (any, error) {
	claimed, _, err := red.Stream().Claim(c.key, c.group, c.consumer, c.ids, c.opts)
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	if c.opts.JustID {
		w.WriteArray(len(claimed))
		for _, e := range claimed {
			w.WriteBulkString(e.ID.String())
		}
		return claimed, nil
	}
	writeEntries(w, claimed)
	return claimed, nil
}

// XAutoClaim claims idle pending entries starting at a cursor.
// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
// https://redis.io/commands/xautoclaim
type XAutoClaim struct {
	redis.BaseCmd
	key, group, consumer string
	minIdle              int64
	start                restypes.StreamID
	count                int
	justID               bool
}

func ParseXAutoClaim(b redis.BaseCmd) (XAutoClaim, error) {
	a := b.Args()
	if len(a) < 5 {
		return XAutoClaim{}, errArgs
	}
	c := XAutoClaim{BaseCmd: b, key: string(a[0]), group: string(a[1]), consumer: string(a[2]), count: 100}
	var err error
	if c.minIdle, err = strconv.ParseInt(string(a[3]), 10, 64); err != nil {
		return XAutoClaim{}, errors.New("ERR Invalid min-idle-time argument for XAUTOCLAIM")
	}
	if c.start, err = parseID(string(a[4]), 0); err != nil {
		return XAutoClaim{}, err
	}
	rest := a[5:]
	for len(rest) > 0 {
		switch strings.ToLower(string(rest[0])) {
		case "justid":
			c.justID, rest = true, rest[1:]
		case "count":
			if len(rest) < 2 {
				return XAutoClaim{}, redis.ErrSyntaxError
			}
			n, err := strconv.Atoi(string(rest[1]))
			if err != nil || n < 1 {
				return XAutoClaim{}, errors.New("ERR COUNT must be > 0")
			}
			c.count, rest = n, rest[2:]
		default:
			return XAutoClaim{}, redis.ErrSyntaxError
		}
	}
	return c, nil
}

func (c XAutoClaim) Run(w redis.Writer, red redis.Redka) (any, error) {
	g, err := red.Stream().Group(c.key, c.group)
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	now := nowMS()
	var ids []restypes.StreamID
	next := restypes.StreamID{} // 0-0: scan finished
	for i, p := range g.Pending {
		if p.ID.Cmp(c.start) < 0 || now-p.DeliveredMS < c.minIdle {
			continue
		}
		if len(ids) == c.count {
			next = g.Pending[i].ID
			break
		}
		ids = append(ids, p.ID)
	}
	claimed, deleted, err := red.Stream().Claim(c.key, c.group, c.consumer, ids,
		restypes.ClaimOpts{MinIdleMS: c.minIdle, RetryCount: -1, JustID: c.justID})
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteArray(3)
	w.WriteBulkString(next.String())
	if c.justID {
		w.WriteArray(len(claimed))
		for _, e := range claimed {
			w.WriteBulkString(e.ID.String())
		}
	} else {
		writeEntries(w, claimed)
	}
	w.WriteArray(len(deleted))
	for _, id := range deleted {
		w.WriteBulkString(id.String())
	}
	return claimed, nil
}

// ---- XINFO ----

// XInfo describes streams, groups and consumers.
// XINFO STREAM key [FULL [COUNT n]] | XINFO GROUPS key | XINFO CONSUMERS key group
// https://redis.io/commands/xinfo
type XInfo struct {
	redis.BaseCmd
	sub, key, group string
}

func ParseXInfo(b redis.BaseCmd) (XInfo, error) {
	a := b.Args()
	if len(a) < 2 {
		return XInfo{}, errArgs
	}
	c := XInfo{BaseCmd: b, sub: strings.ToLower(string(a[0])), key: string(a[1])}
	switch c.sub {
	case "stream", "groups":
	case "consumers":
		if len(a) != 3 {
			return XInfo{}, errArgs
		}
		c.group = string(a[2])
	default:
		return XInfo{}, redis.ErrUnknownSubcmd
	}
	return c, nil
}

// pairs writes a flat map: alternating names and values (int, int64, string,
// nil or *restypes.StreamEntry).
func pairs(w redis.Writer, kv ...any) {
	w.WriteMap(len(kv) / 2)
	for i := 0; i < len(kv); i += 2 {
		w.WriteBulkString(kv[i].(string))
		switch v := kv[i+1].(type) {
		case nil:
			w.WriteNull()
		case int:
			w.WriteInt(v)
		case int64:
			w.WriteInt64(v)
		case string:
			w.WriteBulkString(v)
		case *restypes.StreamEntry:
			if v == nil {
				w.WriteNull()
			} else {
				writeEntry(w, *v)
			}
		}
	}
}

func (c XInfo) Run(w redis.Writer, red redis.Redka) (any, error) {
	rs := red.Stream()
	fail := func(err error) (any, error) {
		if err == redis.ErrNoGroup && c.sub != "consumers" {
			err = redis.ErrNotFound
		}
		w.WriteError(c.Error(err))
		return nil, err
	}
	switch c.sub {
	case "groups":
		gs, err := rs.Groups(c.key)
		if err != nil {
			return fail(err)
		}
		w.WriteArray(len(gs))
		for _, g := range gs {
			pairs(w, "name", g.Name, "consumers", len(g.Consumers), "pending", len(g.Pending),
				"last-delivered-id", g.LastID.String(), "entries-read", nil, "lag", nil)
		}
	case "consumers":
		g, err := rs.Group(c.key, c.group)
		if err != nil {
			return fail(err)
		}
		now := nowMS()
		w.WriteArray(len(g.Consumers))
		for _, co := range g.Consumers {
			pairs(w, "name", co.Name, "pending", co.Pending, "idle", max(now-co.SeenMS, 0))
		}
	case "stream":
		n, err := rs.Len(c.key)
		if err != nil {
			return fail(err)
		}
		if ok, _ := red.Key().Exists(c.key); !ok {
			return fail(redis.ErrNotFound)
		}
		last, _ := rs.LastID(c.key)
		gs, _ := rs.Groups(c.key)
		var first, lastE *restypes.StreamEntry
		if es, _ := rs.Range(c.key, restypes.StreamBound{}, maxBound(), 1, false); len(es) > 0 {
			first = &es[0]
		}
		if es, _ := rs.Range(c.key, restypes.StreamBound{}, maxBound(), 1, true); len(es) > 0 {
			lastE = &es[0]
		}
		firstID := "0-0"
		if first != nil {
			firstID = first.ID.String()
		}
		pairs(w, "length", n, "radix-tree-keys", 1, "radix-tree-nodes", 2,
			"last-generated-id", last.String(), "max-deleted-entry-id", "0-0", "entries-added", n,
			"recorded-first-entry-id", firstID, "groups", len(gs), "first-entry", first, "last-entry", lastE)
	}
	return nil, nil
}
