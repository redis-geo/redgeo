// Package zset implements the sorted-set commands against the redgeo backend.
// Range commands build a restypes.RangeOpts (redgeo lifted redka's fluent
// RangeCmd builder into a backend-neutral option struct, DESIGN §3).
package zset

import (
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/parser"
	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

func writeItems(w redis.Writer, items []restypes.ZSetItem, withScores bool) {
	if withScores {
		w.WriteArray(len(items) * 2)
		for _, it := range items {
			w.WriteBulk(it.Elem)
			redis.WriteFloat(w, it.Score)
		}
		return
	}
	w.WriteArray(len(items))
	for _, it := range items {
		w.WriteBulk(it.Elem)
	}
}

// ---- ZADD ----

type ZAdd struct {
	redis.BaseCmd
	key   string
	items map[any]float64
}

func ParseZAdd(b redis.BaseCmd) (ZAdd, error) {
	cmd := ZAdd{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.FloatMap(&cmd.items),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZAdd{}, err
	}
	return cmd, nil
}

func (cmd ZAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.ZSet().AddMany(cmd.key, cmd.items)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}

// ---- ZSCORE ----

type ZScore struct {
	redis.BaseCmd
	key  string
	elem string
}

func ParseZScore(b redis.BaseCmd) (ZScore, error) {
	cmd := ZScore{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return ZScore{}, redis.ErrInvalidArgNum
	}
	cmd.key, cmd.elem = string(cmd.Args()[0]), string(cmd.Args()[1])
	return cmd, nil
}

func (cmd ZScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	score, err := red.ZSet().GetScore(cmd.key, cmd.elem)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteDouble(score)
	return score, nil
}

// ---- ZREM ----

type ZRem struct {
	redis.BaseCmd
	key   string
	elems []string
}

func ParseZRem(b redis.BaseCmd) (ZRem, error) {
	cmd := ZRem{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.elems),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZRem{}, err
	}
	return cmd, nil
}

func (cmd ZRem) Run(w redis.Writer, red redis.Redka) (any, error) {
	elems := make([]any, len(cmd.elems))
	for i, e := range cmd.elems {
		elems[i] = e
	}
	n, err := red.ZSet().Delete(cmd.key, elems...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// ---- ZCARD ----

type ZCard struct {
	redis.BaseCmd
	key string
}

func ParseZCard(b redis.BaseCmd) (ZCard, error) {
	cmd := ZCard{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return ZCard{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd ZCard) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Len(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// ---- ZCOUNT ----

type ZCount struct {
	redis.BaseCmd
	key      string
	min, max float64
}

func ParseZCount(b redis.BaseCmd) (ZCount, error) {
	cmd := ZCount{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.min),
		parser.Float(&cmd.max),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZCount{}, err
	}
	return cmd, nil
}

func (cmd ZCount) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.ZSet().Count(cmd.key, cmd.min, cmd.max)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// ---- ZINCRBY ----

type ZIncrBy struct {
	redis.BaseCmd
	key   string
	delta float64
	elem  string
}

func ParseZIncrBy(b redis.BaseCmd) (ZIncrBy, error) {
	cmd := ZIncrBy{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.delta),
		parser.String(&cmd.elem),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZIncrBy{}, err
	}
	return cmd, nil
}

func (cmd ZIncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	score, err := red.ZSet().Incr(cmd.key, cmd.elem, cmd.delta)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteDouble(score)
	return score, nil
}

// ---- ZRANK / ZREVRANK ----

type ZRank struct {
	redis.BaseCmd
	key  string
	elem string
	rev  bool
}

func ParseZRank(b redis.BaseCmd, rev bool) (ZRank, error) {
	cmd := ZRank{BaseCmd: b, rev: rev}
	if len(cmd.Args()) != 2 {
		return ZRank{}, redis.ErrInvalidArgNum
	}
	cmd.key, cmd.elem = string(cmd.Args()[0]), string(cmd.Args()[1])
	return cmd, nil
}

func (cmd ZRank) Run(w redis.Writer, red redis.Redka) (any, error) {
	var rank int
	var err error
	if cmd.rev {
		rank, _, err = red.ZSet().GetRankRev(cmd.key, cmd.elem)
	} else {
		rank, _, err = red.ZSet().GetRank(cmd.key, cmd.elem)
	}
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(rank)
	return rank, nil
}

// ---- ZRANGE (by rank or score, with REV/LIMIT/WITHSCORES) ----

type ZRange struct {
	redis.BaseCmd
	key          string
	start, stop  float64
	byScore, rev bool
	offset, cnt  int
	withScores   bool
}

func ParseZRange(b redis.BaseCmd) (ZRange, error) {
	cmd := ZRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.start),
		parser.Float(&cmd.stop),
		parser.Flag("byscore", &cmd.byScore),
		parser.Flag("rev", &cmd.rev),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.cnt)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRange{}, err
	}
	return cmd, nil
}

func (cmd ZRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	opts := restypes.RangeOpts{Desc: cmd.rev, Offset: cmd.offset, Count: cmd.cnt}
	if cmd.byScore {
		opts.ByScore = true
		opts.Min, opts.Max = cmd.start, cmd.stop
	} else {
		opts.ByRank = true
		opts.Start, opts.Stop = int(cmd.start), int(cmd.stop)
	}
	items, err := red.ZSet().Range(cmd.key, opts)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeItems(w, items, cmd.withScores)
	return items, nil
}

// ---- ZREVRANGE (by rank, descending) ----

type ZRevRange struct {
	redis.BaseCmd
	key         string
	start, stop int
	withScores  bool
}

func ParseZRevRange(b redis.BaseCmd) (ZRevRange, error) {
	cmd := ZRevRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.stop),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRevRange{}, err
	}
	return cmd, nil
}

func (cmd ZRevRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.ZSet().Range(cmd.key, restypes.RangeOpts{
		ByRank: true, Start: cmd.start, Stop: cmd.stop, Desc: true,
	})
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeItems(w, items, cmd.withScores)
	return items, nil
}

// ---- ZRANGEBYSCORE / ZREVRANGEBYSCORE ----

type ZRangeByScore struct {
	redis.BaseCmd
	key         string
	min, max    float64
	rev         bool
	withScores  bool
	offset, cnt int
}

// ParseZRangeByScore parses both forms; rev swaps the min/max argument order.
func ParseZRangeByScore(b redis.BaseCmd, rev bool) (ZRangeByScore, error) {
	cmd := ZRangeByScore{BaseCmd: b, rev: rev}
	var a, bb float64
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&a),
		parser.Float(&bb),
		parser.Flag("withscores", &cmd.withScores),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.cnt)),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRangeByScore{}, err
	}
	if rev {
		cmd.max, cmd.min = a, bb // ZREVRANGEBYSCORE key max min
	} else {
		cmd.min, cmd.max = a, bb
	}
	return cmd, nil
}

func (cmd ZRangeByScore) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.ZSet().Range(cmd.key, restypes.RangeOpts{
		ByScore: true, Min: cmd.min, Max: cmd.max, Desc: cmd.rev,
		Offset: cmd.offset, Count: cmd.cnt,
	})
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	writeItems(w, items, cmd.withScores)
	return items, nil
}

// ---- ZREMRANGEBYRANK / ZREMRANGEBYSCORE ----

type ZRemRange struct {
	redis.BaseCmd
	key     string
	byScore bool
	lo, hi  float64
}

func ParseZRemRangeByRank(b redis.BaseCmd) (ZRemRange, error) {
	return parseZRemRange(b, false)
}
func ParseZRemRangeByScore(b redis.BaseCmd) (ZRemRange, error) {
	return parseZRemRange(b, true)
}

func parseZRemRange(b redis.BaseCmd, byScore bool) (ZRemRange, error) {
	cmd := ZRemRange{BaseCmd: b, byScore: byScore}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.lo),
		parser.Float(&cmd.hi),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRemRange{}, err
	}
	return cmd, nil
}

func (cmd ZRemRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	var opts restypes.RangeOpts
	if cmd.byScore {
		opts = restypes.RangeOpts{ByScore: true, Min: cmd.lo, Max: cmd.hi}
	} else {
		opts = restypes.RangeOpts{ByRank: true, Start: int(cmd.lo), Stop: int(cmd.hi)}
	}
	n, err := red.ZSet().DeleteRange(cmd.key, opts)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}

// ---- ZSCAN ----

type ZScan struct {
	redis.BaseCmd
	key    string
	cursor int
	match  string
	count  int
}

func ParseZScan(b redis.BaseCmd) (ZScan, error) {
	cmd := ZScan{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.cursor),
		parser.Named("match", parser.String(&cmd.match)),
		parser.Named("count", parser.Int(&cmd.count)),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZScan{}, err
	}
	return cmd, nil
}

func (cmd ZScan) Run(w redis.Writer, red redis.Redka) (any, error) {
	res, err := red.ZSet().Scan(cmd.key, cmd.cursor, cmd.match, cmd.count)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(2)
	w.WriteBulkString(strconv.Itoa(res.Cursor))
	w.WriteArray(len(res.Items) * 2)
	for _, it := range res.Items {
		w.WriteBulk(it.Elem)
		redis.WriteFloat(w, it.Score)
	}
	return res, nil
}

// ---- ZUNIONSTORE / ZINTERSTORE ----

type ZStore struct {
	redis.BaseCmd
	dest  string
	keys  []string
	agg   string
	inter bool
}

func ParseZUnionStore(b redis.BaseCmd) (ZStore, error) { return parseZStore(b, false) }
func ParseZInterStore(b redis.BaseCmd) (ZStore, error) { return parseZStore(b, true) }

// parseZStore hand-parses "dest numkeys key [key ...] [AGGREGATE SUM|MIN|MAX]".
func parseZStore(b redis.BaseCmd, inter bool) (ZStore, error) {
	args := b.Args()
	if len(args) < 3 {
		return ZStore{}, redis.ErrInvalidArgNum
	}
	cmd := ZStore{BaseCmd: b, inter: inter, agg: "sum"}
	cmd.dest = string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 {
		return ZStore{}, redis.ErrInvalidInt
	}
	if len(args) < 2+numKeys {
		return ZStore{}, redis.ErrInvalidArgNum
	}
	for i := 0; i < numKeys; i++ {
		cmd.keys = append(cmd.keys, string(args[2+i]))
	}
	// Optional AGGREGATE.
	rest := args[2+numKeys:]
	for i := 0; i < len(rest); i++ {
		if strings.EqualFold(string(rest[i]), "aggregate") && i+1 < len(rest) {
			cmd.agg = string(rest[i+1])
			i++
		}
	}
	return cmd, nil
}

func (cmd ZStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	if cmd.inter {
		n, err = red.ZSet().InterStore(cmd.dest, cmd.agg, cmd.keys...)
	} else {
		n, err = red.ZSet().UnionStore(cmd.dest, cmd.agg, cmd.keys...)
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
