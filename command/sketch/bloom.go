// Package sketch implements the probabilistic-sketch commands (RedisBloom
// subset): BF.* Bloom filters, CMS.* count-min sketches and TOPK.* top-k.
package sketch

import (
	"errors"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

var errNotFound = errors.New("ERR not found")

func argErr() error { return redis.ErrInvalidArgNum }

func parseFloat(b []byte) (float64, error) {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, errors.New("ERR bad error rate")
	}
	return f, nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, redis.ErrInvalidInt
	}
	return n, nil
}

func writeBools(w redis.Writer, bs []bool) {
	w.WriteArray(len(bs))
	for _, b := range bs {
		if b {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}
	}
}

// ---- BF.RESERVE ----

// BFReserve creates an empty Bloom filter.
// BF.RESERVE key error_rate capacity [EXPANSION expansion | NONSCALING]
// https://redis.io/commands/bf.reserve
type BFReserve struct {
	redis.BaseCmd
	key  string
	opts restypes.BloomOpts
}

func ParseBFReserve(b redis.BaseCmd) (BFReserve, error) {
	a := b.Args()
	if len(a) < 3 {
		return BFReserve{}, argErr()
	}
	c := BFReserve{BaseCmd: b, key: string(a[0])}
	var err error
	if c.opts.ErrorRate, err = parseFloat(a[1]); err != nil {
		return BFReserve{}, err
	}
	if c.opts.Capacity, err = parseInt(a[2]); err != nil {
		return BFReserve{}, err
	}
	c.opts.Expansion = 2
	for rest := a[3:]; len(rest) > 0; {
		switch strings.ToLower(string(rest[0])) {
		case "nonscaling":
			c.opts.Expansion, rest = 0, rest[1:]
		case "expansion":
			if len(rest) < 2 {
				return BFReserve{}, redis.ErrSyntaxError
			}
			n, err := parseInt(rest[1])
			if err != nil || n < 1 {
				return BFReserve{}, errors.New("ERR bad expansion")
			}
			c.opts.Expansion, rest = int(n), rest[2:]
		default:
			return BFReserve{}, redis.ErrSyntaxError
		}
	}
	return c, nil
}

func (c BFReserve) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Sketch().BFReserve(c.key, c.opts); err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}

// ---- BF.ADD / BF.MADD / BF.INSERT ----

func defaultBloom() *restypes.BloomOpts {
	return &restypes.BloomOpts{ErrorRate: 0.01, Capacity: 100, Expansion: 2}
}

// BFAdd adds one or more items, creating the filter if needed.
// BF.ADD key item
// BF.MADD key item [item ...]
// BF.INSERT key [CAPACITY cap] [ERROR error] [EXPANSION expansion] [NOCREATE]
//
//	[NONSCALING] ITEMS item [item ...]
//
// https://redis.io/commands/bf.add
type BFAdd struct {
	redis.BaseCmd
	key    string
	items  [][]byte
	create *restypes.BloomOpts
	single bool // BF.ADD replies with an integer, not an array
}

func ParseBFAdd(b redis.BaseCmd, single bool) (BFAdd, error) {
	a := b.Args()
	if len(a) < 2 || (single && len(a) != 2) {
		return BFAdd{}, argErr()
	}
	return BFAdd{BaseCmd: b, key: string(a[0]), items: a[1:], create: defaultBloom(), single: single}, nil
}

func ParseBFInsert(b redis.BaseCmd) (BFAdd, error) {
	a := b.Args()
	if len(a) < 3 {
		return BFAdd{}, argErr()
	}
	c := BFAdd{BaseCmd: b, key: string(a[0]), create: defaultBloom()}
	rest := a[1:]
	for len(rest) > 0 {
		kw := strings.ToLower(string(rest[0]))
		switch {
		case kw == "items":
			if len(rest) < 2 {
				return BFAdd{}, redis.ErrSyntaxError
			}
			c.items = rest[1:]
			return c, nil
		case kw == "nocreate":
			c.create, rest = nil, rest[1:]
		case kw == "nonscaling":
			if c.create != nil {
				c.create.Expansion = 0
			}
			rest = rest[1:]
		case (kw == "capacity" || kw == "error" || kw == "expansion") && len(rest) >= 2:
			var err error
			if c.create != nil {
				switch kw {
				case "capacity":
					c.create.Capacity, err = parseInt(rest[1])
				case "error":
					c.create.ErrorRate, err = parseFloat(rest[1])
				case "expansion":
					var n int64
					if n, err = parseInt(rest[1]); err == nil {
						c.create.Expansion = int(n)
					}
				}
			}
			if err != nil {
				return BFAdd{}, err
			}
			rest = rest[2:]
		default:
			return BFAdd{}, redis.ErrSyntaxError
		}
	}
	return BFAdd{}, errors.New("ERR Missing ITEMS")
}

func (c BFAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	added, err := red.Sketch().BFAdd(c.key, c.items, c.create)
	if err == core.ErrNotFound {
		err = errNotFound
	}
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	if c.single {
		if added[0] {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}
		return added, nil
	}
	writeBools(w, added)
	return added, nil
}

// ---- BF.EXISTS / BF.MEXISTS ----

// BFExists tests whether items were (probably) added.
// BF.EXISTS key item
// BF.MEXISTS key item [item ...]
// https://redis.io/commands/bf.exists
type BFExists struct {
	redis.BaseCmd
	key    string
	items  [][]byte
	single bool
}

func ParseBFExists(b redis.BaseCmd, single bool) (BFExists, error) {
	a := b.Args()
	if len(a) < 2 || (single && len(a) != 2) {
		return BFExists{}, argErr()
	}
	return BFExists{BaseCmd: b, key: string(a[0]), items: a[1:], single: single}, nil
}

func (c BFExists) Run(w redis.Writer, red redis.Redka) (any, error) {
	found, err := red.Sketch().BFExists(c.key, c.items)
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	if c.single {
		if found[0] {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}
		return found, nil
	}
	writeBools(w, found)
	return found, nil
}

// ---- BF.INFO / BF.CARD ----

// BFInfo describes a Bloom filter.
// BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
// BF.CARD key
// https://redis.io/commands/bf.info
type BFInfo struct {
	redis.BaseCmd
	key  string
	only string
	card bool
}

func ParseBFInfo(b redis.BaseCmd, card bool) (BFInfo, error) {
	a := b.Args()
	if len(a) < 1 || (card && len(a) != 1) || len(a) > 2 {
		return BFInfo{}, argErr()
	}
	c := BFInfo{BaseCmd: b, key: string(a[0]), card: card}
	if len(a) == 2 {
		c.only = strings.ToLower(string(a[1]))
		switch c.only {
		case "capacity", "size", "filters", "items", "expansion":
		default:
			return BFInfo{}, errors.New("ERR Invalid information value")
		}
	}
	return c, nil
}

func (c BFInfo) Run(w redis.Writer, red redis.Redka) (any, error) {
	info, err := red.Sketch().BFInfo(c.key)
	if err == core.ErrNotFound {
		if c.card {
			w.WriteInt(0)
			return 0, nil
		}
		err = errNotFound
	}
	if err != nil {
		w.WriteError(c.Error(err))
		return nil, err
	}
	if c.card {
		w.WriteInt64(info.Items)
		return info.Items, nil
	}
	expansion := func() {
		if info.Expansion == 0 {
			w.WriteNull()
		} else {
			w.WriteInt(info.Expansion)
		}
	}
	switch c.only {
	case "capacity":
		w.WriteInt64(info.Capacity)
	case "size":
		w.WriteInt64(info.SizeBytes)
	case "filters":
		w.WriteInt(info.Filters)
	case "items":
		w.WriteInt64(info.Items)
	case "expansion":
		expansion()
	default:
		w.WriteMap(5)
		w.WriteBulkString("Capacity")
		w.WriteInt64(info.Capacity)
		w.WriteBulkString("Size")
		w.WriteInt64(info.SizeBytes)
		w.WriteBulkString("Number of filters")
		w.WriteInt(info.Filters)
		w.WriteBulkString("Number of items inserted")
		w.WriteInt64(info.Items)
		w.WriteBulkString("Expansion rate")
		expansion()
	}
	return info, nil
}
