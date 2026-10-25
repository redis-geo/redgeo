package sketch

import (
	"errors"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

var (
	errTopKNoKey  = errors.New("TOPK: key does not exist")
	errTopKExists = errors.New("TOPK: key already exists")
)

func topkErr(err error) error {
	switch err {
	case core.ErrNotFound:
		return errTopKNoKey
	case redis.ErrSketchExists:
		return errTopKExists
	}
	return err
}

// TopKReserve creates a top-k sketch.
// TOPK.RESERVE key topk [width depth decay]
// https://redis.io/commands/topk.reserve
type TopKReserve struct {
	redis.BaseCmd
	key             string
	k, width, depth int
	decay           float64
}

func ParseTopKReserve(b redis.BaseCmd) (TopKReserve, error) {
	a := b.Args()
	if len(a) != 2 && len(a) != 5 {
		return TopKReserve{}, argErr()
	}
	c := TopKReserve{BaseCmd: b, key: string(a[0]), width: 8, depth: 7, decay: 0.9}
	k, err := strconv.Atoi(string(a[1]))
	if err != nil || k < 1 {
		return TopKReserve{}, errors.New("TOPK: invalid k")
	}
	c.k = k
	if len(a) == 5 {
		w, e1 := strconv.Atoi(string(a[2]))
		d, e2 := strconv.Atoi(string(a[3]))
		dec, e3 := strconv.ParseFloat(string(a[4]), 64)
		if e1 != nil || w < 1 {
			return TopKReserve{}, errors.New("TOPK: invalid width")
		}
		if e2 != nil || d < 1 {
			return TopKReserve{}, errors.New("TOPK: invalid depth")
		}
		if e3 != nil || !(dec > 0 && dec <= 1) {
			return TopKReserve{}, errors.New("TOPK: invalid decay value. must be between 0 and 1")
		}
		c.width, c.depth, c.decay = w, d, dec
	}
	return c, nil
}

func (c TopKReserve) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Sketch().TopKReserve(c.key, c.k, c.width, c.depth, c.decay); err != nil {
		err = topkErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}

// TopKAdd counts items; TopKIncrBy counts them with explicit increments. Each
// replies with the item expelled from the top-k by that update, or nil.
// TOPK.ADD key item [item ...]
// TOPK.INCRBY key item increment [item increment ...]
// https://redis.io/commands/topk.add
type TopKAdd struct {
	redis.BaseCmd
	key   string
	items [][]byte
	incs  []int64
}

func ParseTopKAdd(b redis.BaseCmd) (TopKAdd, error) {
	a := b.Args()
	if len(a) < 2 {
		return TopKAdd{}, argErr()
	}
	c := TopKAdd{BaseCmd: b, key: string(a[0]), items: a[1:]}
	for range c.items {
		c.incs = append(c.incs, 1)
	}
	return c, nil
}

func ParseTopKIncrBy(b redis.BaseCmd) (TopKAdd, error) {
	a := b.Args()
	if len(a) < 3 || len(a)%2 != 1 {
		return TopKAdd{}, argErr()
	}
	c := TopKAdd{BaseCmd: b, key: string(a[0])}
	for i := 1; i < len(a); i += 2 {
		n, err := strconv.ParseInt(string(a[i+1]), 10, 64)
		if err != nil || n < 1 || n > 100000 {
			return TopKAdd{}, errors.New("TOPK: invalid increment value")
		}
		c.items, c.incs = append(c.items, a[i]), append(c.incs, n)
	}
	return c, nil
}

func (c TopKAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	expelled, err := red.Sketch().TopKAdd(c.key, c.items, c.incs)
	if err != nil {
		err = topkErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteArray(len(expelled))
	for _, e := range expelled {
		if e == nil {
			w.WriteNull()
		} else {
			w.WriteBulk(e)
		}
	}
	return expelled, nil
}

// TopKQuery tests whether items are in the top-k; TopKCount estimates counts.
// TOPK.QUERY key item [item ...]
// TOPK.COUNT key item [item ...]
// https://redis.io/commands/topk.query
type TopKQuery struct {
	redis.BaseCmd
	key   string
	items [][]byte
	count bool
}

func ParseTopKQuery(b redis.BaseCmd, count bool) (TopKQuery, error) {
	a := b.Args()
	if len(a) < 2 {
		return TopKQuery{}, argErr()
	}
	return TopKQuery{BaseCmd: b, key: string(a[0]), items: a[1:], count: count}, nil
}

func (c TopKQuery) Run(w redis.Writer, red redis.Redka) (any, error) {
	if c.count {
		counts, err := red.Sketch().TopKCount(c.key, c.items)
		if err != nil {
			err = topkErr(err)
			w.WriteError(c.Error(err))
			return nil, err
		}
		w.WriteArray(len(counts))
		for _, n := range counts {
			w.WriteInt64(n)
		}
		return counts, nil
	}
	in, err := red.Sketch().TopKQuery(c.key, c.items)
	if err != nil {
		err = topkErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	writeBools(w, in)
	return in, nil
}

// TopKList returns the top-k items, heaviest first.
// TOPK.LIST key [WITHCOUNT]
// https://redis.io/commands/topk.list
type TopKList struct {
	redis.BaseCmd
	key       string
	withCount bool
}

func ParseTopKList(b redis.BaseCmd) (TopKList, error) {
	a := b.Args()
	if len(a) < 1 || len(a) > 2 {
		return TopKList{}, argErr()
	}
	c := TopKList{BaseCmd: b, key: string(a[0])}
	if len(a) == 2 {
		if !strings.EqualFold(string(a[1]), "withcount") {
			return TopKList{}, redis.ErrSyntaxError
		}
		c.withCount = true
	}
	return c, nil
}

func (c TopKList) Run(w redis.Writer, red redis.Redka) (any, error) {
	list, err := red.Sketch().TopKList(c.key)
	if err != nil {
		err = topkErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	if c.withCount {
		w.WriteArray(len(list) * 2)
		for _, e := range list {
			w.WriteBulk(e.Item)
			w.WriteInt64(e.Count)
		}
		return list, nil
	}
	w.WriteArray(len(list))
	for _, e := range list {
		w.WriteBulk(e.Item)
	}
	return list, nil
}

// TopKInfo describes a top-k sketch.
// TOPK.INFO key
// https://redis.io/commands/topk.info
type TopKInfo struct {
	redis.BaseCmd
	key string
}

func ParseTopKInfo(b redis.BaseCmd) (TopKInfo, error) {
	if len(b.Args()) != 1 {
		return TopKInfo{}, argErr()
	}
	return TopKInfo{BaseCmd: b, key: string(b.Args()[0])}, nil
}

func (c TopKInfo) Run(w redis.Writer, red redis.Redka) (any, error) {
	info, err := red.Sketch().TopKInfo(c.key)
	if err != nil {
		err = topkErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteMap(4)
	w.WriteBulkString("k")
	w.WriteInt(info.K)
	w.WriteBulkString("width")
	w.WriteInt(info.Width)
	w.WriteBulkString("depth")
	w.WriteInt(info.Depth)
	w.WriteBulkString("decay")
	w.WriteBulkString(strconv.FormatFloat(info.Decay, 'g', -1, 64))
	return info, nil
}
