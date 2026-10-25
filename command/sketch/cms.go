package sketch

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

var (
	errCMSNoKey  = errors.New("CMS: key does not exist")
	errCMSExists = errors.New("CMS: key already exists")
)

// cmsErr maps backend errors to RedisBloom's messages.
func cmsErr(err error) error {
	switch err {
	case core.ErrNotFound:
		return errCMSNoKey
	case redis.ErrSketchExists:
		return errCMSExists
	}
	return err
}

func parsePositive(b []byte, what string) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || n < 1 {
		return 0, errors.New("CMS: invalid " + what)
	}
	return n, nil
}

// CMSInit creates a count-min sketch from explicit dimensions or from an error
// bound and probability.
// CMS.INITBYDIM key width depth
// CMS.INITBYPROB key error probability
// https://redis.io/commands/cms.initbydim
type CMSInit struct {
	redis.BaseCmd
	key          string
	width, depth int
}

func ParseCMSInitByDim(b redis.BaseCmd) (CMSInit, error) {
	a := b.Args()
	if len(a) != 3 {
		return CMSInit{}, argErr()
	}
	w, err := parsePositive(a[1], "width")
	if err != nil {
		return CMSInit{}, err
	}
	d, err := parsePositive(a[2], "depth")
	if err != nil {
		return CMSInit{}, err
	}
	return CMSInit{BaseCmd: b, key: string(a[0]), width: int(w), depth: int(d)}, nil
}

func ParseCMSInitByProb(b redis.BaseCmd) (CMSInit, error) {
	a := b.Args()
	if len(a) != 3 {
		return CMSInit{}, argErr()
	}
	e, err1 := strconv.ParseFloat(string(a[1]), 64)
	p, err2 := strconv.ParseFloat(string(a[2]), 64)
	if err1 != nil || !(e > 0 && e < 1) {
		return CMSInit{}, errors.New("CMS: invalid overestimation value")
	}
	if err2 != nil || !(p > 0 && p < 1) {
		return CMSInit{}, errors.New("CMS: invalid prob value")
	}
	return CMSInit{
		BaseCmd: b, key: string(a[0]),
		width: int(math.Ceil(2 / e)),
		depth: int(math.Ceil(math.Log(p) / math.Log(0.5))),
	}, nil
}

func (c CMSInit) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Sketch().CMSInit(c.key, c.width, c.depth); err != nil {
		err = cmsErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}

// CMSIncrBy adds to the counts of items.
// CMS.INCRBY key item increment [item increment ...]
// https://redis.io/commands/cms.incrby
type CMSIncrBy struct {
	redis.BaseCmd
	key   string
	items [][]byte
	incs  []int64
}

func ParseCMSIncrBy(b redis.BaseCmd) (CMSIncrBy, error) {
	a := b.Args()
	if len(a) < 3 || len(a)%2 != 1 {
		return CMSIncrBy{}, argErr()
	}
	c := CMSIncrBy{BaseCmd: b, key: string(a[0])}
	for i := 1; i < len(a); i += 2 {
		n, err := strconv.ParseInt(string(a[i+1]), 10, 64)
		if err != nil || n < 1 {
			return CMSIncrBy{}, errors.New("CMS: Increment must be a positive integer")
		}
		c.items, c.incs = append(c.items, a[i]), append(c.incs, n)
	}
	return c, nil
}

func (c CMSIncrBy) Run(w redis.Writer, red redis.Redka) (any, error) {
	counts, err := red.Sketch().CMSIncr(c.key, c.items, c.incs)
	if err != nil {
		err = cmsErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteArray(len(counts))
	for _, n := range counts {
		w.WriteInt64(n)
	}
	return counts, nil
}

// CMSQuery returns the estimated counts of items.
// CMS.QUERY key item [item ...]
// https://redis.io/commands/cms.query
type CMSQuery struct {
	redis.BaseCmd
	key   string
	items [][]byte
}

func ParseCMSQuery(b redis.BaseCmd) (CMSQuery, error) {
	a := b.Args()
	if len(a) < 2 {
		return CMSQuery{}, argErr()
	}
	return CMSQuery{BaseCmd: b, key: string(a[0]), items: a[1:]}, nil
}

func (c CMSQuery) Run(w redis.Writer, red redis.Redka) (any, error) {
	counts, err := red.Sketch().CMSQuery(c.key, c.items)
	if err != nil {
		err = cmsErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteArray(len(counts))
	for _, n := range counts {
		w.WriteInt64(n)
	}
	return counts, nil
}

// CMSMerge sums several sketches into a destination sketch.
// CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight [weight ...]]
// https://redis.io/commands/cms.merge
type CMSMerge struct {
	redis.BaseCmd
	dest    string
	srcs    []string
	weights []int64
}

func ParseCMSMerge(b redis.BaseCmd) (CMSMerge, error) {
	a := b.Args()
	if len(a) < 3 {
		return CMSMerge{}, argErr()
	}
	n, err := strconv.Atoi(string(a[1]))
	if err != nil || n < 1 || n > len(a)-2 {
		return CMSMerge{}, errors.New("CMS: invalid numKeys")
	}
	c := CMSMerge{BaseCmd: b, dest: string(a[0])}
	for _, s := range a[2 : 2+n] {
		c.srcs = append(c.srcs, string(s))
	}
	rest := a[2+n:]
	if len(rest) == 0 {
		for range c.srcs {
			c.weights = append(c.weights, 1)
		}
		return c, nil
	}
	if !strings.EqualFold(string(rest[0]), "weights") || len(rest)-1 != n {
		return CMSMerge{}, errors.New("CMS: wrong number of weights")
	}
	for _, x := range rest[1:] {
		w, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return CMSMerge{}, errors.New("CMS: invalid weight value")
		}
		c.weights = append(c.weights, w)
	}
	return c, nil
}

func (c CMSMerge) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Sketch().CMSMerge(c.dest, c.srcs, c.weights); err != nil {
		err = cmsErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}

// CMSInfo describes a sketch.
// CMS.INFO key
// https://redis.io/commands/cms.info
type CMSInfo struct {
	redis.BaseCmd
	key string
}

func ParseCMSInfo(b redis.BaseCmd) (CMSInfo, error) {
	if len(b.Args()) != 1 {
		return CMSInfo{}, argErr()
	}
	return CMSInfo{BaseCmd: b, key: string(b.Args()[0])}, nil
}

func (c CMSInfo) Run(w redis.Writer, red redis.Redka) (any, error) {
	width, depth, count, err := red.Sketch().CMSInfo(c.key)
	if err != nil {
		err = cmsErr(err)
		w.WriteError(c.Error(err))
		return nil, err
	}
	w.WriteMap(3)
	w.WriteBulkString("width")
	w.WriteInt(width)
	w.WriteBulkString("depth")
	w.WriteInt(depth)
	w.WriteBulkString("count")
	w.WriteInt64(count)
	return nil, nil
}
