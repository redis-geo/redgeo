package list

import (
	"github.com/redis-geo/redgeo/parser"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// LPush prepends one or more elements to a list, creating it if needed.
// Like Redis, elements are inserted one-by-one at the head, so the resulting
// order is reversed relative to the argument order.
// LPUSH key element [element ...]
// https://redis.io/commands/lpush
type LPush struct {
	redis.BaseCmd
	key   string
	elems []string
}

func ParseLPush(b redis.BaseCmd) (LPush, error) {
	cmd := LPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.elems),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return LPush{}, err
	}
	return cmd, nil
}

func (cmd LPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	for _, e := range cmd.elems {
		n, err = red.List().PushFront(cmd.key, e)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
	}
	w.WriteInt(n)
	return n, nil
}
