package list

import (
	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/parser"
	"github.com/redis-geo/redgeo/redisapi"
)

// Returns the first element of a list after removing it.
// LPOP key
// https://redis.io/commands/lpop
type LPop struct {
	redis.BaseCmd
	key string
}

func ParseLPop(b redis.BaseCmd) (LPop, error) {
	cmd := LPop{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return LPop{}, err
	}
	return cmd, nil
}

func (cmd LPop) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.List().PopFront(cmd.key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return val, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
