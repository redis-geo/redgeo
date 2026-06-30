package list

import (
	"github.com/redis-geo/redgeo/parser"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// RPush appends one or more elements to a list, creating it if needed.
// RPUSH key element [element ...]
// https://redis.io/commands/rpush
type RPush struct {
	redis.BaseCmd
	key   string
	elems []string
}

func ParseRPush(b redis.BaseCmd) (RPush, error) {
	cmd := RPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Strings(&cmd.elems),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return RPush{}, err
	}
	return cmd, nil
}

func (cmd RPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	for _, e := range cmd.elems {
		n, err = red.List().PushBack(cmd.key, e)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
	}
	w.WriteInt(n)
	return n, nil
}
