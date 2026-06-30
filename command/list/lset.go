package list

import (
	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/parser"
	"github.com/redis-geo/redgeo/redisapi"
)

// Sets the value of an element in a list by its index.
// LSET key index element
// https://redis.io/commands/lset
type LSet struct {
	redis.BaseCmd
	key   string
	index int
	elem  []byte
}

func ParseLSet(b redis.BaseCmd) (LSet, error) {
	cmd := LSet{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.index),
		parser.Bytes(&cmd.elem),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return LSet{}, err
	}
	return cmd, nil
}

func (cmd LSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	err := red.List().Set(cmd.key, cmd.index, cmd.elem)
	if err == core.ErrNotFound {
		w.WriteError(cmd.Error(redis.ErrOutOfRange))
		return nil, err
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return nil, nil
}
