package key

import (
	"time"

	"github.com/redis-geo/redgeo/core"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// PTTL returns the remaining time to live of a key in milliseconds.
// PTTL key
// https://redis.io/commands/pttl
type PTTL struct {
	redis.BaseCmd
	key string
}

func ParsePTTL(b redis.BaseCmd) (PTTL, error) {
	cmd := PTTL{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return PTTL{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd PTTL) Run(w redis.Writer, red redis.Redka) (any, error) {
	k, err := red.Key().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteInt(-2)
		return -2, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if k.ETime == nil {
		w.WriteInt(-1)
		return -1, nil
	}
	pttl := int(*k.ETime - time.Now().UnixMilli())
	w.WriteInt(pttl)
	return pttl, nil
}
