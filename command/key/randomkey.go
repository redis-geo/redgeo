package key

import (
	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/redisapi"
)

// Returns a random key name from the database.
// RANDOMKEY
// https://redis.io/commands/randomkey
type RandomKey struct {
	redis.BaseCmd
}

func ParseRandomKey(b redis.BaseCmd) (RandomKey, error) {
	cmd := RandomKey{BaseCmd: b}
	if len(cmd.Args()) != 0 {
		return RandomKey{}, redis.ErrInvalidArgNum
	}
	return cmd, nil
}

func (cmd RandomKey) Run(w redis.Writer, red redis.Redka) (any, error) {
	key, err := red.Key().Random()
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulkString(key.Key)
	return key, nil
}
