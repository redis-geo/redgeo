package hash

import "github.com/redis-geo/redgeo/redisapi"

// Returns all fields and values in a hash.
// HGETALL key
// https://redis.io/commands/hgetall
type HGetAll struct {
	redis.BaseCmd
	key string
}

func ParseHGetAll(b redis.BaseCmd) (HGetAll, error) {
	cmd := HGetAll{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return HGetAll{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd HGetAll) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.Hash().Items(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	// RESP3 returns a map; RESP2 a flat array (WriteMap handles both).
	w.WriteMap(len(items))
	for field, val := range items {
		w.WriteBulkString(field)
		w.WriteBulk(val)
	}
	return items, nil
}
