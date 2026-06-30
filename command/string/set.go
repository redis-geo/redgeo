package string

import (
	"time"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/parser"
	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

// Set sets the string value of a key.
// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
// https://redis.io/commands/set
type Set struct {
	redis.BaseCmd
	key     string
	value   []byte
	ifNX    bool
	ifXX    bool
	get     bool
	ttl     time.Duration
	at      time.Time
	keepTTL bool
}

func ParseSet(b redis.BaseCmd) (Set, error) {
	cmd := Set{BaseCmd: b}
	var ttlSec, ttlMs, atSec, atMs int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.value),
		parser.OneOf(
			parser.Flag("nx", &cmd.ifNX),
			parser.Flag("xx", &cmd.ifXX),
		),
		parser.Flag("get", &cmd.get),
		parser.OneOf(
			parser.Named("ex", parser.Int(&ttlSec)),
			parser.Named("px", parser.Int(&ttlMs)),
			parser.Named("exat", parser.Int(&atSec)),
			parser.Named("pxat", parser.Int(&atMs)),
			parser.Flag("keepttl", &cmd.keepTTL),
		),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return Set{}, err
	}
	if ttlSec > 0 {
		cmd.ttl = time.Duration(ttlSec) * time.Second
	} else if ttlMs > 0 {
		cmd.ttl = time.Duration(ttlMs) * time.Millisecond
	} else if atSec > 0 {
		cmd.at = time.Unix(int64(atSec), 0)
	} else if atMs > 0 {
		cmd.at = time.Unix(0, int64(atMs)*int64(time.Millisecond))
	}
	if cmd.ttl < 0 {
		return Set{}, redis.ErrInvalidExpireTime
	}
	return cmd, nil
}

func (cmd Set) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Simple SET (only an optional relative TTL).
	if !cmd.ifNX && !cmd.ifXX && !cmd.get && !cmd.keepTTL && cmd.at.IsZero() {
		if err := red.Str().SetExpire(cmd.key, cmd.value, cmd.ttl); err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		w.WriteString("OK")
		return true, nil
	}

	opts := restypes.SetOpts{
		IfNotExists: cmd.ifNX,
		IfExists:    cmd.ifXX,
		Get:         cmd.get,
		KeepTTL:     cmd.keepTTL,
	}
	switch {
	case cmd.ttl > 0:
		opts.TTLMS = cmd.ttl.Milliseconds()
	case !cmd.at.IsZero():
		opts.AtMS = cmd.at.UnixMilli()
	}

	out, err := red.Str().SetWith(cmd.key, cmd.value, opts)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	var ok bool
	switch {
	case cmd.ifXX:
		ok = out.Updated
	case cmd.ifNX:
		ok = out.Created
	default:
		ok = true
	}

	if cmd.get {
		if out.Created {
			w.WriteNull()
			return core.Value(nil), nil
		}
		w.WriteBulk(out.Prev)
		return out.Prev, nil
	}
	if !ok {
		w.WriteNull()
		return false, nil
	}
	w.WriteString("OK")
	return true, nil
}
