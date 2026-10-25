package string

import (
	"github.com/redis-geo/redgeo/parser"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// PFAdd adds elements to a HyperLogLog, creating it if needed.
// PFADD key [element [element ...]]
// https://redis.io/commands/pfadd
type PFAdd struct {
	redis.BaseCmd
	key   string
	elems [][]byte
}

func ParsePFAdd(b redis.BaseCmd) (PFAdd, error) {
	cmd := PFAdd{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return PFAdd{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.elems = cmd.Args()[1:]
	return cmd, nil
}

func (cmd PFAdd) Run(w redis.Writer, red redis.Redka) (any, error) {
	changed, err := red.Str().PFAdd(cmd.key, cmd.elems...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	n := 0
	if changed {
		n = 1
	}
	w.WriteInt(n)
	return n, nil
}

// PFCount returns the approximate cardinality of the union of HyperLogLogs.
// PFCOUNT key [key ...]
// https://redis.io/commands/pfcount
type PFCount struct {
	redis.BaseCmd
	keys []string
}

func ParsePFCount(b redis.BaseCmd) (PFCount, error) {
	cmd := PFCount{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.keys),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return PFCount{}, err
	}
	return cmd, nil
}

func (cmd PFCount) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Str().PFCount(cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(int(n))
	return n, nil
}

// PFMerge merges HyperLogLogs into a destination key.
// PFMERGE destkey [sourcekey [sourcekey ...]]
// https://redis.io/commands/pfmerge
type PFMerge struct {
	redis.BaseCmd
	dest string
	srcs []string
}

func ParsePFMerge(b redis.BaseCmd) (PFMerge, error) {
	cmd := PFMerge{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return PFMerge{}, redis.ErrInvalidArgNum
	}
	cmd.dest = string(cmd.Args()[0])
	for _, a := range cmd.Args()[1:] {
		cmd.srcs = append(cmd.srcs, string(a))
	}
	return cmd, nil
}

func (cmd PFMerge) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Str().PFMerge(cmd.dest, cmd.srcs...); err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
