package string

import (
	"fmt"
	"strings"

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

// PFDebug exposes HyperLogLog internals.
// PFDEBUG GETREG key | DECODE key | ENCODING key | TODENSE key
// https://github.com/redis/redis/blob/unstable/src/hyperloglog.c (pfdebugCommand)
type PFDebug struct {
	redis.BaseCmd
	sub, key string
}

func ParsePFDebug(b redis.BaseCmd) (PFDebug, error) {
	a := b.Args()
	if len(a) != 2 {
		return PFDebug{}, redis.ErrInvalidArgNum
	}
	c := PFDebug{BaseCmd: b, sub: strings.ToLower(string(a[0])), key: string(a[1])}
	switch c.sub {
	case "getreg", "decode", "encoding", "todense":
		return c, nil
	}
	return PFDebug{}, fmt.Errorf("ERR Unknown PFDEBUG subcommand '%s'", a[0])
}

func (c PFDebug) Run(w redis.Writer, red redis.Redka) (any, error) {
	rs := red.Str()
	fail := func(err error) (any, error) {
		w.WriteError(c.Error(err))
		return nil, err
	}
	switch c.sub {
	case "getreg":
		regs, err := rs.PFRegisters(c.key)
		if err != nil {
			return fail(err)
		}
		w.WriteArray(len(regs))
		for _, v := range regs {
			w.WriteInt(int(v))
		}
	case "decode":
		s, err := rs.PFDecode(c.key)
		if err != nil {
			return fail(err)
		}
		w.WriteBulkString(s)
	case "encoding":
		s, err := rs.PFEncoding(c.key)
		if err != nil {
			return fail(err)
		}
		w.WriteString(s)
	case "todense":
		changed, err := rs.PFToDense(c.key)
		if err != nil {
			return fail(err)
		}
		if changed {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}
	}
	return nil, nil
}

// PFSelfTest runs the HyperLogLog implementation's built-in checks.
// PFSELFTEST
type PFSelfTest struct{ redis.BaseCmd }

func ParsePFSelfTest(b redis.BaseCmd) (PFSelfTest, error) {
	if len(b.Args()) != 0 {
		return PFSelfTest{}, redis.ErrInvalidArgNum
	}
	return PFSelfTest{b}, nil
}

func (c PFSelfTest) Run(w redis.Writer, red redis.Redka) (any, error) {
	if err := red.Str().PFSelfTest(); err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
