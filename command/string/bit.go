package string

import (
	"errors"
	"strconv"
	"strings"

	redis "github.com/redis-geo/redgeo/redisapi"
	"github.com/redis-geo/redgeo/restypes"
)

var errBitOffset = errors.New("ERR bit offset is not an integer or out of range")
var errBitValue = errors.New("ERR bit is not an integer or out of range")

func parseOffset(b []byte) (uint64, error) {
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil || n > 1<<32-1 {
		return 0, errBitOffset
	}
	return n, nil
}

func parseBit(b []byte) (bool, error) {
	switch string(b) {
	case "0":
		return false, nil
	case "1":
		return true, nil
	}
	return false, errBitValue
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// SetBit sets or clears the bit at offset and returns its previous value.
// SETBIT key offset value
// https://redis.io/commands/setbit
type SetBit struct {
	redis.BaseCmd
	key    string
	offset uint64
	on     bool
}

func ParseSetBit(b redis.BaseCmd) (SetBit, error) {
	cmd := SetBit{BaseCmd: b}
	if len(cmd.Args()) != 3 {
		return SetBit{}, redis.ErrInvalidArgNum
	}
	var err error
	cmd.key = string(cmd.Args()[0])
	if cmd.offset, err = parseOffset(cmd.Args()[1]); err != nil {
		return SetBit{}, err
	}
	if cmd.on, err = parseBit(cmd.Args()[2]); err != nil {
		return SetBit{}, err
	}
	return cmd, nil
}

func (cmd SetBit) Run(w redis.Writer, red redis.Redka) (any, error) {
	old, err := red.Str().SetBit(cmd.key, cmd.offset, cmd.on)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(boolInt(old))
	return boolInt(old), nil
}

// GetBit returns the bit at offset.
// GETBIT key offset
// https://redis.io/commands/getbit
type GetBit struct {
	redis.BaseCmd
	key    string
	offset uint64
}

func ParseGetBit(b redis.BaseCmd) (GetBit, error) {
	cmd := GetBit{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return GetBit{}, redis.ErrInvalidArgNum
	}
	var err error
	cmd.key = string(cmd.Args()[0])
	if cmd.offset, err = parseOffset(cmd.Args()[1]); err != nil {
		return GetBit{}, err
	}
	return cmd, nil
}

func (cmd GetBit) Run(w redis.Writer, red redis.Redka) (any, error) {
	v, err := red.Str().GetBit(cmd.key, cmd.offset)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(boolInt(v))
	return boolInt(v), nil
}

// parseBitRange parses [start [end [BYTE|BIT]]] (end is required for the unit).
// needEnd makes start and end mandatory together (BITCOUNT).
func parseBitRange(args [][]byte, needEnd bool) (restypes.BitRange, error) {
	var r restypes.BitRange
	if len(args) == 0 {
		return r, nil
	}
	var err error
	if r.Start, err = strconv.ParseInt(string(args[0]), 10, 64); err != nil {
		return r, redis.ErrInvalidInt
	}
	r.HasStart = true
	if len(args) == 1 {
		if needEnd {
			return r, redis.ErrSyntaxError
		}
		return r, nil
	}
	if r.End, err = strconv.ParseInt(string(args[1]), 10, 64); err != nil {
		return r, redis.ErrInvalidInt
	}
	r.HasEnd = true
	switch {
	case len(args) == 2:
	case len(args) == 3 && strings.EqualFold(string(args[2]), "bit"):
		r.InBits = true
	case len(args) == 3 && strings.EqualFold(string(args[2]), "byte"):
	default:
		return r, redis.ErrSyntaxError
	}
	return r, nil
}

// BitCount counts set bits in a string.
// BITCOUNT key [start end [BYTE | BIT]]
// https://redis.io/commands/bitcount
type BitCount struct {
	redis.BaseCmd
	key string
	rng restypes.BitRange
}

func ParseBitCount(b redis.BaseCmd) (BitCount, error) {
	cmd := BitCount{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return BitCount{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	var err error
	if cmd.rng, err = parseBitRange(cmd.Args()[1:], true); err != nil {
		return BitCount{}, err
	}
	return cmd, nil
}

func (cmd BitCount) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Str().BitCount(cmd.key, cmd.rng)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(int(n))
	return n, nil
}

// BitPos finds the first set or clear bit in a string.
// BITPOS key bit [start [end [BYTE | BIT]]]
// https://redis.io/commands/bitpos
type BitPos struct {
	redis.BaseCmd
	key string
	bit bool
	rng restypes.BitRange
}

func ParseBitPos(b redis.BaseCmd) (BitPos, error) {
	cmd := BitPos{BaseCmd: b}
	if len(cmd.Args()) < 2 {
		return BitPos{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	var err error
	if cmd.bit, err = parseBit(cmd.Args()[1]); err != nil {
		return BitPos{}, err
	}
	if cmd.rng, err = parseBitRange(cmd.Args()[2:], false); err != nil {
		return BitPos{}, err
	}
	return cmd, nil
}

func (cmd BitPos) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Str().BitPos(cmd.key, cmd.bit, cmd.rng)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(int(n))
	return n, nil
}

// BitOp performs a bitwise operation between strings and stores the result.
// BITOP AND|OR|XOR|NOT destkey key [key ...]
// https://redis.io/commands/bitop
type BitOp struct {
	redis.BaseCmd
	op   string
	dest string
	srcs []string
}

func ParseBitOp(b redis.BaseCmd) (BitOp, error) {
	cmd := BitOp{BaseCmd: b}
	if len(cmd.Args()) < 3 {
		return BitOp{}, redis.ErrInvalidArgNum
	}
	cmd.op = strings.ToLower(string(cmd.Args()[0]))
	switch cmd.op {
	case "and", "or", "xor", "not":
	default:
		return BitOp{}, redis.ErrSyntaxError
	}
	cmd.dest = string(cmd.Args()[1])
	for _, a := range cmd.Args()[2:] {
		cmd.srcs = append(cmd.srcs, string(a))
	}
	if cmd.op == "not" && len(cmd.srcs) != 1 {
		return BitOp{}, errors.New("ERR BITOP NOT must be called with a single source key")
	}
	return cmd, nil
}

func (cmd BitOp) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.Str().BitOp(cmd.op, cmd.dest, cmd.srcs...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(int(n))
	return n, nil
}
