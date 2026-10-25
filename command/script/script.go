// Package script implements EVAL, EVALSHA and SCRIPT. The Lua VM lives in the
// top-level script package; this package adapts it to the command layer.
package script

import (
	"errors"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/script"

	redis "github.com/redis-geo/redgeo/redisapi"
)

// Dispatch parses a command for redis.call. The command package sets it at init
// (it cannot be imported from here without a cycle).
var Dispatch func(args [][]byte) (redis.Cmd, error)

var engine = script.NewEngine()

// notAllowed are commands scripts may not call (connection/transaction state).
var notAllowed = map[string]bool{
	"multi": true, "exec": true, "discard": true, "watch": true, "unwatch": true,
	"subscribe": true, "psubscribe": true, "unsubscribe": true, "punsubscribe": true,
	"publish": true, "hello": true,
	"eval": true, "evalsha": true, "eval_ro": true, "evalsha_ro": true, "script": true,
}

func writeReply(w redis.Writer, r script.Reply) {
	switch r.Kind {
	case '+':
		w.WriteString(r.Str)
	case '-':
		w.WriteError(r.Str)
	case ':':
		w.WriteInt64(r.Int)
	case '$':
		w.WriteBulkString(r.Str)
	case '*':
		w.WriteArray(len(r.Arr))
		for _, c := range r.Arr {
			writeReply(w, c)
		}
	default:
		w.WriteNull()
	}
}

// Eval runs a script given inline (EVAL) or by SHA1 (EVALSHA).
// EVAL script numkeys [key [key ...]] [arg [arg ...]]
// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
// https://redis.io/commands/eval
type Eval struct {
	redis.BaseCmd
	src   string // script body (EVAL) or sha (EVALSHA)
	bySHA bool
	keys  [][]byte
	argv  [][]byte
}

func ParseEval(b redis.BaseCmd, bySHA bool) (Eval, error) {
	cmd := Eval{BaseCmd: b, bySHA: bySHA}
	args := cmd.Args()
	if len(args) < 2 {
		return Eval{}, redis.ErrInvalidArgNum
	}
	cmd.src = string(args[0])
	n, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return Eval{}, redis.ErrInvalidInt
	}
	if n < 0 {
		return Eval{}, errors.New("ERR Number of keys can't be negative")
	}
	if n > len(args)-2 {
		return Eval{}, errors.New("ERR Number of keys can't be greater than number of args")
	}
	cmd.keys, cmd.argv = args[2:2+n], args[2+n:]
	return cmd, nil
}

func (cmd Eval) Run(w redis.Writer, red redis.Redka) (any, error) {
	sha := cmd.src
	if !cmd.bySHA {
		var err error
		if sha, err = engine.Load(cmd.src); err != nil {
			w.WriteError(err.Error())
			return nil, err
		}
	}
	call := func(args [][]byte) script.Reply {
		name := strings.ToLower(string(args[0]))
		if notAllowed[name] {
			return script.Reply{Kind: '-', Str: "ERR This Redis command is not allowed from script"}
		}
		pcmd, err := Dispatch(args)
		if err != nil {
			if pcmd == nil {
				return script.Reply{Kind: '-', Str: err.Error()}
			}
			return script.Reply{Kind: '-', Str: pcmd.Error(err)}
		}
		var rec script.Recorder
		_, _ = pcmd.Run(&rec, red)
		return rec.Reply()
	}
	rep, err := engine.Run(sha, cmd.keys, cmd.argv, call)
	if err != nil {
		w.WriteError(err.Error())
		return nil, err
	}
	writeReply(w, rep)
	return rep, nil
}

// Script implements SCRIPT LOAD | EXISTS | FLUSH | KILL.
// https://redis.io/commands/script
type Script struct {
	redis.BaseCmd
	sub  string
	args [][]byte
}

func ParseScript(b redis.BaseCmd) (Script, error) {
	cmd := Script{BaseCmd: b}
	if len(cmd.Args()) < 1 {
		return Script{}, redis.ErrInvalidArgNum
	}
	cmd.sub = strings.ToLower(string(cmd.Args()[0]))
	cmd.args = cmd.Args()[1:]
	switch cmd.sub {
	case "load":
		if len(cmd.args) != 1 {
			return Script{}, redis.ErrInvalidArgNum
		}
	case "exists":
		if len(cmd.args) < 1 {
			return Script{}, redis.ErrInvalidArgNum
		}
	case "flush":
		if len(cmd.args) > 1 {
			return Script{}, redis.ErrSyntaxError
		}
	case "kill":
	default:
		return Script{}, redis.ErrUnknownSubcmd
	}
	return cmd, nil
}

func (cmd Script) Run(w redis.Writer, _ redis.Redka) (any, error) {
	switch cmd.sub {
	case "load":
		sha, err := engine.Load(string(cmd.args[0]))
		if err != nil {
			w.WriteError(err.Error())
			return nil, err
		}
		w.WriteBulkString(sha)
	case "exists":
		w.WriteArray(len(cmd.args))
		for _, a := range cmd.args {
			if engine.Exists(string(a)) {
				w.WriteInt(1)
			} else {
				w.WriteInt(0)
			}
		}
	case "flush":
		engine.Flush()
		w.WriteString("OK")
	case "kill":
		w.WriteError("NOTBUSY No scripts in execution right now.")
	}
	return nil, nil
}
