package script

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

// Timeout bounds a single script run. Redis lets a slow script keep running
// until SCRIPT KILL; we abort it instead, since we have no KILL.
const Timeout = 5 * time.Second

// Engine caches compiled scripts by SHA1 (per node, like Redis' script cache).
type Engine struct {
	mu    sync.RWMutex
	procs map[string]*lua.FunctionProto
}

func NewEngine() *Engine { return &Engine{procs: map[string]*lua.FunctionProto{}} }

// SHA1 returns the lowercase hex SHA1 of a script body.
func SHA1(src string) string {
	h := sha1.Sum([]byte(src))
	return hex.EncodeToString(h[:])
}

// Load compiles and caches a script, returning its SHA1.
func (e *Engine) Load(src string) (string, error) {
	sha := SHA1(src)
	e.mu.RLock()
	_, ok := e.procs[sha]
	e.mu.RUnlock()
	if ok {
		return sha, nil
	}
	chunk, err := parse.Parse(strings.NewReader(src), "user_script")
	if err != nil {
		return "", fmt.Errorf("ERR Error compiling script (new function): %v", err)
	}
	proto, err := lua.Compile(chunk, "user_script")
	if err != nil {
		return "", fmt.Errorf("ERR Error compiling script (new function): %v", err)
	}
	e.mu.Lock()
	e.procs[sha] = proto
	e.mu.Unlock()
	return sha, nil
}

func (e *Engine) Exists(sha string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.procs[strings.ToLower(sha)]
	return ok
}

func (e *Engine) Flush() {
	e.mu.Lock()
	e.procs = map[string]*lua.FunctionProto{}
	e.mu.Unlock()
}

// ErrNoScript is returned by Run for an unknown SHA.
var ErrNoScript = errors.New("NOSCRIPT No matching script. Please use EVAL.")

// Call executes one Redis command on behalf of the script.
type Call func(args [][]byte) Reply

// Run executes the cached script sha with the given KEYS/ARGV.
func (e *Engine) Run(sha string, keys, argv [][]byte, call Call) (Reply, error) {
	e.mu.RLock()
	proto, ok := e.procs[strings.ToLower(sha)]
	e.mu.RUnlock()
	if !ok {
		return Reply{}, ErrNoScript
	}
	L := newSandbox()
	defer L.Close()
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	L.SetContext(ctx)

	L.SetGlobal("KEYS", bytesTable(L, keys))
	L.SetGlobal("ARGV", bytesTable(L, argv))
	installRedis(L, call)
	openCJSON(L)

	fn := L.NewFunctionFromProto(proto)
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		if ctx.Err() != nil {
			return Reply{Kind: '-', Str: "ERR Script killed: execution time limit exceeded"}, nil
		}
		return errorReply(err), nil
	}
	ret := L.Get(-1)
	return fromLua(ret), nil
}

func newSandbox() *lua.LState {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	for _, lib := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		L.Push(L.NewFunction(lib.fn))
		L.Push(lua.LString(lib.name))
		L.Call(1, 0)
	}
	for _, name := range []string{"dofile", "loadfile", "print", "require", "module", "_printregs"} { // no file/stdout/module access
		L.SetGlobal(name, lua.LNil)
	}
	return L
}

func bytesTable(L *lua.LState, bs [][]byte) *lua.LTable {
	t := L.CreateTable(len(bs), 0)
	for _, b := range bs {
		t.Append(lua.LString(b))
	}
	return t
}

func installRedis(L *lua.LState, call Call) {
	r := L.NewTable()
	run := func(raise bool) lua.LGFunction {
		return func(L *lua.LState) int {
			n := L.GetTop()
			if n == 0 {
				return raiseErr(L, "ERR Please specify at least one argument for this redis lib call")
			}
			args := make([][]byte, 0, n)
			for i := 1; i <= n; i++ {
				switch v := L.Get(i).(type) {
				case lua.LString:
					args = append(args, []byte(v))
				case lua.LNumber:
					args = append(args, []byte(numToString(float64(v))))
				default:
					return raiseErr(L, "ERR Lua redis lib command arguments must be strings or integers")
				}
			}
			rep := call(args)
			if rep.Kind == '-' && raise {
				return raiseErr(L, rep.Str)
			}
			L.Push(toLua(L, rep))
			return 1
		}
	}
	L.SetField(r, "call", L.NewFunction(run(true)))
	L.SetField(r, "pcall", L.NewFunction(run(false)))
	L.SetField(r, "status_reply", L.NewFunction(func(L *lua.LState) int {
		t := L.NewTable()
		L.SetField(t, "ok", lua.LString(L.CheckString(1)))
		L.Push(t)
		return 1
	}))
	L.SetField(r, "error_reply", L.NewFunction(func(L *lua.LState) int {
		t := L.NewTable()
		L.SetField(t, "err", lua.LString(L.CheckString(1)))
		L.Push(t)
		return 1
	}))
	L.SetField(r, "sha1hex", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(SHA1(L.CheckString(1))))
		return 1
	}))
	L.SetField(r, "log", L.NewFunction(func(L *lua.LState) int { return 0 }))
	L.SetField(r, "setresp", L.NewFunction(func(L *lua.LState) int { return 0 }))
	for i, name := range []string{"LOG_DEBUG", "LOG_VERBOSE", "LOG_NOTICE", "LOG_WARNING"} {
		L.SetField(r, name, lua.LNumber(i))
	}
	L.SetField(r, "REDIS_VERSION", lua.LString("7.2.0"))
	L.SetGlobal("redis", r)
}

func raiseErr(L *lua.LState, msg string) int {
	t := L.NewTable()
	L.SetField(t, "err", lua.LString(msg))
	L.Error(t, 1)
	return 0
}

func numToString(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 17, 64)
}

// toLua converts a Redis reply to its Lua value (Redis → Lua rules).
func toLua(L *lua.LState, r Reply) lua.LValue {
	switch r.Kind {
	case ':':
		return lua.LNumber(r.Int)
	case '$':
		return lua.LString(r.Str)
	case '+':
		t := L.NewTable()
		L.SetField(t, "ok", lua.LString(r.Str))
		return t
	case '-':
		t := L.NewTable()
		L.SetField(t, "err", lua.LString(r.Str))
		return t
	case '*':
		t := L.CreateTable(len(r.Arr), 0)
		for _, c := range r.Arr {
			t.Append(toLua(L, c))
		}
		return t
	}
	return lua.LFalse // nil bulk / nil array
}

// fromLua converts a script's return value to a Redis reply (Lua → Redis rules).
func fromLua(v lua.LValue) Reply {
	switch x := v.(type) {
	case lua.LString:
		return Reply{Kind: '$', Str: string(x)}
	case lua.LNumber:
		return Reply{Kind: ':', Int: int64(x)}
	case lua.LBool:
		if x {
			return Reply{Kind: ':', Int: 1}
		}
		return Reply{Kind: '_'}
	case *lua.LTable:
		if s, ok := x.RawGetString("ok").(lua.LString); ok {
			return Reply{Kind: '+', Str: string(s)}
		}
		if s, ok := x.RawGetString("err").(lua.LString); ok {
			return Reply{Kind: '-', Str: string(s)}
		}
		out := Reply{Kind: '*'}
		for i := 1; ; i++ {
			e := x.RawGetInt(i)
			if e == lua.LNil {
				break
			}
			out.Arr = append(out.Arr, fromLua(e))
		}
		return out
	}
	return Reply{Kind: '_'}
}

// errorReply maps a Lua runtime error to an error reply.
func errorReply(err error) Reply {
	var ae *lua.ApiError
	if errors.As(err, &ae) && ae.Object != nil {
		if t, ok := ae.Object.(*lua.LTable); ok {
			if s, ok := t.RawGetString("err").(lua.LString); ok {
				return Reply{Kind: '-', Str: string(s)}
			}
		}
		return Reply{Kind: '-', Str: "ERR Error running script: " + ae.Object.String()}
	}
	return Reply{Kind: '-', Str: "ERR Error running script: " + err.Error()}
}
