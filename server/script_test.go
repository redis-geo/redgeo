package server

import (
	"strings"
	"testing"
)

func TestLuaScripting(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	want := func(s string, args ...string) {
		t.Helper()
		if r := cl.do(t, args...); r.Str != s {
			t.Fatalf("%v = %+v want %q", args, r, s)
		}
	}
	// Value conversions.
	want("3", "EVAL", "return 3", "0")
	want("hello", "EVAL", "return 'hello'", "0")
	want("1", "EVAL", "return 1.9 + 0.5 > 2 and 1 or 0", "0")
	if r := cl.do(t, "EVAL", "return {1, 'a', {2, 3}, false}", "0"); len(r.Arr) != 4 || r.Arr[1].Str != "a" || len(r.Arr[2].Arr) != 2 || !r.Arr[3].Null {
		t.Fatalf("array = %+v", r) // false becomes a nil element; only Lua nil ends the array
	}
	if r := cl.do(t, "EVAL", "return false", "0"); !r.Null {
		t.Fatalf("false = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return redis.status_reply('FINE')", "0"); r.Kind != '+' || r.Str != "FINE" {
		t.Fatalf("status = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return redis.error_reply('MY err')", "0"); r.Kind != '-' || r.Str != "MY err" {
		t.Fatalf("error = %+v", r)
	}
	// KEYS / ARGV and redis.call.
	want("OK", "EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "k", "v")
	want("v", "GET", "k")
	r := cl.do(t, "EVAL", "return {KEYS[1], ARGV[1], ARGV[2]}", "1", "a", "b", "c")
	if len(r.Arr) != 3 || r.Arr[0].Str != "a" || r.Arr[2].Str != "c" {
		t.Fatalf("KEYS/ARGV = %+v", r)
	}
	// Read-your-writes inside a script, int/array/nil replies from commands.
	src := `redis.call('INCRBY', KEYS[1], 5)
local n = redis.call('INCRBY', KEYS[1], 2)
redis.call('RPUSH', 'l', 'x', 'y')
local l = redis.call('LRANGE', 'l', 0, -1)
local missing = redis.call('GET', 'nokey')
return {n, #l, tostring(missing == false)}`
	r = cl.do(t, "EVAL", src, "1", "cnt")
	if len(r.Arr) != 3 || r.Arr[0].Str != "7" || r.Arr[1].Str != "2" || r.Arr[2].Str != "true" {
		t.Fatalf("script = %+v", r)
	}
	want("7", "GET", "cnt")

	// Errors: redis.call raises, redis.pcall returns.
	if r := cl.do(t, "EVAL", "return redis.call('LPUSH', KEYS[1], 'x')", "1", "k"); r.Kind != '-' || !strings.Contains(r.Str, "type") {
		t.Fatalf("redis.call error = %+v", r)
	}
	want("caught", "EVAL", "local r = redis.pcall('LPUSH', KEYS[1], 'x'); if r.err then return 'caught' end", "1", "k")
	if r := cl.do(t, "EVAL", "error('boom')", "0"); r.Kind != '-' || !strings.Contains(r.Str, "boom") {
		t.Fatalf("lua error = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return +", "0"); r.Kind != '-' || !strings.Contains(r.Str, "compiling") {
		t.Fatalf("syntax error = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return redis.call('MULTI')", "0"); r.Kind != '-' || !strings.Contains(r.Str, "not allowed") {
		t.Fatalf("blocked command = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return redis.call('NOSUCH')", "0"); r.Kind != '-' {
		t.Fatalf("unknown command = %+v", r)
	}
	if r := cl.do(t, "EVAL", "return 1", "2", "a"); r.Kind != '-' || !strings.Contains(r.Str, "greater than") {
		t.Fatalf("numkeys = %+v", r)
	}

	// Sandbox: no os/io/require.
	want("true", "EVAL", "return tostring(os == nil and io == nil and require == nil and dofile == nil)", "0")

	// SCRIPT LOAD / EVALSHA / EXISTS / FLUSH.
	sha := cl.do(t, "SCRIPT", "LOAD", "return ARGV[1] .. '!'").Str
	if len(sha) != 40 {
		t.Fatalf("SCRIPT LOAD = %q", sha)
	}
	want("hey!", "EVALSHA", sha, "0", "hey")
	if r := cl.do(t, "SCRIPT", "EXISTS", sha, strings.Repeat("0", 40)); len(r.Arr) != 2 || r.Arr[0].Str != "1" || r.Arr[1].Str != "0" {
		t.Fatalf("SCRIPT EXISTS = %+v", r)
	}
	want("OK", "SCRIPT", "FLUSH")
	if r := cl.do(t, "EVALSHA", sha, "0", "x"); r.Kind != '-' || !strings.HasPrefix(r.Str, "NOSCRIPT") {
		t.Fatalf("EVALSHA after flush = %+v", r)
	}
	want("abc", "EVAL", "return redis.sha1hex('abc') == 'a9993e364706816aba3e25717850c26c9cd0d89d' and 'abc' or 'bad'", "0")

	// cjson.
	want(`{"a":1}`, "EVAL", `return cjson.encode(cjson.decode('{"a":1}'))`, "0")
	want("3", "EVAL", `return #cjson.decode('[1,2,3]')`, "0")

	// Inside MULTI, a script is queued and runs at EXEC.
	cl.do(t, "MULTI")
	want("QUEUED", "EVAL", "return redis.call('SET', 'm', 'x')", "0")
	if r := cl.do(t, "EXEC"); len(r.Arr) != 1 || r.Arr[0].Str != "OK" {
		t.Fatalf("EXEC = %+v", r)
	}
	want("x", "GET", "m")

	// A runaway loop is aborted, not hung.
	if testing.Short() {
		return
	}
	if r := cl.do(t, "EVAL", "while true do end", "0"); r.Kind != '-' || !strings.Contains(r.Str, "killed") {
		t.Fatalf("infinite loop = %+v", r)
	}
}
