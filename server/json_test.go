package server

import (
	"strings"
	"testing"
)

func TestRedisJSON(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	want := func(s string, args ...string) {
		t.Helper()
		if r := cl.do(t, args...); r.Str != s || r.Kind == '-' {
			t.Fatalf("%v = %+v want %q", args, r, s)
		}
	}
	// arr renders an array reply compactly: elements space-separated, nested
	// arrays as [a,b], nils as "nil".
	arr := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind == '-' {
			return "ERR:" + r.Str
		}
		var parts []string
		for _, e := range r.Arr {
			switch {
			case e.Null:
				parts = append(parts, "nil")
			case e.Kind == '*':
				var sub []string
				for _, s := range e.Arr {
					sub = append(sub, s.Str)
				}
				parts = append(parts, "["+strings.Join(sub, ",")+"]")
			default:
				parts = append(parts, e.Str)
			}
		}
		return strings.Join(parts, " ")
	}
	isErr := func(args ...string) string {
		t.Helper()
		r := cl.do(t, args...)
		if r.Kind != '-' {
			t.Fatalf("%v = %+v want error", args, r)
		}
		return r.Str
	}
	eq := func(got, want string, args ...string) {
		t.Helper()
		if got != want {
			t.Fatalf("%v = %q want %q", args, got, want)
		}
	}

	// ---- SET / GET: $ paths return arrays of matches, legacy paths one value.
	want("OK", "JSON.SET", "doc", "$", `{"a":2,"s":"hi","l":[1,2,3],"o":{"x":true,"y":null},"f":1.5}`)
	want(`[{"a":2,"f":1.5,"l":[1,2,3],"o":{"x":true,"y":null},"s":"hi"}]`, "JSON.GET", "doc", "$")
	want(`[2]`, "JSON.GET", "doc", "$.a")
	want(`2`, "JSON.GET", "doc", ".a")
	want(`[]`, "JSON.GET", "doc", "$.nope")
	isErr("JSON.GET", "doc", ".nope")
	want(`{"$.a":[2],"$.s":["hi"]}`, "JSON.GET", "doc", "$.a", "$.s")
	want(`[[1,2,3]]`, "JSON.GET", "doc", "INDENT", "  ", "$.l") // formatting options accepted, output stays compact
	if r := cl.do(t, "JSON.GET", "nokey"); !r.Null {
		t.Fatalf("GET missing = %+v", r)
	}
	if r := cl.do(t, "TYPE", "doc"); r.Str != "ReJSON-RL" {
		t.Fatalf("TYPE = %+v", r)
	}

	// ---- SET semantics: new fields, NX/XX, root-only creation, errors.
	want("OK", "JSON.SET", "doc", "$.newfield", `"v"`)
	want(`["v"]`, "JSON.GET", "doc", "$.newfield")
	if r := cl.do(t, "JSON.SET", "doc", "$.a", `9`, "NX"); !r.Null {
		t.Fatalf("NX on existing = %+v", r)
	}
	if r := cl.do(t, "JSON.SET", "doc", "$.zzz", `9`, "XX"); !r.Null {
		t.Fatalf("XX on missing = %+v", r)
	}
	want("OK", "JSON.SET", "doc", "$.a", `3`, "XX")
	want("OK", "JSON.SET", "doc", "$.fresh", `1`, "NX")
	isErr("JSON.SET", "newkey", "$.a", `1`) // new documents must start at the root
	isErr("JSON.SET", "doc", "$.a", `{bad`)
	isErr("JSON.SET", "doc", "$.a", `1`, "BOGUS")
	if r := cl.do(t, "JSON.SET", "doc", "$.l[9]", `1`); !r.Null {
		t.Fatalf("SET out of range = %+v", r)
	}
	cl.do(t, "SET", "str", "x")
	isErr("JSON.SET", "str", "$", `1`)
	isErr("JSON.GET", "str")
	want("OK", "JSON.SET", "doc", "$.fresh", `null`)

	// ---- TYPE / lengths / keys
	eq(arr("JSON.TYPE", "doc", "$.a"), "integer", "type a")
	eq(arr("JSON.TYPE", "doc", "$.f"), "number", "type f")
	eq(arr("JSON.TYPE", "doc", "$.s"), "string")
	eq(arr("JSON.TYPE", "doc", "$.l"), "array")
	eq(arr("JSON.TYPE", "doc", "$.o"), "object")
	eq(arr("JSON.TYPE", "doc", "$.o.x"), "boolean")
	eq(arr("JSON.TYPE", "doc", "$.o.y"), "null")
	eq(arr("JSON.TYPE", "doc", "$.nope"), "")
	want("integer", "JSON.TYPE", "doc", ".a")
	eq(arr("JSON.STRLEN", "doc", "$.s"), "2")
	eq(arr("JSON.STRLEN", "doc", "$.a"), "nil")
	eq(arr("JSON.ARRLEN", "doc", "$.l"), "3")
	eq(arr("JSON.ARRLEN", "doc", "$.s"), "nil")
	eq(arr("JSON.OBJLEN", "doc", "$.o"), "2")
	eq(arr("JSON.OBJKEYS", "doc", "$.o"), "[x,y]")
	eq(arr("JSON.OBJKEYS", "doc", "$"), "[a,f,fresh,l,newfield,o,s]")
	want("3", "JSON.ARRLEN", "doc", ".l")
	if r := cl.do(t, "JSON.TYPE", "nokey", "$"); !r.Null {
		t.Fatalf("TYPE missing = %+v", r)
	}

	// ---- numbers
	want(`[5]`, "JSON.NUMINCRBY", "doc", "$.a", "2")
	want(`7`, "JSON.NUMINCRBY", "doc", ".a", "2")
	want(`[21]`, "JSON.NUMMULTBY", "doc", "$.a", "3")
	want(`[2.5]`, "JSON.NUMINCRBY", "doc", "$.f", "1")
	want(`[null]`, "JSON.NUMINCRBY", "doc", "$.s", "1")
	isErr("JSON.NUMINCRBY", "doc", ".s", "1")
	isErr("JSON.NUMINCRBY", "doc", "$.a", "x")
	isErr("JSON.NUMINCRBY", "nokey", "$.a", "1")
	want(`[9223372036854775807]`, "JSON.NUMINCRBY", "doc", "$.a", "9223372036854775786") // exactly int64 max
	if r := cl.do(t, "JSON.NUMINCRBY", "doc", "$.a", "1"); r.Kind == '-' || !strings.Contains(r.Str, "e+18") {
		t.Fatalf("overflow should fall back to float: %+v", r)
	}

	// ---- strings
	eq(arr("JSON.STRAPPEND", "doc", "$.s", `" there"`), "8")
	want(`["hi there"]`, "JSON.GET", "doc", "$.s")
	eq(arr("JSON.STRAPPEND", "doc", "$.a", `"x"`), "nil")
	isErr("JSON.STRAPPEND", "doc", "$.s", `5`)

	// ---- booleans / clear
	eq(arr("JSON.TOGGLE", "doc", "$.o.x"), "0")
	eq(arr("JSON.TOGGLE", "doc", "$.o.x"), "1")
	eq(arr("JSON.TOGGLE", "doc", "$.a"), "nil")

	// ---- arrays
	want("OK", "JSON.SET", "arr", "$", `[1,2,3]`)
	eq(arr("JSON.ARRAPPEND", "arr", "$", `4`, `"five"`), "5")
	want(`[[1,2,3,4,"five"]]`, "JSON.GET", "arr", "$")
	eq(arr("JSON.ARRINSERT", "arr", "$", "1", `1.5`, `1.7`), "7")
	eq(arr("JSON.ARRINSERT", "arr", "$", "-1", `"x"`), "8")
	want(`[[1,1.5,1.7,2,3,4,"x","five"]]`, "JSON.GET", "arr", "$")
	isErr("JSON.ARRINSERT", "arr", "$", "99", `1`)
	eq(arr("JSON.ARRINDEX", "arr", "$", `3`), "4")
	eq(arr("JSON.ARRINDEX", "arr", "$", `"five"`), "7")
	eq(arr("JSON.ARRINDEX", "arr", "$", `3`, "5"), "-1")
	isErr("JSON.ARRINDEX", "arr", "$", `nothere`)
	eq(arr("JSON.ARRPOP", "arr", "$"), `"five"`)
	eq(arr("JSON.ARRPOP", "arr", "$", "0"), "1")
	eq(arr("JSON.ARRPOP", "arr", "$", "-1"), `"x"`)
	want(`[[1.5,1.7,2,3,4]]`, "JSON.GET", "arr", "$")
	eq(arr("JSON.ARRTRIM", "arr", "$", "1", "3"), "3")
	want(`[[1.7,2,3]]`, "JSON.GET", "arr", "$")
	eq(arr("JSON.ARRTRIM", "arr", "$", "5", "9"), "0")
	eq(arr("JSON.ARRPOP", "arr", "$"), "nil")
	want("OK", "JSON.SET", "nested", "$", `{"a":[1],"b":{"a":[2,3]},"c":"s"}`)
	eq(arr("JSON.ARRAPPEND", "nested", "$..a", `0`), "2 3") // every match, in document order
	eq(arr("JSON.ARRLEN", "nested", "$..a"), "2 3")
	eq(arr("JSON.ARRAPPEND", "nested", "$.c", `0`), "nil")

	// ---- clear / del / forget
	want("OK", "JSON.SET", "c", "$", `{"n":5,"l":[1],"o":{"k":1},"s":"x"}`)
	want("3", "JSON.CLEAR", "c", "$.*") // number, array and object cleared; string untouched
	want(`[{"l":[],"n":0,"o":{},"s":"x"}]`, "JSON.GET", "c", "$")
	want("1", "JSON.DEL", "c", "$.s")
	want("0", "JSON.DEL", "c", "$.s")
	want("1", "JSON.FORGET", "c", ".o")
	want("0", "JSON.DEL", "nokey")
	want("1", "JSON.DEL", "c")
	if r := cl.do(t, "EXISTS", "c"); r.Str != "0" {
		t.Fatalf("EXISTS after DEL root = %+v", r)
	}

	// ---- MGET / MSET
	want("OK", "JSON.MSET", "m1", "$", `{"v":1}`, "m2", "$", `{"v":2}`)
	r := cl.do(t, "JSON.MGET", "m1", "nokey", "m2", "$.v")
	if len(r.Arr) != 3 || r.Arr[0].Str != "[1]" || !r.Arr[1].Null || r.Arr[2].Str != "[2]" {
		t.Fatalf("MGET = %+v", r)
	}

	// ---- keyspace integration
	want("1", "EXPIRE", "m1", "100")
	if r := cl.do(t, "TTL", "m1"); r.Str == "-1" || r.Str == "-2" {
		t.Fatalf("TTL = %+v", r)
	}
	want("OK", "RENAME", "m2", "m3")
	want(`[{"v":2}]`, "JSON.GET", "m3")
	if r := cl.do(t, "SCAN", "0", "TYPE", "ReJSON-RL", "COUNT", "100"); len(r.Arr[1].Arr) < 4 {
		t.Fatalf("SCAN TYPE ReJSON-RL = %+v", r)
	}

	// ---- inside MULTI and a Lua script
	cl.do(t, "MULTI")
	cl.do(t, "JSON.SET", "tx", "$", `{"n":1}`)
	cl.do(t, "JSON.NUMINCRBY", "tx", "$.n", "4")
	if r := cl.do(t, "EXEC"); len(r.Arr) != 2 || r.Arr[1].Str != "[5]" {
		t.Fatalf("EXEC = %+v", r)
	}
	want("[6]", "EVAL", `return redis.call('JSON.NUMINCRBY', KEYS[1], '$.n', '1')`, "1", "tx")
}
