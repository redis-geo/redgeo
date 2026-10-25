package script

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"

	lua "github.com/yuin/gopher-lua"
)

// A small cjson.encode / cjson.decode, the one library scripts most often need.

var cjsonNull = &lua.LUserData{}

func openCJSON(L *lua.LState) {
	t := L.NewTable()
	L.SetField(t, "null", cjsonNull)
	L.SetField(t, "encode", L.NewFunction(func(L *lua.LState) int {
		b, err := json.Marshal(luaToGo(L.CheckAny(1), 0))
		if err != nil {
			L.RaiseError("cjson.encode: %v", err)
		}
		L.Push(lua.LString(b))
		return 1
	}))
	L.SetField(t, "decode", L.NewFunction(func(L *lua.LState) int {
		var v any
		if err := json.Unmarshal([]byte(L.CheckString(1)), &v); err != nil {
			L.RaiseError("cjson.decode: %v", err)
		}
		L.Push(goToLua(L, v))
		return 1
	}))
	L.SetGlobal("cjson", t)
}

const maxJSONDepth = 100

func luaToGo(v lua.LValue, depth int) any {
	if depth > maxJSONDepth {
		return nil
	}
	switch x := v.(type) {
	case lua.LBool:
		return bool(x)
	case lua.LNumber:
		f := float64(x)
		if f == math.Trunc(f) && math.Abs(f) < 1e15 {
			return json.Number(strconv.FormatInt(int64(f), 10))
		}
		return f
	case lua.LString:
		return string(x)
	case *lua.LTable:
		n := x.Len()
		count := 0
		x.ForEach(func(lua.LValue, lua.LValue) { count++ })
		if n > 0 && n == count { // pure sequence
			arr := make([]any, 0, n)
			for i := 1; i <= n; i++ {
				arr = append(arr, luaToGo(x.RawGetInt(i), depth+1))
			}
			return arr
		}
		keys := []string{}
		obj := map[string]any{}
		x.ForEach(func(k, val lua.LValue) {
			ks := k.String()
			keys = append(keys, ks)
			obj[ks] = luaToGo(val, depth+1)
		})
		sort.Strings(keys)
		return obj
	}
	return nil
}

func goToLua(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return cjsonNull
	case bool:
		return lua.LBool(x)
	case float64:
		return lua.LNumber(x)
	case string:
		return lua.LString(x)
	case []any:
		t := L.CreateTable(len(x), 0)
		for _, e := range x {
			t.Append(goToLua(L, e))
		}
		return t
	case map[string]any:
		t := L.CreateTable(0, len(x))
		for k, e := range x {
			t.RawSetString(k, goToLua(L, e))
		}
		return t
	}
	return lua.LNil
}
