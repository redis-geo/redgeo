package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// reply is a parsed RESP reply: Kind is one of '+','-',':','$','*'.
type reply struct {
	Kind byte
	Str  string  // for +, -, : and $ (bulk body); "" with Null=true for nil bulk
	Null bool    // nil bulk / nil array
	Arr  []reply // for *
}

type client struct {
	c net.Conn
	r *bufio.Reader
}

func dialC(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &client{c: c, r: bufio.NewReader(c)}
}

func (cl *client) do(t *testing.T, args ...string) reply {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := cl.c.Write([]byte(b.String())); err != nil {
		t.Fatalf("write: %v", err)
	}
	rep, err := readReply(cl.r)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return rep
}

func readReply(r *bufio.Reader) (reply, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return reply{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return reply{}, fmt.Errorf("empty line")
	}
	kind, rest := line[0], line[1:]
	switch kind {
	case '+', '-', ':', ',':
		// simple string / error / integer / RESP3 double
		return reply{Kind: kind, Str: rest}, nil
	case '_': // RESP3 null
		return reply{Kind: '_', Null: true}, nil
	case '#': // RESP3 boolean
		return reply{Kind: '#', Str: rest}, nil
	case '%': // RESP3 map: 2N elements follow
		n, _ := strconv.Atoi(rest)
		arr := make([]reply, n*2)
		for i := 0; i < n*2; i++ {
			arr[i], err = readReply(r)
			if err != nil {
				return reply{}, err
			}
		}
		return reply{Kind: '%', Arr: arr}, nil
	case '>': // RESP3 push: N elements follow (e.g. pub/sub)
		n, _ := strconv.Atoi(rest)
		arr := make([]reply, n)
		for i := 0; i < n; i++ {
			arr[i], err = readReply(r)
			if err != nil {
				return reply{}, err
			}
		}
		return reply{Kind: '>', Arr: arr}, nil
	case '$':
		n, _ := strconv.Atoi(rest)
		if n < 0 {
			return reply{Kind: '$', Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			return reply{}, err
		}
		return reply{Kind: '$', Str: string(buf[:n])}, nil
	case '*':
		n, _ := strconv.Atoi(rest)
		if n < 0 {
			return reply{Kind: '*', Null: true}, nil
		}
		arr := make([]reply, n)
		for i := 0; i < n; i++ {
			arr[i], err = readReply(r)
			if err != nil {
				return reply{}, err
			}
		}
		return reply{Kind: '*', Arr: arr}, nil
	}
	return reply{}, fmt.Errorf("bad kind %q", kind)
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (cl *client) close() { cl.c.Close() }

func TestStringsAndKeys(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	// SET / GET
	if r := cl.do(t, "SET", "k1", "hello"); r.Str != "OK" {
		t.Fatalf("SET = %+v", r)
	}
	if r := cl.do(t, "GET", "k1"); r.Str != "hello" {
		t.Fatalf("GET k1 = %+v", r)
	}
	// GET missing -> nil
	if r := cl.do(t, "GET", "nope"); !r.Null {
		t.Fatalf("GET nope = %+v, want nil", r)
	}
	// STRLEN
	if r := cl.do(t, "STRLEN", "k1"); r.Str != "5" {
		t.Fatalf("STRLEN = %+v", r)
	}
	// GETSET returns previous
	if r := cl.do(t, "GETSET", "k1", "world"); r.Str != "hello" {
		t.Fatalf("GETSET = %+v", r)
	}
	if r := cl.do(t, "GET", "k1"); r.Str != "world" {
		t.Fatalf("GET after GETSET = %+v", r)
	}

	// SET NX on existing -> nil (no write); NX on new -> OK
	if r := cl.do(t, "SET", "k1", "x", "NX"); !r.Null {
		t.Fatalf("SET k1 NX = %+v, want nil", r)
	}
	if r := cl.do(t, "SET", "k2", "v2", "NX"); r.Str != "OK" {
		t.Fatalf("SET k2 NX = %+v", r)
	}
	// SET XX on missing -> nil
	if r := cl.do(t, "SET", "k3", "v3", "XX"); !r.Null {
		t.Fatalf("SET k3 XX = %+v, want nil", r)
	}

	// MSET / MGET
	cl.do(t, "MSET", "a", "1", "b", "2", "c", "3")
	r := cl.do(t, "MGET", "a", "b", "missing", "c")
	if len(r.Arr) != 4 || r.Arr[0].Str != "1" || r.Arr[1].Str != "2" || !r.Arr[2].Null || r.Arr[3].Str != "3" {
		t.Fatalf("MGET = %+v", r)
	}

	// EXISTS / TYPE
	if r := cl.do(t, "EXISTS", "k1", "missing", "a"); r.Str != "2" {
		t.Fatalf("EXISTS = %+v", r)
	}
	if r := cl.do(t, "TYPE", "k1"); r.Str != "string" {
		t.Fatalf("TYPE = %+v", r)
	}
	if r := cl.do(t, "TYPE", "missing"); r.Str != "none" {
		t.Fatalf("TYPE missing = %+v", r)
	}

	// DEL
	if r := cl.do(t, "DEL", "k1", "k2", "missing"); r.Str != "2" {
		t.Fatalf("DEL = %+v", r)
	}
	if r := cl.do(t, "GET", "k1"); !r.Null {
		t.Fatalf("GET after DEL = %+v, want nil", r)
	}

	// RENAME
	cl.do(t, "SET", "src", "moveme")
	if r := cl.do(t, "RENAME", "src", "dst"); r.Str != "OK" {
		t.Fatalf("RENAME = %+v", r)
	}
	if r := cl.do(t, "GET", "dst"); r.Str != "moveme" {
		t.Fatalf("GET dst = %+v", r)
	}
	if r := cl.do(t, "EXISTS", "src"); r.Str != "0" {
		t.Fatalf("EXISTS src after rename = %+v", r)
	}
}

func TestKeysScanAndDBIsolation(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	cl := dialC(t, addr)
	defer cl.close()

	cl.do(t, "MSET", "user:1", "a", "user:2", "b", "other", "c")

	// KEYS pattern
	r := cl.do(t, "KEYS", "user:*")
	if len(r.Arr) != 2 {
		t.Fatalf("KEYS user:* = %+v, want 2", r)
	}
	// SCAN returns all in one page (cursor 0)
	r = cl.do(t, "SCAN", "0")
	if len(r.Arr) != 2 || r.Arr[0].Str != "0" {
		t.Fatalf("SCAN cursor = %+v", r)
	}
	if len(r.Arr[1].Arr) != 3 {
		t.Fatalf("SCAN keys = %+v, want 3", r.Arr[1])
	}
	if r := cl.do(t, "DBSIZE"); r.Str != "3" {
		t.Fatalf("DBSIZE = %+v", r)
	}

	// DB isolation: db 1 is empty, writes there don't leak to db 0.
	cl.do(t, "SELECT", "1")
	if r := cl.do(t, "DBSIZE"); r.Str != "0" {
		t.Fatalf("DBSIZE db1 = %+v, want 0", r)
	}
	cl.do(t, "SET", "only1", "x")
	if r := cl.do(t, "DBSIZE"); r.Str != "1" {
		t.Fatalf("DBSIZE db1 after set = %+v", r)
	}
	cl.do(t, "SELECT", "0")
	if r := cl.do(t, "DBSIZE"); r.Str != "3" {
		t.Fatalf("DBSIZE db0 = %+v, want 3 (db1 leaked?)", r)
	}
	if r := cl.do(t, "GET", "only1"); !r.Null {
		t.Fatalf("GET only1 in db0 = %+v, want nil", r)
	}

	// FLUSHDB scoped to current db
	cl.do(t, "FLUSHDB")
	if r := cl.do(t, "DBSIZE"); r.Str != "0" {
		t.Fatalf("DBSIZE after FLUSHDB = %+v", r)
	}
}
