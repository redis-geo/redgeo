// Package command implements Redis-compatible commands for operations on
// data structures. It is a fork of redka's command layer; the registry grows
// one phase at a time (DESIGN §9).
package command

import (
	"strings"

	"github.com/redis-geo/redgeo/command/conn"
	"github.com/redis-geo/redgeo/command/hash"
	"github.com/redis-geo/redgeo/command/key"
	"github.com/redis-geo/redgeo/command/server"
	"github.com/redis-geo/redgeo/command/set"
	str "github.com/redis-geo/redgeo/command/string"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// Parse parses a raw RESP argument list into a Cmd.
func Parse(args [][]byte) (redis.Cmd, error) {
	name := strings.ToLower(string(args[0]))
	b := redis.NewBaseCmd(args)
	switch name {
	// connection
	case "echo":
		return conn.ParseEcho(b)
	case "ping":
		return conn.ParsePing(b)
	case "select":
		return conn.ParseSelect(b)

	// server
	case "command":
		return server.ParseOK(b)
	case "info":
		return server.ParseOK(b)
	case "config":
		return server.ParseOK(b)
	case "lolwut":
		return server.ParseLolwut(b)
	case "dbsize":
		return server.ParseDBSize(b)
	case "flushdb":
		return key.ParseFlushDB(b)
	case "flushall":
		return key.ParseFlushDB(b)

	// string
	case "get":
		return str.ParseGet(b)
	case "set":
		return str.ParseSet(b)
	case "getset":
		return str.ParseGetSet(b)
	case "mget":
		return str.ParseMGet(b)
	case "mset":
		return str.ParseMSet(b)
	case "strlen":
		return str.ParseStrlen(b)

	// key
	case "del":
		return key.ParseDel(b)
	case "unlink":
		return key.ParseDel(b)
	case "exists":
		return key.ParseExists(b)
	case "type":
		return key.ParseType(b)
	case "keys":
		return key.ParseKeys(b)
	case "scan":
		return key.ParseScan(b)
	case "randomkey":
		return key.ParseRandomKey(b)
	case "rename":
		return key.ParseRename(b)
	case "renamenx":
		return key.ParseRenameNX(b)

	// hash
	case "hdel":
		return hash.ParseHDel(b)
	case "hexists":
		return hash.ParseHExists(b)
	case "hget":
		return hash.ParseHGet(b)
	case "hgetall":
		return hash.ParseHGetAll(b)
	case "hkeys":
		return hash.ParseHKeys(b)
	case "hlen":
		return hash.ParseHLen(b)
	case "hmget":
		return hash.ParseHMGet(b)
	case "hmset":
		return hash.ParseHMSet(b)
	case "hscan":
		return hash.ParseHScan(b)
	case "hset":
		return hash.ParseHSet(b)
	case "hsetnx":
		return hash.ParseHSetNX(b)
	case "hvals":
		return hash.ParseHVals(b)

	// set
	case "sadd":
		return set.ParseSAdd(b)
	case "scard":
		return set.ParseSCard(b)
	case "sdiff":
		return set.ParseSDiff(b)
	case "sdiffstore":
		return set.ParseSDiffStore(b)
	case "sinter":
		return set.ParseSInter(b)
	case "sinterstore":
		return set.ParseSInterStore(b)
	case "sismember":
		return set.ParseSIsMember(b)
	case "smembers":
		return set.ParseSMembers(b)
	case "smove":
		return set.ParseSMove(b)
	case "spop":
		return set.ParseSPop(b)
	case "srandmember":
		return set.ParseSRandMember(b)
	case "srem":
		return set.ParseSRem(b)
	case "sscan":
		return set.ParseSScan(b)
	case "sunion":
		return set.ParseSUnion(b)
	case "sunionstore":
		return set.ParseSUnionStore(b)

	default:
		return server.ParseUnknown(b)
	}
}
