// Package command implements Redis-compatible commands for operations on
// data structures. It is a fork of redka's command layer; the registry grows
// one phase at a time (DESIGN §9).
package command

import (
	"strings"

	"github.com/redis-geo/redgeo/command/conn"
	"github.com/redis-geo/redgeo/command/geo"
	"github.com/redis-geo/redgeo/command/hash"
	jsoncmd "github.com/redis-geo/redgeo/command/json"
	"github.com/redis-geo/redgeo/command/key"
	"github.com/redis-geo/redgeo/command/list"
	"github.com/redis-geo/redgeo/command/script"
	"github.com/redis-geo/redgeo/command/server"
	"github.com/redis-geo/redgeo/command/set"
	sketchcmd "github.com/redis-geo/redgeo/command/sketch"
	"github.com/redis-geo/redgeo/command/stream"
	str "github.com/redis-geo/redgeo/command/string"
	"github.com/redis-geo/redgeo/command/zset"
	redis "github.com/redis-geo/redgeo/redisapi"
)

func init() { script.Dispatch = Parse }

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
	case "setex":
		return str.ParseSetEX(b, 1000)
	case "psetex":
		return str.ParseSetEX(b, 1)
	case "incr":
		return str.ParseIncr(b, 1)
	case "decr":
		return str.ParseIncr(b, -1)
	case "incrby":
		return str.ParseIncrBy(b, 1)
	case "decrby":
		return str.ParseIncrBy(b, -1)
	case "incrbyfloat":
		return str.ParseIncrByFloat(b)
	case "setbit":
		return str.ParseSetBit(b)
	case "getbit":
		return str.ParseGetBit(b)
	case "bitcount":
		return str.ParseBitCount(b)
	case "bitpos":
		return str.ParseBitPos(b)
	case "bitop":
		return str.ParseBitOp(b)
	case "pfadd":
		return str.ParsePFAdd(b)
	case "pfcount":
		return str.ParsePFCount(b)
	case "pfmerge":
		return str.ParsePFMerge(b)
	case "pfdebug":
		return str.ParsePFDebug(b)
	case "pfselftest":
		return str.ParsePFSelfTest(b)

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
	case "ttl":
		return key.ParseTTL(b)
	case "pttl":
		return key.ParsePTTL(b)
	case "expire":
		return key.ParseExpire(b, 1000)
	case "pexpire":
		return key.ParseExpire(b, 1)
	case "expireat":
		return key.ParseExpireAt(b, 1000)
	case "pexpireat":
		return key.ParseExpireAt(b, 1)
	case "persist":
		return key.ParsePersist(b)

	// hash
	case "hdel":
		return hash.ParseHDel(b)
	case "hexists":
		return hash.ParseHExists(b)
	case "hget":
		return hash.ParseHGet(b)
	case "hincrby":
		return hash.ParseHIncrBy(b)
	case "hincrbyfloat":
		return hash.ParseHIncrByFloat(b)
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

	// list
	case "lpush":
		return list.ParseLPush(b)
	case "rpush":
		return list.ParseRPush(b)
	case "lpop":
		return list.ParseLPop(b)
	case "rpop":
		return list.ParseRPop(b)
	case "llen":
		return list.ParseLLen(b)
	case "lindex":
		return list.ParseLIndex(b)
	case "lrange":
		return list.ParseLRange(b)
	case "lset":
		return list.ParseLSet(b)
	case "lrem":
		return list.ParseLRem(b)
	case "linsert":
		return list.ParseLInsert(b)
	case "ltrim":
		return list.ParseLTrim(b)
	case "rpoplpush":
		return list.ParseRPopLPush(b)

	// geo (zset-backed)
	case "geoadd":
		return geo.ParseGeoAdd(b)
	case "geopos":
		return geo.ParseGeoPos(b)
	case "geodist":
		return geo.ParseGeoDist(b)
	case "geohash":
		return geo.ParseGeoHash(b)
	case "geosearch":
		return geo.ParseGeoSearch(b)
	case "geosearchstore":
		return geo.ParseGeoSearchStore(b)

	// JSON (RedisJSON subset)
	case "json.set":
		return jsoncmd.ParseSet(b)
	case "json.mset":
		return jsoncmd.ParseMSet(b)
	case "json.get":
		return jsoncmd.ParseGet(b)
	case "json.mget":
		return jsoncmd.ParseMGet(b)
	case "json.del", "json.forget":
		return jsoncmd.ParseDel(b)
	case "json.type", "json.strlen", "json.arrlen", "json.objlen", "json.objkeys":
		return jsoncmd.ParseQuery(b, strings.TrimPrefix(name, "json."))
	case "json.numincrby":
		return jsoncmd.ParseNumOp(b, false)
	case "json.nummultby":
		return jsoncmd.ParseNumOp(b, true)
	case "json.toggle":
		return jsoncmd.ParseToggle(b)
	case "json.clear":
		return jsoncmd.ParseClear(b)
	case "json.strappend":
		return jsoncmd.ParseStrAppend(b)
	case "json.arrappend":
		return jsoncmd.ParseArrAppend(b)
	case "json.arrinsert":
		return jsoncmd.ParseArrInsert(b)
	case "json.arrpop":
		return jsoncmd.ParseArrPop(b)
	case "json.arrtrim":
		return jsoncmd.ParseArrTrim(b)
	case "json.arrindex":
		return jsoncmd.ParseArrIndex(b)

	// Bloom filter (RedisBloom subset)
	case "bf.reserve":
		return sketchcmd.ParseBFReserve(b)
	case "bf.add":
		return sketchcmd.ParseBFAdd(b, true)
	case "bf.madd":
		return sketchcmd.ParseBFAdd(b, false)
	case "bf.insert":
		return sketchcmd.ParseBFInsert(b)
	case "bf.exists":
		return sketchcmd.ParseBFExists(b, true)
	case "bf.mexists":
		return sketchcmd.ParseBFExists(b, false)
	case "bf.info":
		return sketchcmd.ParseBFInfo(b, false)
	case "bf.card":
		return sketchcmd.ParseBFInfo(b, true)

	// count-min sketch
	case "cms.initbydim":
		return sketchcmd.ParseCMSInitByDim(b)
	case "cms.initbyprob":
		return sketchcmd.ParseCMSInitByProb(b)
	case "cms.incrby":
		return sketchcmd.ParseCMSIncrBy(b)
	case "cms.query":
		return sketchcmd.ParseCMSQuery(b)
	case "cms.merge":
		return sketchcmd.ParseCMSMerge(b)
	case "cms.info":
		return sketchcmd.ParseCMSInfo(b)

	// scripting
	case "eval", "eval_ro":
		return script.ParseEval(b, false)
	case "evalsha", "evalsha_ro":
		return script.ParseEval(b, true)
	case "script":
		return script.ParseScript(b)

	// stream
	case "xadd":
		return stream.ParseXAdd(b)
	case "xlen":
		return stream.ParseXLen(b)
	case "xrange":
		return stream.ParseXRange(b, false)
	case "xrevrange":
		return stream.ParseXRange(b, true)
	case "xdel":
		return stream.ParseXDel(b)
	case "xtrim":
		return stream.ParseXTrim(b)
	case "xread":
		return stream.ParseXRead(b)
	case "xgroup":
		return stream.ParseXGroup(b)
	case "xreadgroup":
		return stream.ParseXReadGroup(b)
	case "xack":
		return stream.ParseXAck(b)
	case "xpending":
		return stream.ParseXPending(b)
	case "xclaim":
		return stream.ParseXClaim(b)
	case "xautoclaim":
		return stream.ParseXAutoClaim(b)
	case "xinfo":
		return stream.ParseXInfo(b)

	// sorted set
	case "zadd":
		return zset.ParseZAdd(b)
	case "zscore":
		return zset.ParseZScore(b)
	case "zrem":
		return zset.ParseZRem(b)
	case "zcard":
		return zset.ParseZCard(b)
	case "zcount":
		return zset.ParseZCount(b)
	case "zincrby":
		return zset.ParseZIncrBy(b)
	case "zrank":
		return zset.ParseZRank(b, false)
	case "zrevrank":
		return zset.ParseZRank(b, true)
	case "zrange":
		return zset.ParseZRange(b)
	case "zrevrange":
		return zset.ParseZRevRange(b)
	case "zrangebyscore":
		return zset.ParseZRangeByScore(b, false)
	case "zrevrangebyscore":
		return zset.ParseZRangeByScore(b, true)
	case "zremrangebyrank":
		return zset.ParseZRemRangeByRank(b)
	case "zremrangebyscore":
		return zset.ParseZRemRangeByScore(b)
	case "zscan":
		return zset.ParseZScan(b)
	case "zunionstore":
		return zset.ParseZUnionStore(b)
	case "zinterstore":
		return zset.ParseZInterStore(b)

	default:
		return server.ParseUnknown(b)
	}
}
