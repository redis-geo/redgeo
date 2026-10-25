package redis

import (
	"errors"
	"time"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/jsondoc"
	"github.com/redis-geo/redgeo/restypes"
)

// The six storage interfaces are the seam between the command layer and the
// backend (DESIGN §3). The crdtstore backend implements them against
// go-ds-crdt. They mirror redka's R* interfaces, except builder-returning
// methods are replaced with direct option-struct methods (DESIGN's blessed
// "lift result types" refactor) so the interfaces stay backend-neutral.

// RStr is the string/register storage interface.
type RStr interface {
	Get(key string) (core.Value, error)
	GetMany(keys ...string) (map[string]core.Value, error)
	Set(key string, value any) error
	SetExpire(key string, value any, ttl time.Duration) error
	SetMany(items map[string]any) error
	SetWith(key string, value any, opts restypes.SetOpts) (restypes.SetOut, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)

	// HyperLogLog (a string-typed sketch, merged by register-wise max).
	PFAdd(key string, elems ...[]byte) (bool, error)
	PFCount(keys ...string) (int64, error)
	PFMerge(dest string, srcs ...string) error

	// Bitmaps (a string-typed chunked value).
	SetBit(key string, offset uint64, on bool) (bool, error)
	GetBit(key string, offset uint64) (bool, error)
	BitCount(key string, rng restypes.BitRange) (int64, error)
	BitPos(key string, bit bool, rng restypes.BitRange) (int64, error)
	BitOp(op, dest string, srcs ...string) (int64, error)
}

// RKey is the generic key-management storage interface.
type RKey interface {
	Count(keys ...string) (int, error)
	Delete(keys ...string) (int, error)
	DeleteAll() error
	Exists(key string) (bool, error)
	Expire(key string, ttl time.Duration) error
	ExpireAt(key string, at time.Time) error
	Persist(key string) error
	Get(key string) (core.Key, error)
	Keys(pattern string) ([]core.Key, error)
	Len() (int, error)
	Random() (core.Key, error)
	Rename(key, newKey string) error
	RenameNotExists(key, newKey string) (bool, error)
	Scan(cursor int, pattern string, ktype core.TypeID, count int) (restypes.KeyScanResult, error)
}

// RHash is the hash (LWW-Map) storage interface.
type RHash interface {
	Delete(key string, fields ...string) (int, error)
	Exists(key, field string) (bool, error)
	Fields(key string) ([]string, error)
	Get(key, field string) (core.Value, error)
	GetMany(key string, fields ...string) (map[string]core.Value, error)
	Incr(key, field string, delta int) (int, error)
	IncrFloat(key, field string, delta float64) (float64, error)
	Items(key string) (map[string]core.Value, error)
	Len(key string) (int, error)
	Scan(key string, cursor int, pattern string, count int) (restypes.HashScanResult, error)
	Set(key, field string, value any) (bool, error)
	SetMany(key string, items map[string]any) (int, error)
	SetNotExists(key, field string, value any) (bool, error)
	Values(key string) ([]core.Value, error)
}

// RSet is the set (native OR-Set) storage interface.
type RSet interface {
	Add(key string, elems ...any) (int, error)
	Delete(key string, elems ...any) (int, error)
	Diff(keys ...string) ([]core.Value, error)
	DiffStore(dest string, keys ...string) (int, error)
	Exists(key string, elem any) (bool, error)
	Inter(keys ...string) ([]core.Value, error)
	InterStore(dest string, keys ...string) (int, error)
	Items(key string) ([]core.Value, error)
	Len(key string) (int, error)
	Move(src, dest string, elem any) error
	Pop(key string) (core.Value, error)
	Random(key string) (core.Value, error)
	Scan(key string, cursor int, pattern string, count int) (restypes.SetScanResult, error)
	Union(keys ...string) ([]core.Value, error)
	UnionStore(dest string, keys ...string) (int, error)
}

// RZSet is the sorted-set (score LWW-Map) storage interface.
type RZSet interface {
	Add(key string, elem any, score float64) (bool, error)
	AddMany(key string, items map[any]float64) (int, error)
	Count(key string, min, max float64) (int, error)
	Delete(key string, elems ...any) (int, error)
	DeleteRange(key string, opts restypes.RangeOpts) (int, error)
	GetRank(key string, elem any) (rank int, score float64, err error)
	GetRankRev(key string, elem any) (rank int, score float64, err error)
	GetScore(key string, elem any) (float64, error)
	Incr(key string, elem any, delta float64) (float64, error)
	Inter(keys ...string) ([]restypes.ZSetItem, error)
	InterStore(dest string, aggregate string, keys ...string) (int, error)
	Len(key string) (int, error)
	Range(key string, opts restypes.RangeOpts) ([]restypes.ZSetItem, error)
	Scan(key string, cursor int, pattern string, count int) (restypes.ZScanResult, error)
	Union(keys ...string) ([]restypes.ZSetItem, error)
	UnionStore(dest string, aggregate string, keys ...string) (int, error)
}

// RStream is the stream (append-only, HLC-ordered log) storage interface.
type RStream interface {
	// Add appends an entry; ok=false when NoMkStream and the stream is absent.
	Add(key string, opts restypes.StreamAddOpts, fields [][]byte) (id restypes.StreamID, ok bool, err error)
	Len(key string) (int, error)
	// Range returns live entries with lo <= ID <= hi in ascending order, or
	// descending when rev; count <= 0 means unlimited.
	Range(key string, lo, hi restypes.StreamBound, count int, rev bool) ([]restypes.StreamEntry, error)
	Del(key string, ids ...restypes.StreamID) (int, error)
	Trim(key string, t restypes.StreamTrim) (int, error)
	// LastID is the highest ID ever added (0-0 if none), as `$` in XREAD.
	LastID(key string) (restypes.StreamID, error)

	// Consumer groups. Delivery state is merged across replicas without
	// coordination, so delivery is at-least-once: two regions can hand the same
	// entry to different consumers (DESIGN §12.3b).
	GroupCreate(key, group string, id restypes.StreamID, mkstream bool) error
	GroupDestroy(key, group string) (bool, error)
	GroupSetID(key, group string, id restypes.StreamID) error
	ConsumerCreate(key, group, consumer string) (bool, error)
	ConsumerDelete(key, group, consumer string) (pending int, err error)
	// Groups / Group describe consumer groups (ErrNoGroup if absent).
	Groups(key string) ([]restypes.GroupInfo, error)
	Group(key, group string) (restypes.GroupInfo, error)
	// ReadNew delivers up to count (<=0: all) entries after the group's
	// last-delivered ID to consumer, recording them as pending unless noack.
	ReadNew(key, group, consumer string, count int, noack bool) ([]restypes.StreamEntry, error)
	// ReadPending re-reads consumer's own pending entries with ID > after. An
	// entry deleted from the stream comes back with nil Fields.
	ReadPending(key, group, consumer string, after restypes.StreamID, count int) ([]restypes.StreamEntry, error)
	Ack(key, group string, ids ...restypes.StreamID) (int, error)
	// Claim moves pending entries to consumer. Entries deleted from the stream
	// are dropped from the pending list and returned in deleted.
	Claim(key, group, consumer string, ids []restypes.StreamID, o restypes.ClaimOpts) (claimed []restypes.StreamEntry, deleted []restypes.StreamID, err error)
}

// ErrNoGroup / ErrGroupExists are the consumer-group error sentinels.
var (
	ErrNoGroup     = errors.New("NOGROUP No such key or consumer group")
	ErrGroupExists = errors.New("BUSYGROUP Consumer Group name already exists")
)

// RJSON is the JSON document storage interface. A document is loaded into a
// jsondoc.Doc, mutated by fn, and the difference is persisted.
type RJSON interface {
	// View runs fn over a snapshot; core.ErrNotFound if the key is absent.
	View(key string, fn func(d *jsondoc.Doc) error) error
	// Update runs fn under the key lock and persists changes. With create a
	// missing key yields an empty document; otherwise core.ErrNotFound.
	Update(key string, create bool, fn func(d *jsondoc.Doc) error) error
}

// RList is the list (fractional-index sequence) storage interface.
type RList interface {
	Delete(key string, elem any) (int, error)
	DeleteBack(key string, elem any, count int) (int, error)
	DeleteFront(key string, elem any, count int) (int, error)
	Get(key string, idx int) (core.Value, error)
	InsertAfter(key string, pivot, elem any) (int, error)
	InsertBefore(key string, pivot, elem any) (int, error)
	Len(key string) (int, error)
	PopBack(key string) (core.Value, error)
	PopBackPushFront(src, dest string) (core.Value, error)
	PopFront(key string) (core.Value, error)
	PushBack(key string, elem any) (int, error)
	PushFront(key string, elem any) (int, error)
	Range(key string, start, stop int) ([]core.Value, error)
	Set(key string, idx int, elem any) error
	Trim(key string, start, stop int) (int, error)
}

// Redka bundles the six storage interfaces and is passed to every command's
// Run method. The wiring layer constructs it from a crdtstore backend.
type Redka struct {
	str    RStr
	key    RKey
	hash   RHash
	set    RSet
	zset   RZSet
	list   RList
	stream RStream
	json   RJSON
}

// New builds a Redka from the six backend repositories.
func New(str RStr, key RKey, hash RHash, set RSet, zset RZSet, list RList, stream RStream, json RJSON) Redka {
	return Redka{str: str, key: key, hash: hash, set: set, zset: zset, list: list, stream: stream, json: json}
}

func (r Redka) Str() RStr       { return r.str }
func (r Redka) Key() RKey       { return r.key }
func (r Redka) Hash() RHash     { return r.hash }
func (r Redka) Set() RSet       { return r.set }
func (r Redka) ZSet() RZSet     { return r.zset }
func (r Redka) List() RList     { return r.list }
func (r Redka) JSON() RJSON     { return r.json }
func (r Redka) Stream() RStream { return r.stream }
