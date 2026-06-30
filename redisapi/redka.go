package redis

import (
	"time"

	"github.com/redis-geo/redgeo/core"
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
	str  RStr
	key  RKey
	hash RHash
	set  RSet
	zset RZSet
	list RList
}

// New builds a Redka from the six backend repositories.
func New(str RStr, key RKey, hash RHash, set RSet, zset RZSet, list RList) Redka {
	return Redka{str: str, key: key, hash: hash, set: set, zset: zset, list: list}
}

func (r Redka) Str() RStr   { return r.str }
func (r Redka) Key() RKey   { return r.key }
func (r Redka) Hash() RHash { return r.hash }
func (r Redka) Set() RSet   { return r.set }
func (r Redka) ZSet() RZSet { return r.zset }
func (r Redka) List() RList { return r.list }
