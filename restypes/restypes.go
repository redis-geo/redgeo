// Package restypes holds backend-neutral result and option structs that appear
// in the redisapi R* interface signatures. redka leaked concrete repo structs
// (rhash.ScanResult, rstring.SetCmd, rzset.SetItem, …) into those interfaces;
// per DESIGN §3 we lift them here so the interfaces stay backend-neutral and
// the command layer and the crdtstore backend can both depend on this leaf
// package without an import cycle.
package restypes

import (
	"strconv"

	"github.com/redis-geo/redgeo/core"
)

// ---- strings ----

// SetOpts carries the options of a SET command (replaces redka's SetCmd builder).
type SetOpts struct {
	TTLMS       int64 // relative TTL in ms; 0 = none
	AtMS        int64 // absolute expiry unix ms; 0 = none
	KeepTTL     bool
	IfExists    bool // XX
	IfNotExists bool // NX
	Get         bool // return previous value
}

// SetOut is the result of a SET-with-options call.
type SetOut struct {
	Prev    core.Value
	Created bool
	Updated bool
}

// BitRange is the optional [start,end] range of BITCOUNT/BITPOS (bytes unless
// InBits).
type BitRange struct {
	Start, End       int64
	HasStart, HasEnd bool
	InBits           bool
}

// ---- streams ----

// StreamID is a stream entry ID, ordered by (Ms, Seq).
type StreamID struct{ Ms, Seq uint64 }

func (a StreamID) Cmp(b StreamID) int {
	switch {
	case a.Ms != b.Ms:
		if a.Ms < b.Ms {
			return -1
		}
		return 1
	case a.Seq != b.Seq:
		if a.Seq < b.Seq {
			return -1
		}
		return 1
	}
	return 0
}

func (a StreamID) String() string {
	return strconv.FormatUint(a.Ms, 10) + "-" + strconv.FormatUint(a.Seq, 10)
}

// StreamEntry is one stream entry: an ID and its flat field/value list.
type StreamEntry struct {
	ID     StreamID
	Fields [][]byte
}

// StreamBound is one end of an XRANGE interval.
type StreamBound struct {
	ID   StreamID
	Excl bool
}

// StreamIDMode says how XADD picks the ID.
type StreamIDMode int

const (
	IDExplicit StreamIDMode = iota // ms-seq given
	IDAuto                         // *
	IDAutoSeq                      // ms-*
)

// StreamTrim is a MAXLEN / MINID trim request (approximate "~" is treated as
// exact, which satisfies its "at least" contract).
type StreamTrim struct {
	MaxLen   int64 // >= 0 when ByMaxLen
	ByMaxLen bool
	MinID    StreamID
	ByMinID  bool
}

// StreamAddOpts carries the options of XADD.
type StreamAddOpts struct {
	NoMkStream bool
	Mode       StreamIDMode
	ID         StreamID // IDExplicit: full ID; IDAutoSeq: only Ms
	Trim       *StreamTrim
}

// ---- stream consumer groups ----

// PendingEntry is one delivered-but-unacknowledged entry of a consumer group.
type PendingEntry struct {
	ID          StreamID
	Consumer    string
	DeliveredMS int64 // unix ms of the last delivery
	Count       int   // times delivered
}

// ConsumerInfo describes a consumer of a group.
type ConsumerInfo struct {
	Name    string
	Pending int
	SeenMS  int64 // unix ms of its last activity
}

// GroupInfo describes a consumer group.
type GroupInfo struct {
	Name      string
	LastID    StreamID // last-delivered ID
	Consumers []ConsumerInfo
	Pending   []PendingEntry // ascending by ID
}

// ClaimOpts carries the options of XCLAIM / XAUTOCLAIM.
type ClaimOpts struct {
	MinIdleMS  int64
	IdleMS     int64 // XCLAIM IDLE: set the entry's idle time
	TimeMS     int64 // XCLAIM TIME: set the delivery time (unix ms); 0 = unset
	RetryCount int   // XCLAIM RETRYCOUNT; -1 = unset
	Force      bool
	JustID     bool // do not increment the delivery count
}

// ---- probabilistic sketches ----

// BloomOpts are the parameters of a Bloom filter. Expansion 0 means
// non-scaling (a single fixed-size filter).
type BloomOpts struct {
	ErrorRate float64
	Capacity  int64
	Expansion int
}

// BloomInfo describes a Bloom filter (BF.INFO).
type BloomInfo struct {
	Capacity  int64 // total capacity of all sub-filters
	SizeBytes int64
	Filters   int
	Items     int64
	Expansion int // 0 = non-scaling
}

// ---- hashes ----

// HashItem is a single field/value pair of a hash.
type HashItem struct {
	Field string
	Value core.Value
}

// HashScanResult is the result of an HSCAN call.
type HashScanResult struct {
	Cursor int
	Items  []HashItem
}

// ---- keys ----

// KeyScanResult is the result of a SCAN call.
type KeyScanResult struct {
	Cursor int
	Keys   []core.Key
}

// ---- sets ----

// SetScanResult is the result of an SSCAN call.
type SetScanResult struct {
	Cursor int
	Items  []core.Value
}

// ---- sorted sets ----

// ZSetItem is a member/score pair of a sorted set.
type ZSetItem struct {
	Elem  core.Value
	Score float64
}

// ZScanResult is the result of a ZSCAN call.
type ZScanResult struct {
	Cursor int
	Items  []ZSetItem
}

// RangeOpts carries the options of the ZSet range family (replaces redka's
// RangeCmd builder). Exactly one of ByRank/ByScore is set.
type RangeOpts struct {
	ByRank  bool
	ByScore bool
	Min     float64 // ByScore: score lower bound; ByRank: start index
	Max     float64 // ByScore: score upper bound; ByRank: stop index
	Start   int     // ByRank start (when ByRank)
	Stop    int     // ByRank stop (when ByRank)
	Desc    bool
	Offset  int
	Count   int
}
