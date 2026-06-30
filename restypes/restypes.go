// Package restypes holds backend-neutral result and option structs that appear
// in the redisapi R* interface signatures. redka leaked concrete repo structs
// (rhash.ScanResult, rstring.SetCmd, rzset.SetItem, …) into those interfaces;
// per DESIGN §3 we lift them here so the interfaces stay backend-neutral and
// the command layer and the crdtstore backend can both depend on this leaf
// package without an import cycle.
package restypes

import "github.com/redis-geo/redgeo/core"

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
