// Package hlc implements a Hybrid Logical Clock (DESIGN §6.7).
//
// Every single-value register write is stamped with an HLC so that reads can
// pick the wall-clock-latest writer (true last-writer-wins) across replicas,
// instead of relying on go-ds-crdt's DAG-height resolution. The clock tracks
// (physical wall-ms, logical counter); the logical counter disambiguates
// multiple events within the same millisecond and absorbs modest clock skew.
package hlc

import (
	"encoding/binary"
	"sync"
	"time"
)

// Stamp is a single HLC reading: a physical millisecond component plus a
// logical counter. Stamps are totally ordered by (WallMS, Logical), with the
// writing replicaID as the final tie-breaker at the slot layer.
type Stamp struct {
	WallMS  int64
	Logical uint32
}

// Compare returns -1, 0, or +1 ordering a before b.
func (a Stamp) Compare(b Stamp) int {
	switch {
	case a.WallMS < b.WallMS:
		return -1
	case a.WallMS > b.WallMS:
		return 1
	case a.Logical < b.Logical:
		return -1
	case a.Logical > b.Logical:
		return 1
	default:
		return 0
	}
}

// Less reports whether a orders strictly before b.
func (a Stamp) Less(b Stamp) bool { return a.Compare(b) < 0 }

// Encode packs the stamp into 12 big-endian bytes (8 wall + 4 logical) so that
// lexicographic byte order matches Stamp ordering.
func (a Stamp) Encode() []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(a.WallMS))
	binary.BigEndian.PutUint32(buf[8:12], a.Logical)
	return buf
}

// Decode parses a 12-byte encoding produced by Encode.
func Decode(b []byte) (Stamp, bool) {
	if len(b) != 12 {
		return Stamp{}, false
	}
	return Stamp{
		WallMS:  int64(binary.BigEndian.Uint64(b[0:8])),
		Logical: binary.BigEndian.Uint32(b[8:12]),
	}, true
}

// Clock is a thread-safe Hybrid Logical Clock for one replica.
type Clock struct {
	mu   sync.Mutex
	last Stamp
	now  func() int64 // wall clock in unix ms; injectable for tests
}

// New returns a Clock driven by the system wall clock.
func New() *Clock {
	return &Clock{now: func() int64 { return time.Now().UnixMilli() }}
}

// NewWithClock returns a Clock driven by the supplied wall-ms function.
func NewWithClock(now func() int64) *Clock {
	return &Clock{now: now}
}

// Now advances and returns the next local HLC stamp following the standard
// HLC send/local-event rule: take the max of the last stamp and wall-clock; if
// the physical part didn't advance, bump the logical counter, else reset it.
func (c *Clock) Now() Stamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now()
	if pt > c.last.WallMS {
		c.last = Stamp{WallMS: pt, Logical: 0}
	} else {
		c.last.Logical++
	}
	return c.last
}

// Observe merges a remote stamp into the local clock (HLC receive rule), so
// that local stamps issued afterward strictly dominate the observed one.
func (c *Clock) Observe(remote Stamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now()
	max := c.last
	if remote.Compare(max) > 0 {
		max = remote
	}
	switch {
	case pt > max.WallMS:
		c.last = Stamp{WallMS: pt, Logical: 0}
	default:
		c.last = Stamp{WallMS: max.WallMS, Logical: max.Logical + 1}
	}
}
