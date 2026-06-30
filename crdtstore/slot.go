package crdtstore

import (
	"errors"

	"github.com/redis-geo/redgeo/hlc"
)

// Slot tag values (DESIGN §6.7). A deleted slot with a higher HLC beats an
// older present slot, giving last-writer-wins including deletes.
const (
	tagPresent byte = 0
	tagDeleted byte = 1
)

// errBadSlot indicates a corrupt slot envelope.
var errBadSlot = errors.New("crdtstore: malformed slot envelope")

// slot is a decoded per-replica LWW register envelope: (hlc, tag, value).
type slot struct {
	stamp hlc.Stamp
	tag   byte
	value []byte
}

// encodeSlot packs a slot as [12-byte HLC][1-byte tag][value...].
func encodeSlot(s slot) []byte {
	out := make([]byte, 0, 13+len(s.value))
	out = append(out, s.stamp.Encode()...)
	out = append(out, s.tag)
	out = append(out, s.value...)
	return out
}

// decodeSlot parses a slot envelope produced by encodeSlot.
func decodeSlot(b []byte) (slot, error) {
	if len(b) < 13 {
		return slot{}, errBadSlot
	}
	st, ok := hlc.Decode(b[0:12])
	if !ok {
		return slot{}, errBadSlot
	}
	return slot{stamp: st, tag: b[12], value: b[13:]}, nil
}

// winner picks the slot with the maximum (HLC, replicaID) among candidates
// (DESIGN §6.7 read rule). replicaID breaks exact-HLC ties deterministically.
// It returns the winning slot and whether any candidate existed.
func winner(cands map[string]slot) (slot, string, bool) {
	var best slot
	var bestReplica string
	found := false
	for replica, s := range cands {
		if !found {
			best, bestReplica, found = s, replica, true
			continue
		}
		switch best.stamp.Compare(s.stamp) {
		case -1:
			best, bestReplica = s, replica
		case 0:
			if replica > bestReplica { // tie-break by replicaID
				best, bestReplica = s, replica
			}
		}
	}
	return best, bestReplica, found
}

// liveValue reduces a candidate slot set to the live register value: the
// winning slot's bytes if it is present, else (nil, false) for absent/deleted.
func liveValue(cands map[string]slot) ([]byte, bool) {
	best, _, found := winner(cands)
	if !found || best.tag == tagDeleted {
		return nil, false
	}
	return best.value, true
}
