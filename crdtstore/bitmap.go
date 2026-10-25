package crdtstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis-geo/redgeo/core"
	"github.com/redis-geo/redgeo/restypes"
)

// Bitmaps (SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP). Redis bitmaps are strings, so
// a bitmap is TypeString/flavorBitmap and GET/STRLEN return its assembled
// bytes. To avoid rewriting the whole value per bit, data is split into
// bitChunk-byte chunks, each a per-replica HLC-LWW slot (§6.7):
//
//	/{P}/d/{db}/{key}/b/{chunk:%08x}/{replicaID}
//
// Concurrent flips in DIFFERENT chunks always merge; concurrent flips inside
// the SAME chunk resolve last-writer-wins at chunk granularity (one flip can
// be lost). A chunk value is trimmed to its highest written byte and absent
// chunks read as zeros, so the string length is derivable from the last chunk.
// Deletes are deleted-tagged slots with a fresh HLC, never ds.Delete.

const (
	bitChunk    = 512
	maxBitIndex = 1<<32 - 1
)

type bitmapData struct {
	live map[int][]byte // winning present chunks
}

func (s *Store) bitmapLoad(ctx context.Context, db int, key string) (bitmapData, error) {
	base := bitBase(db, key)
	entries, err := s.query(ctx, base, false)
	if err != nil {
		return bitmapData{}, err
	}
	byChunk := map[int]map[string]slot{}
	for _, e := range entries {
		rest := strings.TrimPrefix(e.Key, base)
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			continue
		}
		idx, perr := strconv.ParseUint(rest[:i], 16, 32)
		replica, derr := decSeg(rest[i+1:])
		sl, serr := decodeSlot(e.Value)
		if perr != nil || derr != nil || serr != nil {
			continue
		}
		if byChunk[int(idx)] == nil {
			byChunk[int(idx)] = map[string]slot{}
		}
		byChunk[int(idx)][replica] = sl
	}
	out := bitmapData{live: map[int][]byte{}}
	for idx, cands := range byChunk {
		if v, ok := liveValue(cands); ok {
			out.live[idx] = v
		}
	}
	return out, nil
}

// bytes assembles the chunks into the string value.
func (d bitmapData) bytes() []byte {
	last, n := -1, 0
	for idx, v := range d.live {
		if idx > last {
			last, n = idx, idx*bitChunk+len(v)
		}
	}
	if last < 0 {
		return nil
	}
	out := make([]byte, n)
	for idx, v := range d.live {
		copy(out[idx*bitChunk:], v)
	}
	return out
}

// bitmapWrite makes data the bitmap's content under key: present slots for
// non-empty chunks, deleted slots for chunks that no longer exist, plus meta.
// Caller holds the key lock.
func (s *Store) bitmapWrite(ctx context.Context, db int, key string, data []byte, etimeMS int64) error {
	old, err := s.bitmapLoad(ctx, db, key)
	if err != nil {
		return err
	}
	keep := map[int]bool{}
	nChunks := (len(data) + bitChunk - 1) / bitChunk
	for idx := 0; idx < nChunks; idx++ {
		chunk := data[idx*bitChunk : min((idx+1)*bitChunk, len(data))]
		if idx < nChunks-1 && allZero(chunk) {
			continue // zeros are implicit, except the last chunk fixes the length
		}
		keep[idx] = true
		if err := s.writeSlot(ctx, bitSlot(db, key, idx, s.replica()), tagPresent, chunk); err != nil {
			return err
		}
	}
	for idx := range old.live {
		if !keep[idx] {
			if err := s.writeSlot(ctx, bitSlot(db, key, idx, s.replica()), tagDeleted, nil); err != nil {
				return err
			}
		}
	}
	return s.writeMeta(ctx, db, key, metaEnvelope{
		KeyMeta: KeyMeta{Type: core.TypeString, ETimeMS: etimeMS},
		Flavor:  flavorBitmap,
	})
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func (s *Store) deleteBitmap(ctx context.Context, db int, key string) error {
	d, err := s.bitmapLoad(ctx, db, key)
	if err != nil {
		return err
	}
	for idx := range d.live {
		if err := s.writeSlot(ctx, bitSlot(db, key, idx, s.replica()), tagDeleted, nil); err != nil {
			return err
		}
	}
	return s.deleteMeta(ctx, db, key)
}

func (s *Store) copyBitmap(ctx context.Context, db int, key, newKey string, etimeMS int64) error {
	d, err := s.bitmapLoad(ctx, db, key)
	if err != nil {
		return err
	}
	return s.bitmapWrite(ctx, db, newKey, d.bytes(), etimeMS)
}

// bitmapKind classifies a key for the bit commands.
type bitmapKind int

const (
	bitAbsent bitmapKind = iota
	bitIsBitmap
	bitIsString // plain string or counter: readable as bytes
)

func (r strRepo) bitKind(ctx context.Context, key string) (bitmapKind, int64, error) {
	k, ok, err := r.s.probe(ctx, r.db, key)
	if err != nil || !ok {
		return bitAbsent, 0, err
	}
	if k.Type != core.TypeString {
		return bitAbsent, 0, core.ErrKeyType
	}
	m, _, _, err := r.s.readMeta(ctx, r.db, key)
	if err != nil {
		return bitAbsent, 0, err
	}
	switch {
	case m.Flavor == flavorBitmap:
		return bitIsBitmap, m.ETimeMS, nil
	case m.Flavor == flavorHLL:
		return bitAbsent, 0, core.ErrKeyType
	}
	return bitIsString, m.ETimeMS, nil
}

// SetBit sets one bit and returns its previous value. A plain string is
// converted to a bitmap in place; counters and HLLs are rejected.
func (r strRepo) SetBit(key string, offset uint64, on bool) (bool, error) {
	if offset > maxBitIndex {
		return false, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}
	ctx := bg()
	unlock := r.s.locks.Lock(lockKey(r.db, key))
	defer unlock()
	kind, etime, err := r.bitKind(ctx, key)
	if err != nil {
		return false, err
	}
	byteOff := int(offset >> 3)
	mask := byte(0x80 >> (offset & 7))
	idx, in := byteOff/bitChunk, byteOff%bitChunk

	if kind == bitIsString {
		m, _, _, err := r.s.readMeta(ctx, r.db, key)
		if err != nil {
			return false, err
		}
		if m.Flavor != flavorString {
			return false, core.ErrKeyType // counters stay counters (§6.4)
		}
		v, err := r.readValue(ctx, key)
		if err != nil {
			return false, err
		}
		if err := r.s.writeSlot(ctx, strSlot(r.db, key, r.s.replica()), tagDeleted, nil); err != nil {
			return false, err
		}
		data := append([]byte(nil), v...)
		old := false
		if byteOff < len(data) {
			old = data[byteOff]&mask != 0
		} else {
			data = append(data, make([]byte, byteOff+1-len(data))...)
		}
		if on {
			data[byteOff] |= mask
		} else {
			data[byteOff] &^= mask
		}
		return old, r.s.bitmapWrite(ctx, r.db, key, data, etime)
	}

	var chunk []byte
	if kind == bitIsBitmap {
		d, err := r.s.bitmapLoad(ctx, r.db, key)
		if err != nil {
			return false, err
		}
		chunk = d.live[idx]
	}
	old := in < len(chunk) && chunk[in]&mask != 0
	if old == on && kind == bitIsBitmap && in < len(chunk) {
		return old, nil // no change
	}
	next := make([]byte, max(len(chunk), in+1))
	copy(next, chunk)
	if on {
		next[in] |= mask
	} else {
		next[in] &^= mask
	}
	if err := r.s.writeSlot(ctx, bitSlot(r.db, key, idx, r.s.replica()), tagPresent, next); err != nil {
		return false, err
	}
	if kind == bitAbsent {
		err = r.s.writeMeta(ctx, r.db, key, metaEnvelope{
			KeyMeta: KeyMeta{Type: core.TypeString},
			Flavor:  flavorBitmap,
		})
	}
	return old, err
}

// loadBytes returns a string key's bytes (nil, false if absent).
func (r strRepo) loadBytes(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := r.readValue(ctx, key)
	switch err {
	case nil:
		return v, true, nil
	case core.ErrNotFound:
		return nil, false, nil
	}
	return nil, false, err
}

func (r strRepo) GetBit(key string, offset uint64) (bool, error) {
	if offset > maxBitIndex {
		return false, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}
	ctx := bg()
	kind, _, err := r.bitKind(ctx, key)
	if err != nil || kind == bitAbsent {
		return false, err
	}
	byteOff := int(offset >> 3)
	var b byte
	if kind == bitIsBitmap {
		cands, err := r.s.readSlots(ctx, bitChunkBase(r.db, key, byteOff/bitChunk))
		if err != nil {
			return false, err
		}
		chunk, _ := liveValue(cands)
		if in := byteOff % bitChunk; in < len(chunk) {
			b = chunk[in]
		}
	} else {
		v, _, err := r.loadBytes(ctx, key)
		if err != nil {
			return false, err
		}
		if byteOff < len(v) {
			b = v[byteOff]
		}
	}
	return b&byte(0x80>>(offset&7)) != 0, nil
}

func (r strRepo) BitCount(key string, rng restypes.BitRange) (int64, error) {
	data, _, err := r.loadBytes(bg(), key)
	if err != nil {
		return 0, err
	}
	s, e, ok := bitRange(int64(len(data)), rng.Start, rng.End, rng.HasStart, rng.HasEnd, rng.InBits)
	if !ok {
		return 0, nil
	}
	return bitCountRange(data, s, e, rng.InBits), nil
}

func (r strRepo) BitPos(key string, bit bool, rng restypes.BitRange) (int64, error) {
	data, exists, err := r.loadBytes(bg(), key)
	if err != nil {
		return 0, err
	}
	if !exists {
		if bit {
			return -1, nil
		}
		return 0, nil
	}
	s, e, ok := bitRange(int64(len(data)), rng.Start, rng.End, rng.HasStart, rng.HasEnd, rng.InBits)
	if !ok {
		return -1, nil
	}
	sb, eb := s, e
	if !rng.InBits {
		sb, eb = s*8, e*8+7
	}
	if pos := bitPosRange(data, bit, sb, eb); pos >= 0 {
		return pos, nil
	}
	if !bit && !rng.HasEnd {
		return eb + 1, nil // all ones: first bit past the string
	}
	return -1, nil
}

// BitOp stores the bitwise combination of srcs in dest and returns its length.
func (r strRepo) BitOp(op, dest string, srcs ...string) (int64, error) {
	ctx := bg()
	var in [][]byte
	for _, src := range srcs {
		v, _, err := r.loadBytes(ctx, src)
		if err != nil {
			return 0, err
		}
		in = append(in, v)
	}
	out := bitop(op, in)

	unlock := r.s.locks.Lock(lockKey(r.db, dest))
	defer unlock()
	k, ok, err := r.s.probe(ctx, r.db, dest)
	if err != nil {
		return 0, err
	}
	if len(out) == 0 {
		if ok {
			return 0, r.s.deleteKey(ctx, r.db, dest, k.Type)
		}
		return 0, nil
	}
	if ok {
		kind, _, err := r.bitKind(ctx, dest)
		if kind != bitIsBitmap && (err == nil || err == core.ErrKeyType) {
			if derr := r.s.deleteKey(ctx, r.db, dest, k.Type); derr != nil {
				return 0, derr
			}
		}
	}
	return int64(len(out)), r.s.bitmapWrite(ctx, r.db, dest, out, 0)
}
