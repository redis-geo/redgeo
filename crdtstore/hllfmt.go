package crdtstore

// Redis' HyperLogLog string format, so sketches can be moved between redgeo and
// Redis with GET/SET (and so PFADD works on a restored value):
//
//	"HYLL" | encoding (0 dense, 1 sparse) | 3 unused | 8-byte cached cardinality | registers
//
// Dense: 16384 six-bit registers, little-endian bit packing (12288 bytes).
// Sparse: opcodes — ZERO 00xxxxxx (1-64 zeros), XZERO 01xxxxxx yyyyyyyy (1-16384
// zeros), VAL 1vvvvvxx (value vvvvv+1 repeated xx+1 times). We always write
// dense. The cached cardinality is written as "invalid" (bit 7 of byte 15), which
// makes Redis recompute it.

const (
	hyllHeader = 16
	hyllDense  = hyllHeader + hllM*6/8
)

var hyllMagic = [4]byte{'H', 'Y', 'L', 'L'}

// encodeRedis renders the sketch as a dense Redis HLL string.
func (h *hllSketch) encodeRedis() []byte {
	out := make([]byte, hyllDense)
	copy(out, hyllMagic[:])
	out[15] = 0x80 // cached cardinality invalid
	regs := out[hyllHeader:]
	for i, v := range h {
		if v == 0 {
			continue
		}
		byteIdx, fb := i*6/8, uint(i*6)&7
		regs[byteIdx] |= v << fb
		if fb > 2 { // the value straddles into the next byte
			regs[byteIdx+1] |= v >> (8 - fb)
		}
	}
	return out
}

// decodeRedis parses a dense or sparse Redis HLL string; ok=false if it is not a
// well-formed one.
func decodeRedis(b []byte) (*hllSketch, bool) {
	if len(b) < hyllHeader || [4]byte(b[:4]) != hyllMagic {
		return nil, false
	}
	h := new(hllSketch)
	body := b[hyllHeader:]
	switch b[4] {
	case 0:
		if len(b) != hyllDense {
			return nil, false
		}
		for i := range h {
			byteIdx, fb := i*6/8, uint(i*6)&7
			v := body[byteIdx] >> fb
			if byteIdx+1 < len(body) {
				v |= body[byteIdx+1] << (8 - fb)
			}
			h[i] = v & 63
			if h[i] > hllQ+1 {
				return nil, false
			}
		}
		return h, true
	case 1:
		idx := 0
		for len(body) > 0 {
			op := body[0]
			switch {
			case op&0xc0 == 0x00: // ZERO
				idx += int(op&0x3f) + 1
				body = body[1:]
			case op&0xc0 == 0x40: // XZERO
				if len(body) < 2 {
					return nil, false
				}
				idx += (int(op&0x3f)<<8 | int(body[1])) + 1
				body = body[2:]
			default: // VAL
				v, run := (op>>2)&0x1f+1, int(op&3)+1
				if idx+run > hllM || v > hllQ+1 {
					return nil, false
				}
				for ; run > 0; run-- {
					h[idx] = v
					idx++
				}
				body = body[1:]
			}
			if idx > hllM {
				return nil, false
			}
		}
		return h, idx == hllM
	}
	return nil, false
}
