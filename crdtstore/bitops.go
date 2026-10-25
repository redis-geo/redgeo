package crdtstore

import "math/bits"

// Pure bit-string helpers shared by BITCOUNT/BITPOS/BITOP. Bit 0 is the most
// significant bit of byte 0 (Redis order).

// bitRange normalizes a Redis [start,end] range (negative = from the end,
// bounds clamped) over a string of n bytes, in bytes or bits. It returns an
// inclusive range and ok=false when the range is empty.
func bitRange(n int64, start, end int64, hasStart, hasEnd, inBits bool) (s, e int64, ok bool) {
	total := n
	if inBits {
		total = n * 8
	}
	if total == 0 {
		return 0, -1, false
	}
	s, e = 0, total-1
	if hasStart {
		s = start
	}
	if hasEnd {
		e = end
	}
	if s < 0 {
		s += total
	}
	if e < 0 {
		e += total
	}
	if s < 0 {
		s = 0
	}
	if e < 0 {
		e = 0
	}
	if e >= total {
		e = total - 1
	}
	return s, e, s <= e
}

// bitCountRange counts set bits in the inclusive range [s,e] (bytes or bits).
func bitCountRange(data []byte, s, e int64, inBits bool) int64 {
	if !inBits {
		var c int64
		for _, b := range data[s : e+1] {
			c += int64(bits.OnesCount8(b))
		}
		return c
	}
	sb, eb := s>>3, e>>3
	var c int64
	for i := sb; i <= eb; i++ {
		b := data[i]
		if i == sb {
			b &= 0xff >> uint(s&7)
		}
		if i == eb {
			b &= 0xff << uint(7-(e&7))
		}
		c += int64(bits.OnesCount8(b))
	}
	return c
}

// bitPosRange returns the offset of the first bit equal to bit within the
// inclusive bit range [sb,eb], or -1.
func bitPosRange(data []byte, bit bool, sb, eb int64) int64 {
	skip := byte(0x00) // a byte that cannot contain the wanted bit
	if !bit {
		skip = 0xff
	}
	for pos := sb; pos <= eb; {
		if pos&7 == 0 && pos+7 <= eb && data[pos>>3] == skip {
			pos += 8
			continue
		}
		if (data[pos>>3]&(0x80>>uint(pos&7)) != 0) == bit {
			return pos
		}
		pos++
	}
	return -1
}

// bitop applies AND/OR/XOR/NOT; shorter operands are zero-padded.
func bitop(op string, srcs [][]byte) []byte {
	n := 0
	for _, s := range srcs {
		if len(s) > n {
			n = len(s)
		}
	}
	out := make([]byte, n)
	if op == "not" {
		for i := range out {
			var b byte
			if i < len(srcs[0]) {
				b = srcs[0][i]
			}
			out[i] = ^b
		}
		return out
	}
	for i := range out {
		var acc byte
		for j, s := range srcs {
			var b byte
			if i < len(s) {
				b = s[i]
			}
			switch {
			case j == 0:
				acc = b
			case op == "and":
				acc &= b
			case op == "or":
				acc |= b
			case op == "xor":
				acc ^= b
			}
		}
		out[i] = acc
	}
	return out
}
