package server

import (
	"math"
	"strconv"

	"github.com/tidwall/redcon"
)

// respWriter adapts a redcon connection (which speaks RESP2) to the redgeo
// redis.Writer interface, emitting RESP3 types when the connection negotiated
// protocol 3 via HELLO (DESIGN §6.11). It embeds redcon.Conn so all the
// RESP2-identical writers (bulk, int, array, error, simple string) pass through
// unchanged; only the type-distinct replies (null, map, double, boolean) differ
// between RESP2 and RESP3.
type respWriter struct {
	redcon.Conn
	proto int
}

func newRespWriter(conn redcon.Conn, proto int) respWriter {
	if proto == 0 {
		proto = 2
	}
	return respWriter{Conn: conn, proto: proto}
}

// WriteNull: RESP3 uses the null type `_`; RESP2 uses a null bulk string.
func (w respWriter) WriteNull() {
	if w.proto >= 3 {
		w.Conn.WriteRaw([]byte("_\r\n"))
		return
	}
	w.Conn.WriteNull()
}

// WriteMap: RESP3 map header `%N`; RESP2 a flat array of 2N elements. The caller
// writes N key/value pairs either way.
func (w respWriter) WriteMap(numPairs int) {
	if w.proto >= 3 {
		w.Conn.WriteRaw([]byte("%" + strconv.Itoa(numPairs) + "\r\n"))
		return
	}
	w.Conn.WriteArray(numPairs * 2)
}

// WriteDouble: RESP3 double `,x`; RESP2 a bulk string.
func (w respWriter) WriteDouble(f float64) {
	if w.proto >= 3 {
		w.Conn.WriteRaw([]byte("," + formatDouble(f) + "\r\n"))
		return
	}
	w.Conn.WriteBulkString(formatDouble(f))
}

// WriteBool: RESP3 boolean `#t`/`#f`; RESP2 integer 1/0.
func (w respWriter) WriteBool(b bool) {
	if w.proto >= 3 {
		if b {
			w.Conn.WriteRaw([]byte("#t\r\n"))
		} else {
			w.Conn.WriteRaw([]byte("#f\r\n"))
		}
		return
	}
	if b {
		w.Conn.WriteInt(1)
	} else {
		w.Conn.WriteInt(0)
	}
}

// formatDouble renders a float the way Redis does, with RESP3 inf/-inf forms.
func formatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}
