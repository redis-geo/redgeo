package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// Version is the reported server version.
const Version = "0.1.0-redgeo"

// doHello implements the HELLO handshake (DESIGN §6.11, RESP3-aware). redcon
// speaks RESP2 on the wire, so the reply is a flat key/value array, which RESP2
// clients accept; we record the negotiated protocol for completeness.
func (h *handler) doHello(conn redcon.Conn, st *connState, args [][]byte) {
	if len(args) >= 2 {
		switch string(args[1]) {
		case "2", "3":
			st.resp, _ = strconv.Atoi(string(args[1]))
		default:
			conn.WriteError("NOPROTO unsupported protocol version")
			return
		}
	}
	proto := st.resp
	if proto == 0 {
		proto = 2
	}
	fields := [][2]string{
		{"server", "redgeo"},
		{"version", Version},
		{"proto", strconv.Itoa(proto)},
		{"id", h.store.ReplicaID()},
		{"mode", "standalone"},
		{"role", "master"},
	}
	// RESP3 returns a map; RESP2 a flat array (WriteMap handles both).
	w := newRespWriter(conn, proto)
	w.WriteMap(len(fields))
	for _, kv := range fields {
		conn.WriteBulkString(kv[0])
		conn.WriteBulkString(kv[1])
	}
}

// doInfo reports server, replication (CRDT), and keyspace stats as a bulk
// string in the standard Redis INFO format (DESIGN §6.11).
func (h *handler) doInfo(conn redcon.Conn) {
	ctx := context.Background()
	stats := h.store.EngineStats(ctx)

	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\nredgeo_version:%s\r\nreplica_id:%s\r\n", Version, h.store.ReplicaID())
	fmt.Fprintf(&b, "# Replication\r\nrole:master\r\ncrdt_heads:%d\r\ncrdt_max_height:%d\r\ncrdt_queued_jobs:%d\r\n",
		stats.Heads, stats.MaxHeight, stats.QueuedJobs)
	b.WriteString("# Keyspace\r\n")
	for db := 0; db < NumDBs; db++ {
		n, err := h.store.DBSize(ctx, db)
		if err == nil && n > 0 {
			fmt.Fprintf(&b, "db%d:keys=%d\r\n", db, n)
		}
	}
	conn.WriteBulkString(b.String())
}
