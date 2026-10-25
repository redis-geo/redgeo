package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// Compactor runs cluster-wide compaction (package rotation implements it).
type Compactor interface {
	Compact(ctx context.Context, part int) error
	CompactAll(ctx context.Context) error
	Members() []string
	Forget(id string)
}

// SetCompactor enables the REDGEO.COMPACT / REDGEO.MEMBERS / REDGEO.FORGET
// administrative commands. Call before Start.
func (s *Server) SetCompactor(c Compactor) { s.h.compactor = c }

// doRedgeo handles the REDGEO.* admin commands (they need no store access).
//
//	REDGEO.COMPACT [partition|ALL]   rotate partitions to purge tombstones, cluster-wide
//	REDGEO.MEMBERS                   replicas whose agreement a compaction needs
//	REDGEO.FORGET replica-id         permanently retire a replica from that set
func (h *handler) doRedgeo(conn redcon.Conn, name string, args [][]byte) {
	if h.compactor == nil {
		conn.WriteError("ERR compaction is not enabled on this node")
		return
	}
	switch name {
	case "redgeo.compact":
		if len(args) > 2 {
			conn.WriteError("ERR wrong number of arguments (redgeo.compact)")
			return
		}
		var err error
		if len(args) == 2 && !strings.EqualFold(string(args[1]), "all") {
			part, perr := strconv.Atoi(string(args[1]))
			if perr != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			err = h.compactor.Compact(context.Background(), part)
		} else {
			err = h.compactor.CompactAll(context.Background())
		}
		if err != nil {
			conn.WriteError("ERR compaction failed: " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "redgeo.members":
		ms := h.compactor.Members()
		conn.WriteArray(len(ms))
		for _, m := range ms {
			conn.WriteBulkString(m)
		}
	case "redgeo.forget":
		if len(args) != 2 {
			conn.WriteError("ERR wrong number of arguments (redgeo.forget)")
			return
		}
		h.compactor.Forget(string(args[1]))
		conn.WriteString("OK")
	}
}
