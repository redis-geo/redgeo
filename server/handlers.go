package server

import (
	"strconv"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/redis-geo/redgeo/command"
	"github.com/redis-geo/redgeo/crdtstore"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// handler wires the command layer to a crdtstore backend over redcon.
type handler struct {
	store *crdtstore.Store
}

func newHandler(store *crdtstore.Store) *handler { return &handler{store: store} }

// serve is the redcon command callback (one call per command per connection).
func (h *handler) serve(conn redcon.Conn, rcmd redcon.Command) {
	if len(rcmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	st := getState(conn)
	name := strings.ToLower(string(rcmd.Args[0]))

	// SELECT mutates connection state, which the command layer can't reach, so
	// it is applied here (DESIGN §6.11).
	if name == "select" {
		h.doSelect(conn, st, rcmd.Args)
		return
	}

	pcmd, err := command.Parse(rcmd.Args)
	if err != nil {
		conn.WriteError(pcmd.Error(err))
		return
	}

	red := h.store.Redka(st.db)
	_, _ = pcmd.Run(conn, red)
}

// doSelect validates and applies a SELECT.
func (h *handler) doSelect(conn redcon.Conn, st *connState, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError(redis.ErrInvalidArgNum.Error() + " (select)")
		return
	}
	idx, err := strconv.Atoi(string(args[1]))
	if err != nil {
		conn.WriteError(redis.ErrInvalidInt.Error())
		return
	}
	if idx < 0 || idx >= NumDBs {
		conn.WriteError("ERR DB index is out of range")
		return
	}
	st.db = idx
	conn.WriteString("OK")
}
