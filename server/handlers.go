package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/redis-geo/redgeo/command"
	"github.com/redis-geo/redgeo/crdtstore"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// handler wires the command layer to a crdtstore backend over redcon, and
// implements the storage-orthogonal middleware (SELECT, MULTI/EXEC/DISCARD,
// pub/sub) that needs access to the connection rather than the store.
type handler struct {
	store *crdtstore.Store
	ps    *redcon.PubSub
}

func newHandler(store *crdtstore.Store) *handler {
	return &handler{store: store, ps: &redcon.PubSub{}}
}

// serve is the redcon command callback (one call per command per connection).
func (h *handler) serve(conn redcon.Conn, rcmd redcon.Command) {
	if len(rcmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	st := getState(conn)
	name := strings.ToLower(string(rcmd.Args[0]))

	switch name {
	case "select":
		h.doSelect(conn, st, rcmd.Args)
		return
	case "multi":
		h.doMulti(conn, st)
		return
	case "exec":
		h.doExec(conn, st)
		return
	case "discard":
		h.doDiscard(conn, st)
		return
	case "subscribe", "psubscribe":
		h.doSubscribe(conn, name, rcmd.Args)
		return
	case "publish":
		h.doPublish(conn, rcmd.Args)
		return
	case "hello":
		h.doHello(conn, st, rcmd.Args)
		return
	case "info":
		h.doInfo(conn)
		return
	}

	pcmd, err := command.Parse(rcmd.Args)
	if err != nil {
		// A parse error inside MULTI aborts the whole transaction (Redis).
		if st.inMulti {
			st.aborted = true
		}
		conn.WriteError(pcmd.Error(err))
		return
	}

	if st.inMulti {
		st.push(pcmd)
		conn.WriteString("QUEUED")
		return
	}

	// The redcon connection natively encodes RESP3 types when its protocol was
	// set to 3 at HELLO (conn.SetProto), so it is passed directly as the Writer.
	red := h.store.Redka(st.db)
	_, _ = pcmd.Run(conn, red)
}

// ---- MULTI / EXEC / DISCARD (DESIGN §6.10) ----

func (h *handler) doMulti(conn redcon.Conn, st *connState) {
	if st.inMulti {
		conn.WriteError(redis.ErrNestedMulti.Error())
		return
	}
	st.inMulti = true
	st.aborted = false
	st.clear()
	conn.WriteString("OK")
}

func (h *handler) doExec(conn redcon.Conn, st *connState) {
	if !st.inMulti {
		conn.WriteError(redis.ErrNotInMulti.Error())
		return
	}
	queued := st.cmds
	aborted := st.aborted
	st.inMulti = false
	st.aborted = false
	st.clear()

	if aborted {
		conn.WriteError("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	// Run queued commands against a transaction-bound store: their writes
	// accumulate into one CRDT Batch (committed as a single atomic delta) and a
	// read-your-writes overlay so later commands see earlier ones (§6.10). No
	// isolation/rollback — Redis-compatible.
	stx, commit, err := h.store.BeginTxn(context.Background())
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	red := stx.Redka(st.db)
	conn.WriteArray(len(queued))
	for _, pcmd := range queued {
		_, _ = pcmd.Run(conn, red)
	}
	if err := commit(); err != nil {
		// Replies were already written; log path is the caller's. Best-effort.
		_ = err
	}
}

func (h *handler) doDiscard(conn redcon.Conn, st *connState) {
	if !st.inMulti {
		conn.WriteError(redis.ErrNotInMulti.Error())
		return
	}
	st.inMulti = false
	st.aborted = false
	st.clear()
	conn.WriteString("OK")
}

// ---- pub/sub (local; DESIGN §6.12) ----

// doSubscribe registers the connection on the given channels. redcon's PubSub
// detaches the connection and runs its own loop afterward (handling further
// (UN)SUBSCRIBE/PING), so this handler won't be called again for that conn.
func (h *handler) doSubscribe(conn redcon.Conn, name string, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError(redis.ErrInvalidArgNum.Error() + " (" + name + ")")
		return
	}
	for _, ch := range args[1:] {
		if name == "psubscribe" {
			h.ps.Psubscribe(conn, string(ch))
		} else {
			h.ps.Subscribe(conn, string(ch))
		}
	}
}

func (h *handler) doPublish(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError(redis.ErrInvalidArgNum.Error() + " (publish)")
		return
	}
	n := h.ps.Publish(string(args[1]), string(args[2]))
	conn.WriteInt(n)
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
