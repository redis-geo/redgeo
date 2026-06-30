package server

import (
	"github.com/tidwall/redcon"

	redis "github.com/redis-geo/redgeo/redisapi"
)

// NumDBs is the number of logical databases (DESIGN §6.11: all 16).
const NumDBs = 16

// connState is per-connection state stored in conn.Context(). It tracks the
// selected database, and (from Phase 7) the MULTI transaction queue.
type connState struct {
	db      int
	inMulti bool
	cmds    []redis.Cmd
}

// getState returns the connection's state, lazily creating it.
func getState(conn redcon.Conn) *connState {
	if v, ok := conn.Context().(*connState); ok {
		return v
	}
	st := &connState{db: 0}
	conn.SetContext(st)
	return st
}

func (s *connState) push(cmd redis.Cmd) { s.cmds = append(s.cmds, cmd) }

func (s *connState) pop() redis.Cmd {
	if len(s.cmds) == 0 {
		return nil
	}
	cmd := s.cmds[len(s.cmds)-1]
	s.cmds = s.cmds[:len(s.cmds)-1]
	return cmd
}

func (s *connState) clear() { s.cmds = nil }
