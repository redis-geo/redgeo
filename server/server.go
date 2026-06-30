// Package server wires the redgeo command layer to a redcon RESP server.
// It is a fork of redka's redsrv, adapted to the crdtstore backend and to
// redgeo's multi-DB connection state.
package server

import (
	"github.com/tidwall/redcon"

	"github.com/redis-geo/redgeo/crdtstore"
)

// Server is a RESP server backed by a crdtstore.Store.
type Server struct {
	network string
	addr    string
	srv     *redcon.Server
	store   *crdtstore.Store
}

// New creates a TCP RESP server listening on addr, backed by store.
func New(addr string, store *crdtstore.Store) *Server {
	h := newHandler(store)
	s := &Server{network: "tcp", addr: addr, store: store}
	s.srv = redcon.NewServerNetwork(
		s.network, addr,
		h.serve,
		func(conn redcon.Conn) bool { return true }, // accept
		func(conn redcon.Conn, err error) {},        // closed
	)
	return s
}

// Start begins serving. When ready is non-nil it receives the listen result.
func (s *Server) Start(ready chan error) error {
	return s.srv.ListenServeAndSignal(ready)
}

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.addr }

// Stop closes the server.
func (s *Server) Stop() error { return s.srv.Close() }
