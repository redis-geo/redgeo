// Package server wires the redgeo command layer to a redcon RESP server.
// It is a fork of redka's redsrv, adapted to the crdtstore backend and to
// redgeo's multi-DB connection state.
package server

import (
	"context"
	"crypto/tls"

	"github.com/tidwall/redcon"

	"github.com/redis-geo/redgeo/crdtstore"
)

// listener is the part of redcon's plain and TLS servers Server uses.
type listener interface {
	ListenServeAndSignal(signal chan error) error
	Close() error
}

// Server is a RESP server backed by a crdtstore.Store.
type Server struct {
	network string
	addr    string
	srv     listener
	store   *crdtstore.Store
	h       *handler
	cancel  context.CancelFunc // stops the pub/sub bus loop
}

// New creates a TCP RESP server listening on addr, backed by store.
func New(addr string, store *crdtstore.Store) *Server {
	s := &Server{network: "tcp", addr: addr, store: store, h: newHandler(store)}
	s.build(nil)
	return s
}

// build (re)creates the listener, optionally over TLS.
func (s *Server) build(tlsCfg *tls.Config) {
	accept := func(conn redcon.Conn) bool {
		s.h.metrics.connsTotal.Add(1)
		s.h.metrics.connsActive.Add(1)
		return true
	}
	closed := func(conn redcon.Conn, err error) { s.h.metrics.connsActive.Add(-1) }
	if tlsCfg != nil {
		s.srv = redcon.NewServerNetworkTLS(s.network, s.addr, s.h.serve, accept, closed, tlsCfg)
		return
	}
	s.srv = redcon.NewServerNetwork(s.network, s.addr, s.h.serve, accept, closed)
}

// Start begins serving. When ready is non-nil it receives the listen result.
func (s *Server) Start(ready chan error) error {
	if s.h.bus != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		go s.h.runBus(ctx)
	}
	return s.srv.ListenServeAndSignal(ready)
}

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.addr }

// Stop closes the server.
func (s *Server) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return s.srv.Close()
}
