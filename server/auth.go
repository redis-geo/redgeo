package server

import (
	"crypto/subtle"
	"crypto/tls"

	"github.com/tidwall/redcon"
)

// SetPassword requires clients to authenticate with AUTH (or HELLO … AUTH)
// before any other command. The only user is "default", as in a Redis without
// ACL users. Replication between nodes (libp2p) is separate and unaffected.
func (s *Server) SetPassword(pw string) { s.h.password = pw }

// SetTLS serves the RESP listener over TLS. Call before Start.
func (s *Server) SetTLS(cfg *tls.Config) { s.build(cfg) }

const (
	errNoAuth    = "NOAUTH Authentication required."
	errWrongPass = "WRONGPASS invalid username-password pair or user is disabled."
)

// checkPassword reports whether (user, pass) is the configured credential. It
// always compares both fields so timing does not reveal which one was wrong.
func (h *handler) checkPassword(user, pass string) bool {
	u := subtle.ConstantTimeCompare([]byte(user), []byte("default"))
	p := subtle.ConstantTimeCompare([]byte(pass), []byte(h.password))
	return u&p == 1
}

// doAuth implements AUTH [username] password.
func (h *handler) doAuth(conn redcon.Conn, st *connState, args [][]byte) {
	if len(args) < 2 || len(args) > 3 {
		conn.WriteError("ERR wrong number of arguments for 'auth' command")
		return
	}
	if h.password == "" {
		conn.WriteError("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
		return
	}
	user, pass := "default", string(args[1])
	if len(args) == 3 {
		user, pass = string(args[1]), string(args[2])
	}
	if !h.checkPassword(user, pass) {
		conn.WriteError(errWrongPass)
		return
	}
	st.authed = true
	conn.WriteString("OK")
}

// needsAuth reports whether the connection must authenticate before running
// name. AUTH and HELLO (which can carry credentials) and QUIT are always
// allowed through; HELLO enforces auth itself.
func (h *handler) needsAuth(st *connState, name string) bool {
	if h.password == "" || st.authed {
		return false
	}
	switch name {
	case "auth", "hello", "quit":
		return false
	}
	return true
}
