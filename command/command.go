// Package command implements Redis-compatible commands for operations on
// data structures. It is a fork of redka's command layer; the registry grows
// one phase at a time (DESIGN §9).
package command

import (
	"strings"

	"github.com/redis-geo/redgeo/command/conn"
	"github.com/redis-geo/redgeo/command/server"
	redis "github.com/redis-geo/redgeo/redisapi"
)

// Parse parses a raw RESP argument list into a Cmd.
func Parse(args [][]byte) (redis.Cmd, error) {
	name := strings.ToLower(string(args[0]))
	b := redis.NewBaseCmd(args)
	switch name {
	// connection
	case "echo":
		return conn.ParseEcho(b)
	case "ping":
		return conn.ParsePing(b)
	case "select":
		return conn.ParseSelect(b)

	// server (storage-orthogonal subset available in Phase 0)
	case "command":
		return server.ParseOK(b)
	case "info":
		return server.ParseOK(b)
	case "config":
		return server.ParseOK(b)
	case "lolwut":
		return server.ParseLolwut(b)

	default:
		return server.ParseUnknown(b)
	}
}
