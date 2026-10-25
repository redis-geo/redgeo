package server

import (
	"context"
	"encoding/binary"
	"errors"
	"time"
)

// Bus carries PUBLISH messages between replicas (DESIGN §6.12, §12.5). It is
// deliberately the same shape as go-ds-crdt's Broadcaster, so a gossipsub topic
// or the in-memory test network can back it. Delivery is at-most-once and
// unordered across publishers, matching Redis pub/sub; nothing is stored.
type Bus interface {
	Broadcast(ctx context.Context, data []byte) error
	Next(ctx context.Context) ([]byte, error)
}

// SetBus makes PUBLISH geo-replicated: every PUBLISH is also broadcast on bus,
// and messages from other replicas are delivered to this node's local
// subscribers. origin is this replica's ID, used to drop our own messages
// (gossipsub echoes them back). Call before Start.
func (s *Server) SetBus(bus Bus, origin string) {
	s.h.bus, s.h.origin = bus, origin
}

// busFrame encodes (origin, channel, message) as three uvarint-prefixed fields.
func busFrame(origin, channel, message string) []byte {
	out := make([]byte, 0, 3*binary.MaxVarintLen64+len(origin)+len(channel)+len(message))
	for _, f := range []string{origin, channel, message} {
		out = binary.AppendUvarint(out, uint64(len(f)))
		out = append(out, f...)
	}
	return out
}

var errBusFrame = errors.New("malformed pub/sub frame")

func parseBusFrame(b []byte) (origin, channel, message string, err error) {
	var f [3]string
	for i := range f {
		n, w := binary.Uvarint(b)
		if w <= 0 || uint64(len(b)-w) < n {
			return "", "", "", errBusFrame
		}
		f[i], b = string(b[w:w+int(n)]), b[w+int(n):]
	}
	return f[0], f[1], f[2], nil
}

// publishRemote broadcasts a locally published message to the other replicas.
func (h *handler) publishRemote(channel, message string) {
	if h.bus == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = h.bus.Broadcast(ctx, busFrame(h.origin, channel, message)) // best effort
}

// runBus delivers remote messages to local subscribers until ctx is done.
func (h *handler) runBus(ctx context.Context) {
	for {
		data, err := h.bus.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(50 * time.Millisecond) // transient; don't spin
			continue
		}
		origin, channel, message, err := parseBusFrame(data)
		if err != nil || origin == h.origin {
			continue
		}
		h.ps.Publish(channel, message)
	}
}
