package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxCmdLabels bounds metric label cardinality: a client sending random command
// names must not grow memory without limit.
const maxCmdLabels = 512

type cmdStat struct {
	calls  atomic.Uint64
	errors atomic.Uint64
	nanos  atomic.Uint64
}

type metrics struct {
	mu          sync.Mutex
	cmds        map[string]*cmdStat
	connsActive atomic.Int64
	connsTotal  atomic.Uint64
	started     time.Time
}

func newMetrics() *metrics { return &metrics{cmds: map[string]*cmdStat{}, started: time.Now()} }

func (m *metrics) stat(name string) *cmdStat {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.cmds[name]; ok {
		return s
	}
	if len(m.cmds) >= maxCmdLabels || len(name) > 48 {
		name = "other"
		if s, ok := m.cmds[name]; ok {
			return s
		}
	}
	s := &cmdStat{}
	m.cmds[name] = s
	return s
}

func (m *metrics) record(name string, d time.Duration, failed bool) {
	s := m.stat(name)
	s.calls.Add(1)
	s.nanos.Add(uint64(d))
	if failed {
		s.errors.Add(1)
	}
}

type cmdSnapshot struct {
	name   string
	calls  uint64
	errors uint64
	nanos  uint64
}

func (m *metrics) snapshot() (out []cmdSnapshot, total uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for n, s := range m.cmds {
		c := s.calls.Load()
		out = append(out, cmdSnapshot{n, c, s.errors.Load(), s.nanos.Load()})
		total += c
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, total
}

// MetricsHandler serves Prometheus text-format metrics: command counts, errors
// and latency, connections, and CRDT replication state (heads, DAG height,
// per-partition generation). It reads cheap counters only — no keyspace scans.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var b strings.Builder
		m := s.h.metrics
		stats := s.store.EngineStats(r.Context())
		gauge := func(name, help string, v any) {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, v)
		}
		fmt.Fprintf(&b, "# HELP redgeo_info Static information about this node.\n# TYPE redgeo_info gauge\nredgeo_info{version=%q,replica=%q} 1\n", Version, s.store.ReplicaID())
		gauge("redgeo_uptime_seconds", "Seconds since the server started.", int64(time.Since(m.started).Seconds()))
		gauge("redgeo_connections_active", "Currently open client connections.", m.connsActive.Load())
		fmt.Fprintf(&b, "# HELP redgeo_connections_total Client connections accepted.\n# TYPE redgeo_connections_total counter\nredgeo_connections_total %d\n", m.connsTotal.Load())
		gauge("redgeo_crdt_heads", "Current CRDT DAG heads (all partitions).", stats.Heads)
		gauge("redgeo_crdt_max_height", "Highest CRDT DAG height.", stats.MaxHeight)
		gauge("redgeo_crdt_queued_jobs", "CRDT delta-processing jobs queued.", stats.QueuedJobs)
		b.WriteString("# HELP redgeo_partition_generation Compaction generation of each partition DAG.\n# TYPE redgeo_partition_generation gauge\n")
		for p, g := range s.store.Generations() {
			fmt.Fprintf(&b, "redgeo_partition_generation{partition=\"%d\"} %d\n", p, g)
		}
		snap, _ := m.snapshot()
		b.WriteString("# HELP redgeo_commands_total Commands processed.\n# TYPE redgeo_commands_total counter\n")
		for _, c := range snap {
			fmt.Fprintf(&b, "redgeo_commands_total{cmd=%q} %d\n", c.name, c.calls)
		}
		b.WriteString("# HELP redgeo_command_errors_total Commands that failed.\n# TYPE redgeo_command_errors_total counter\n")
		for _, c := range snap {
			if c.errors > 0 {
				fmt.Fprintf(&b, "redgeo_command_errors_total{cmd=%q} %d\n", c.name, c.errors)
			}
		}
		b.WriteString("# HELP redgeo_command_duration_seconds_sum Total time spent in each command.\n# TYPE redgeo_command_duration_seconds_sum counter\n")
		for _, c := range snap {
			fmt.Fprintf(&b, "redgeo_command_duration_seconds_sum{cmd=%q} %g\n", c.name, float64(c.nanos)/1e9)
		}
		_, _ = w.Write([]byte(b.String()))
	})
}
