// Package rotation coordinates cluster-wide compaction (DESIGN §5.5, §12.2).
//
// go-ds-crdt never garbage-collects; the only reclamation is rotating a
// partition to a fresh generation seeded with just the live state. In a cluster
// that is safe only if, at the instant of the cut, every replica holds
// identical state and nobody mints new deltas — otherwise a lagging replica's
// still-unseen adds would resurrect deleted keys against the purged tombstones.
// This package establishes that with a small commit protocol over a control
// channel (a gossipsub topic, or the in-memory network in tests):
//
//	coordinator                          every other known member
//	  freeze local writes
//	  prepare ───────────────────────────▶ freeze writes (drain in-flight)
//	          ◀───────────────────────────  prepared{heads digest}
//	  repeat prepare until all digests equal ours (state is identical), else abort
//	  stage+activate generation g+1 seeded with the live state (genesis G)
//	  switch{G} ─────────────────────────▶ stage+activate empty g+1, wait for G
//	          ◀───────────────────────────  ready
//	  done ──────────────────────────────▶ purge g, unfreeze
//
// Any failure before done rolls every replica back to generation g (which is
// kept until done), so a failed compaction loses nothing. A replica that has
// activated g+1 never decides alone: if it misses done/abort (crash, lost
// message) it asks its peers and follows the majority outcome, rolling back only
// when every member reports being in the same doubt.
//
// Membership is every replica ever seen (hello messages and replica IDs found in
// data), persisted across restarts. Compaction requires ALL of them, so an
// absent replica blocks it rather than being left to resurrect old state; use
// Forget to retire one permanently.
package rotation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis-geo/redgeo/engine"
)

// Transport is the control channel. It is the same shape as go-ds-crdt's
// Broadcaster; a nil Transport means a single node.
type Transport interface {
	Broadcast(ctx context.Context, data []byte) error
	Next(ctx context.Context) ([]byte, error)
}

// Config configures a Node. Zero durations take the defaults noted.
type Config struct {
	Engine    *engine.Engine
	ID        string    // this replica
	Transport Transport // nil = no peers
	// Observe lists replica IDs found in the data (optional); each becomes a
	// required member.
	Observe func(ctx context.Context) ([]string, error)
	// Unregister removes a retired replica from the shared registry so no node
	// re-learns it (optional).
	Unregister func(ctx context.Context, id string) error

	PrepareTimeout time.Duration // coordinator: wait for convergence (30s)
	SwitchTimeout  time.Duration // coordinator: wait for all ready (60s)
	IdleTimeout    time.Duration // participant: abandon an un-switched prepare (90s)
	HelloEvery     time.Duration // membership heartbeat (2s)
	PollEvery      time.Duration // how long a coordinator round waits for replies (1s)
}

func (c *Config) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.PrepareTimeout, 30*time.Second)
	set(&c.SwitchTimeout, 60*time.Second)
	set(&c.IdleTimeout, 3*time.Minute)
	set(&c.HelloEvery, 2*time.Second)
	set(&c.PollEvery, time.Second)
}

type msg struct {
	T       string   `json:"t"` // hello prepare prepared switch ready done abort status outcome
	From    string   `json:"from"`
	Round   string   `json:"round,omitempty"`
	Part    int      `json:"part"`
	Gen     int      `json:"gen,omitempty"`
	Heads   []string `json:"heads,omitempty"`   // prepared: this replica's DAG heads
	Union   []string `json:"union,omitempty"`   // prepare: every head seen so far
	Missing []string `json:"missing,omitempty"` // prepared: blocks of Union not yet processed here
	Busy    bool     `json:"busy,omitempty"`    // prepared: still draining its processing queue
	Genesis string   `json:"genesis,omitempty"`
	OK      bool     `json:"ok,omitempty"`
	Reason  string   `json:"reason,omitempty"`
	State   string   `json:"state,omitempty"` // outcome: idle | indoubt
}

// Participant phases.
const (
	phasePrepared = iota + 1
	phaseSwitched
)

type pstate struct {
	round   string
	coord   string
	phase   int
	genesis string
	since   time.Time
	seeded  bool
	mine    bool // this node coordinates the round (exempt from the idle watchdog)
}

// Node is one replica's side of the protocol: it answers other coordinators and
// can itself coordinate a compaction.
type Node struct {
	cfg Config
	eng *engine.Engine

	mu      sync.Mutex
	members map[string]struct{}
	parts   map[int]*pstate     // partitions with a rotation in progress on this node
	waiters map[string]chan msg // coordinator rounds awaiting replies
	pmu     []sync.Mutex        // serialises handlers per partition

	statusReplies map[int]map[string]msg // in-doubt resolution: part → peer → reply
}

// ErrNotConverged and ErrMemberUnavailable explain why a compaction was refused.
var (
	ErrNotConverged      = errors.New("rotation: replicas have not converged")
	ErrMemberUnavailable = errors.New("rotation: a known member did not respond")
	ErrBusy              = errors.New("rotation: a rotation is already in progress for this partition")
)

func New(cfg Config) *Node {
	cfg.defaults()
	n := &Node{
		cfg:           cfg,
		eng:           cfg.Engine,
		members:       map[string]struct{}{},
		parts:         map[int]*pstate{},
		waiters:       map[string]chan msg{},
		pmu:           make([]sync.Mutex, cfg.Engine.NumPartitions()),
		statusReplies: map[int]map[string]msg{},
	}
	n.loadMembers(context.Background())
	return n
}

// Start runs the protocol loops until ctx ends.
func (n *Node) Start(ctx context.Context) {
	// A partition restored mid-rotation (crash between activate and done) waits
	// for its peers to settle the outcome.
	n.mu.Lock()
	for i := 0; i < n.eng.NumPartitions(); i++ {
		if n.eng.InDoubt(i) {
			n.parts[i] = &pstate{phase: phaseSwitched, since: time.Now(), seeded: true}
		}
	}
	n.mu.Unlock()
	if n.cfg.Transport == nil {
		return
	}
	go n.recvLoop(ctx)
	go n.tick(ctx)
}

// ---- membership ----

const membersKey = "rotation-members"

func (n *Node) loadMembers(ctx context.Context) {
	b, err := n.eng.MetaGet(ctx, membersKey)
	if err != nil {
		return
	}
	var ids []string
	if json.Unmarshal(b, &ids) == nil {
		for _, id := range ids {
			if id != n.cfg.ID {
				n.members[id] = struct{}{}
			}
		}
	}
}

func (n *Node) saveMembersLocked() {
	ids := make([]string, 0, len(n.members))
	for id := range n.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	b, _ := json.Marshal(ids)
	_ = n.eng.MetaPut(context.Background(), membersKey, b)
}

func (n *Node) learn(ids ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	changed := false
	for _, id := range ids {
		if id == "" || id == n.cfg.ID {
			continue
		}
		if _, ok := n.members[id]; !ok {
			n.members[id] = struct{}{}
			changed = true
		}
	}
	if changed {
		n.saveMembersLocked()
	}
}

// Members lists the other replicas whose agreement a compaction needs.
func (n *Node) Members() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.members))
	for id := range n.members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Forget permanently drops a replica from the required set (e.g. decommissioned).
// It must never be used for a replica that may still hold un-synced writes.
func (n *Node) Forget(id string) {
	if n.cfg.Unregister != nil {
		_ = n.cfg.Unregister(context.Background(), id)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.members, id)
	n.saveMembersLocked()
}

// ---- transport ----

func (n *Node) send(ctx context.Context, m msg) {
	if n.cfg.Transport == nil {
		return
	}
	m.From = n.cfg.ID
	b, _ := json.Marshal(m)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = n.cfg.Transport.Broadcast(c, b)
}

func (n *Node) recvLoop(ctx context.Context) {
	for {
		data, err := n.cfg.Transport.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var m msg
		if json.Unmarshal(data, &m) != nil || m.From == n.cfg.ID || m.From == "" {
			continue
		}
		n.learn(m.From)
		go n.handle(ctx, m)
	}
}

func (n *Node) tick(ctx context.Context) {
	hello := time.NewTicker(n.cfg.HelloEvery)
	defer hello.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-hello.C:
			n.send(ctx, msg{T: "hello"})
			n.watchdog(ctx)
		}
	}
}

func (n *Node) handle(ctx context.Context, m msg) {
	switch m.T {
	case "prepared", "ready":
		n.mu.Lock()
		ch := n.waiters[m.Round]
		n.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
		return
	case "outcome":
		n.onOutcome(ctx, m)
		return
	case "hello":
		return
	}
	if m.Part < 0 || m.Part >= len(n.pmu) {
		if m.T == "prepare" {
			n.send(ctx, msg{T: "prepared", Round: m.Round, Part: m.Part, Reason: "no such partition"})
		}
		return
	}
	n.pmu[m.Part].Lock()
	defer n.pmu[m.Part].Unlock()
	switch m.T {
	case "prepare":
		n.onPrepare(ctx, m)
	case "switch":
		n.onSwitch(ctx, m)
	case "done":
		n.finish(ctx, m.Part, m.Round, true)
	case "abort":
		n.finish(ctx, m.Part, m.Round, false)
	case "status":
		n.onStatus(ctx, m)
	}
}

// ---- participant ----

func (n *Node) onPrepare(ctx context.Context, m msg) {
	reply := msg{T: "prepared", Round: m.Round, Part: m.Part}
	n.mu.Lock()
	st := n.parts[m.Part]
	n.mu.Unlock()
	switch {
	case st != nil && st.round == m.Round && st.phase == phasePrepared:
		// Retransmit from the coordinator: answer with the current digest.
	case st != nil || n.eng.InDoubt(m.Part):
		reply.Reason = "busy"
		n.send(ctx, reply)
		return
	default:
		fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := n.eng.Freeze(fctx, m.Part)
		cancel()
		if err != nil {
			n.eng.Unfreeze(m.Part)
			reply.Reason = "could not freeze: " + err.Error()
			n.send(ctx, reply)
			return
		}
		n.mu.Lock()
		n.parts[m.Part] = &pstate{round: m.Round, coord: m.From, phase: phasePrepared, since: time.Now()}
		n.mu.Unlock()
	}
	reply.OK = true
	reply.Heads = n.eng.Heads(ctx, m.Part)
	reply.Missing = n.eng.Missing(ctx, m.Part, m.Union)
	reply.Busy = !n.eng.Quiescent(ctx, m.Part)
	n.send(ctx, reply)
}

func (n *Node) onSwitch(ctx context.Context, m msg) {
	n.mu.Lock()
	st := n.parts[m.Part]
	n.mu.Unlock()
	reply := msg{T: "ready", Round: m.Round, Part: m.Part}
	if st == nil || st.round != m.Round {
		reply.Reason = "not prepared"
		n.send(ctx, reply)
		return
	}
	if st.phase == phasePrepared {
		if _, _, err := n.eng.Stage(ctx, m.Part, false); err != nil {
			reply.Reason = err.Error()
			n.send(ctx, reply)
			return
		}
		if err := n.eng.Activate(ctx, m.Part, true); err != nil {
			_ = n.eng.Rollback(ctx, m.Part)
			reply.Reason = err.Error()
			n.send(ctx, reply)
			return
		}
		n.mu.Lock()
		st.phase, st.genesis, st.since = phaseSwitched, m.Genesis, time.Now()
		n.mu.Unlock()
	}
	// Wait for the coordinator's seed to replicate here, off the handler path.
	go func() {
		err := n.eng.WaitSeed(ctx, m.Part, st.genesis, n.cfg.SwitchTimeout)
		if err != nil {
			reply.Reason = err.Error()
		} else {
			reply.OK = true
			n.mu.Lock()
			st.seeded = true
			n.mu.Unlock()
		}
		n.send(ctx, reply)
	}()
}

// finish applies a round's outcome on this participant.
func (n *Node) finish(ctx context.Context, part int, round string, commit bool) {
	n.mu.Lock()
	st := n.parts[part]
	if st == nil || (round != "" && st.round != round) {
		n.mu.Unlock()
		return
	}
	delete(n.parts, part)
	n.mu.Unlock()
	if commit && st.phase == phaseSwitched {
		_ = n.eng.Commit(ctx, part)
	} else {
		_ = n.eng.Rollback(ctx, part)
	}
	n.eng.Unfreeze(part)
}

// watchdog abandons prepares whose coordinator went silent and keeps asking
// peers about rotations this node activated but never saw finish.
func (n *Node) watchdog(ctx context.Context) {
	type item struct {
		part int
		st   *pstate
	}
	var stale, doubt []item
	n.mu.Lock()
	for p, st := range n.parts {
		switch {
		case st.mine:
			// the coordinator manages its own timeouts
		case st.phase == phasePrepared && time.Since(st.since) > n.cfg.IdleTimeout:
			stale = append(stale, item{p, st})
		case st.phase == phaseSwitched && time.Since(st.since) > 2*n.cfg.HelloEvery:
			doubt = append(doubt, item{p, st})
		}
	}
	n.mu.Unlock()
	for _, it := range stale {
		n.pmu[it.part].Lock()
		n.finish(ctx, it.part, it.st.round, false)
		n.pmu[it.part].Unlock()
	}
	for _, it := range doubt {
		n.mu.Lock()
		n.statusReplies[it.part] = map[string]msg{}
		n.mu.Unlock()
		n.send(ctx, msg{T: "status", Part: it.part, Gen: n.eng.Generation(it.part)})
	}
}

// onStatus answers a peer who is unsure how a rotation ended.
func (n *Node) onStatus(ctx context.Context, m msg) {
	n.mu.Lock()
	active := n.parts[m.Part] != nil
	n.mu.Unlock()
	state := "idle"
	if active || n.eng.InDoubt(m.Part) {
		state = "indoubt"
	}
	n.send(ctx, msg{T: "outcome", Part: m.Part, Gen: n.eng.Generation(m.Part), State: state})
}

// onOutcome resolves an in-doubt partition from a peer's answer: a peer past our
// generation committed; a settled peer behind it rolled back; if every member
// is itself in doubt, no one committed and it is safe to roll back.
func (n *Node) onOutcome(ctx context.Context, m msg) {
	if m.Part < 0 || m.Part >= len(n.pmu) {
		return
	}
	n.pmu[m.Part].Lock()
	defer n.pmu[m.Part].Unlock()
	n.mu.Lock()
	st := n.parts[m.Part]
	if st == nil || st.phase != phaseSwitched {
		n.mu.Unlock()
		return
	}
	mine := n.eng.Generation(m.Part)
	replies := n.statusReplies[m.Part]
	if replies != nil {
		replies[m.From] = m
	}
	allInDoubt := len(n.members) > 0
	for id := range n.members {
		r, ok := replies[id]
		if !ok || r.State != "indoubt" || r.Gen != mine {
			allInDoubt = false
		}
	}
	n.mu.Unlock()
	switch {
	case m.State == "idle" && m.Gen >= mine:
		n.finish(ctx, m.Part, "", true)
	case m.State == "idle" && m.Gen < mine:
		n.finish(ctx, m.Part, "", false)
	case allInDoubt:
		n.finish(ctx, m.Part, "", false)
	}
}

// ---- coordinator ----

// Compact rotates one partition across the whole cluster.
func (n *Node) Compact(ctx context.Context, part int) (err error) {
	if part < 0 || part >= len(n.pmu) {
		return fmt.Errorf("rotation: no partition %d", part)
	}
	n.pmu[part].Lock()
	defer n.pmu[part].Unlock()

	if n.cfg.Observe != nil {
		if ids, oerr := n.cfg.Observe(ctx); oerr == nil {
			n.learn(ids...)
		}
	}
	round := fmt.Sprintf("%s/%d/%d", n.cfg.ID, part, time.Now().UnixNano())
	n.mu.Lock()
	if n.parts[part] != nil || n.eng.InDoubt(part) {
		n.mu.Unlock()
		return ErrBusy
	}
	n.parts[part] = &pstate{round: round, coord: n.cfg.ID, phase: phasePrepared, since: time.Now(), mine: true}
	ch := make(chan msg, 1024)
	n.waiters[round] = ch
	n.mu.Unlock()

	members := n.Members()
	activated := false
	defer func() {
		n.mu.Lock()
		delete(n.waiters, round)
		delete(n.parts, part)
		n.mu.Unlock()
		if err != nil {
			for i := 0; i < 3; i++ { // best effort; participants also time out
				n.send(ctx, msg{T: "abort", Round: round, Part: part})
			}
			if activated {
				_ = n.eng.Rollback(ctx, part)
			}
		}
		n.eng.Unfreeze(part)
	}()

	if err = n.eng.Freeze(ctx, part); err != nil {
		return err
	}

	// Phase 1: every member frozen, with every block any member holds processed
	// everywhere. Done by exchanging head sets: the union U of all heads is sent
	// out, each member reports which blocks of U it lacks, and we cut only after
	// consecutive full rounds in which U did not grow and nothing was missing.
	union := toSet(n.eng.Heads(ctx, part))
	stable := 0
	deadline := time.Now().Add(n.cfg.PrepareTimeout)
	for {
		prepared := map[string]msg{}
		n.send(ctx, msg{T: "prepare", Round: round, Part: part, Union: keys(union)})
		if err = n.collect(ctx, ch, "prepared", prepared, members, n.cfg.PollEvery); err != nil {
			return err
		}
		grew := false
		for _, h := range n.eng.Heads(ctx, part) {
			grew = add(union, h) || grew
		}
		missing, behind, rejected := []string{}, []string{}, ""
		for _, id := range members {
			r, ok := prepared[id]
			switch {
			case !ok:
				missing = append(missing, id)
			case !r.OK:
				rejected = id + ": " + r.Reason
			default:
				for _, h := range r.Heads {
					grew = add(union, h) || grew
				}
				if len(r.Missing) > 0 || r.Busy {
					behind = append(behind, id)
				}
			}
		}
		if rejected != "" {
			return fmt.Errorf("%w: %s", ErrBusy, rejected)
		}
		selfBehind := len(n.eng.Missing(ctx, part, keys(union))) > 0 || !n.eng.Quiescent(ctx, part)
		if len(missing) == 0 && len(behind) == 0 && !grew && !selfBehind {
			if stable++; stable >= 2 {
				break
			}
			continue // confirm with another full round
		}
		stable = 0
		if time.Now().After(deadline) {
			if len(missing) > 0 {
				return fmt.Errorf("%w: %v", ErrMemberUnavailable, missing)
			}
			return fmt.Errorf("%w: %v still catching up", ErrNotConverged, behind)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	// Phase 2: seed and activate here, then switch everyone else.
	genesis, _, serr := n.eng.Stage(ctx, part, true)
	if serr != nil {
		return serr
	}
	if err = n.eng.Activate(ctx, part, false); err != nil {
		_ = n.eng.Rollback(ctx, part)
		return err
	}
	activated = true
	gen := n.eng.Generation(part)

	ready := map[string]msg{}
	deadline = time.Now().Add(n.cfg.SwitchTimeout)
	for {
		n.send(ctx, msg{T: "switch", Round: round, Part: part, Gen: gen, Genesis: genesis})
		_ = n.eng.ResendSeed(ctx, part)
		if err = n.collect(ctx, ch, "ready", ready, members, n.cfg.PollEvery); err != nil {
			return err
		}
		allReady, why := true, ""
		for _, id := range members {
			r, ok := ready[id]
			if !ok {
				allReady = false
			} else if !r.OK {
				return fmt.Errorf("rotation: %s failed to switch: %s", id, r.Reason)
			}
		}
		if allReady {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: not all members switched (%s)", ErrMemberUnavailable, why)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	// Phase 3: commit. From here the old generation is gone everywhere.
	for i := 0; i < 3; i++ {
		n.send(ctx, msg{T: "done", Round: round, Part: part})
	}
	activated = false
	return n.eng.Commit(ctx, part)
}

// CompactAll rotates every partition in turn (a rolling compaction: only one
// partition's writes pause at a time).
func (n *Node) CompactAll(ctx context.Context) error {
	for i := 0; i < n.eng.NumPartitions(); i++ {
		if err := n.Compact(ctx, i); err != nil {
			return fmt.Errorf("partition %d: %w", i, err)
		}
	}
	return nil
}

// collect gathers replies of one kind for up to d (or until every member has
// answered), keyed by sender.
func (n *Node) collect(ctx context.Context, ch chan msg, kind string, into map[string]msg, members []string, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		complete := true
		for _, id := range members {
			if _, ok := into[id]; !ok {
				complete = false
			}
		}
		if complete {
			return nil
		}
		select {
		case m := <-ch:
			if m.T == kind {
				into[m.From] = m
			}
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func toSet(xs []string) map[string]struct{} {
	m := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		m[x] = struct{}{}
	}
	return m
}

func add(m map[string]struct{}, x string) bool {
	if _, ok := m[x]; ok {
		return false
	}
	m[x] = struct{}{}
	return true
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
