# AGENTS.md

Guidance for coding agents working in this repo. Humans: see
[README.md](README.md), [DESIGN.md](DESIGN.md), [DEV.md](DEV.md),
[TEST.md](TEST.md). **Read [DEV.md](DEV.md) before non-trivial backend work** —
it has the encoding rules and gotchas that are easy to get subtly wrong.

## What this is

redgeo is an active/active, geo-distributed, **Redis-compatible** server in Go,
backed by go-ds-crdt (CRDT storage), redcon (RESP wire), and a fork of redka's
command layer. Module: `github.com/redis-geo/redgeo`. Go 1.25+.

## Build / test / lint

```sh
go build ./...
go test ./...           # hermetic: in-memory engine, in-process replicas, no external deps
go test ./... -race     # run before committing concurrency-touching changes
go vet ./...
gofmt -w .              # must be clean (gofmt -l . prints nothing)
go run ./cmd/redgeo     # runs a single in-memory node on :6380
```

**Fork dependencies:** `go.mod` has `replace`s to two local sibling forks
(`../go-ds-crdt` on branch `lazy-broadcast-batch-ch`, `../redcon` on branch
`resp3`). The repo only builds with those checked out as siblings. Do not remove
the `replace`s; do not "fix" the build by deleting them.

## Architecture (where things live)

```
cmd/redgeo/   binary + flags
server/       redcon wiring; SELECT/MULTI/EXEC/DISCARD/pub-sub/HELLO/INFO middleware
command/      forked redka commands, one subpackage per type; command.go is the registry
redisapi/     Cmd/Writer + the six R* storage interfaces (the seam) + Redka holder
restypes/     backend-neutral result/option structs
parser/ core/ forked redka arg parser + core types
hlc/          hybrid logical clock
engine/       go-ds-crdt setup: partition DAG routing, Pebble/in-mem, broadcasters, rotation
crdtstore/    ★ the backend: six R* impls + key codec, slots, meta, counters, locks, scan, txn, compaction
```

The command layer depends only on the six `redisapi.R*` interfaces. `crdtstore`
implements them against go-ds-crdt by decomposing each Redis type into flat
CRDT keys. To add a command: implement/extend the backend method
(`crdtstore/r<type>.go`) → add to the `R*` interface if new → add a
`command/<type>/` file → register a `case` in `command/command.go` → add tests.
Full recipe in DEV.md.

## Non-negotiable rules (these cause silent CRDT bugs if violated)

- **Backend data ops go through the txn-aware Store wrappers** `s.put / s.get /
  s.has / s.del / s.query` — never `s.eng.*` directly (breaks MULTI/EXEC
  batching + read-your-writes).
- **Build keys only via the `keys.go` builders.** User segments are base32-
  encoded; never hand-concatenate paths.
- **Deleting a slot register = writing a `deleted`-tagged slot** with a fresh
  HLC (string / hash-field / zset / meta), NOT `ds.Delete`. Set members and
  list elements use `ds.Delete` (OR-Set remove). Getting this wrong lets deleted
  keys resurrect.
- **Sort `Query` results explicitly** when order matters — go-ds-crdt does not
  guarantee order.
- **Hold `s.locks.Lock(lockKey(db,key))`** around read-modify-write.
- **Counters and plain strings don't mix** (enforced) — preserve that.
- **Don't change the 256 partition-bucket count** — it's effectively immutable
  once data exists.
- **Tests inject time** (`nowMS` var, `hlc.NewWithClock`) — don't add `sleep`s
  for TTL/clock logic; convergence tests poll via `eventually`.

## Manual RESP testing

Use `redis-cli` (or `redis-cli -3` for RESP3) or the Go test client. Do **not**
pipe many inline commands through `nc` — inline pipelining over `nc` is
unreliable and produces fake-looking corruption; it is not a server bug.

## Conventions

- `gofmt`; keep `go vet` clean; match surrounding style and comment density.
- Reference DESIGN sections (§x.y) in comments for non-obvious encodings.
- Commit messages: imperative subject, sign off (`git commit -s`), one logical
  change per commit. Only commit/push when asked.

## Status & residuals

Phases 0–9 of DESIGN §9 are implemented plus all originally-deferred items.
Known residual limitations (deliberate trade-offs, not bugs) are in **DESIGN
§12** — read it before assuming something is broken (e.g. cross-partition MULTI
is intentionally not atomic across partitions; collection HSCAN/SSCAN/ZSCAN are
single-page; geo pub/sub is local-only).
