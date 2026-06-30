# Developing redgeo

This is the orientation a Go developer needs to work productively in this repo.
Read **[DESIGN.md](DESIGN.md)** for the *why*; this document is the *how* and the
map. Section references like §6.4 point into DESIGN.md.

## Mental model in one paragraph

go-ds-crdt gives us exactly one CRDT type: an Add-Wins OR-Set used as a
`key → []byte` register with **no** atomic RMW/CAS, **no** TTL, **no** native
counters/lists/hashes, and tombstones that are never GC'd. We cannot bolt Redis
onto it as a transactional KV store. Instead we **decompose every Redis
structure into many flat keys** chosen so concurrent writers touch different
keys, and fall back to **embedded CRDT codecs in the value** (counters) when
that isn't possible. The Redis command/RESP machinery is reused by forking
redka and reimplementing its storage seam (the six `R*` interfaces) against
go-ds-crdt.

## Repository layout

```
cmd/redgeo/      main: flags, libp2p bootstrap, server start
server/          redcon wiring, per-connection state, middleware (SELECT, MULTI, pub/sub, HELLO, INFO)
command/         forked redka command implementations, one subpackage per type
  conn/ key/ string/ hash/ set/ zset/ list/ server/
  command.go     the Parse() registry (grows as commands are added)
redisapi/        forked redka `redis` pkg: Cmd/Writer interfaces + the six R* interfaces + Redka holder
restypes/        result/option structs lifted out of redka's leaked builder types (backend-neutral)
parser/          forked redka argument parser (Pipeline + leaf/combinator parsers)
core/            forked redka core types (TypeID, Key, Value, error sentinels)
hlc/             hybrid logical clock
engine/          go-ds-crdt setup: partition DAG routing, Pebble/in-mem backing, broadcasters, rotation
crdtstore/       ★ the backend: the six R* impls + key codec, slots, meta, counters, locks, scan, txn, compaction
```

### Dependency direction (no import cycles)

```
core, restypes            (leaf: no redgeo deps)
  └─ redisapi             (interfaces over core + restypes)
       └─ command/*       (+ parser)
crdtstore  ── implements redisapi structurally ──>  (imports redisapi, restypes, core, engine, hlc)
engine     ── go-ds-crdt / pebble / libp2p
server     ── command, crdtstore, redisapi, engine
```

`crdtstore` satisfies the `redisapi.R*` interfaces **structurally** (Go ducktyping)
and imports `redisapi` only to build the `Redka` holder — there is no cycle.

## The seam: the six R* interfaces

`redisapi.Redka` bundles `RStr, RKey, RHash, RSet, RZSet, RList`. The entire
copied command layer depends *only* on these plus `redisapi.Writer`/`Cmd`.
**Implement them and the whole RESP/command layer comes for free.** `crdtstore`
implements all six; `Store.Redka(db)` returns them bound to a logical DB.

redka leaked some concrete result/builder structs into those interfaces
(`rstring.SetCmd`, `rzset.RangeCmd`, scan results). We lifted those into the
backend-neutral `restypes` package and replaced builder-returning methods with
direct option-struct methods (e.g. `Range(key, restypes.RangeOpts)`).

## Key & value encoding (the heart — crdtstore/keys.go, slot.go, meta.go)

Every key is path-like and leads with a partition bucket:

```
/{P}/m/{db}/{key}/{replicaID}            key metadata (HLC-LWW slot)
/{P}/d/{db}/{key}/v/{replicaID}          string value (HLC-LWW slot)
/{P}/d/{db}/{key}/c/{replicaID}          counter component (summed)
/{P}/d/{db}/{key}/h/{field}/{replicaID}  hash field (HLC-LWW slot)
/{P}/d/{db}/{key}/h/{field}/c/{replica}  hash-field counter component
/{P}/d/{db}/{key}/e/{member}             set member (presence-only OR-Set)
/{P}/d/{db}/{key}/z/{member}/{replicaID} zset score (HLC-LWW slot)
/{P}/d/{db}/{key}/l/{posKey}             list element (order-preserving posKey)
```

- `{P}` = `bucket(db, key)` over 256 buckets (FNV-1a). Immutable once data
  exists (§11). Routes to a partition DAG in multi-DAG mode.
- Each user-supplied segment (`key`, `field`, `member`, `replicaID`) is
  **base32-encoded** so arbitrary binary can't break the `/`-delimited path
  (§5.4). Always use the builders in `keys.go`; never hand-format a key.
- A **slot** (`slot.go`) is `(hlc, tag{present|deleted}, value)`. Reads pick the
  max-`(HLC, replicaID)` slot → true last-writer-wins incl. deletes (§6.7). Each
  replica writes only *its own* slot, so the store's height-wins path is never
  exercised.
- **Counters** (`counter.go`) sum per-replica components — no replica writes
  another's component, so they converge by addition (§6.4).
- **Existence** of a collection is derived from live members (a prefix scan),
  not from meta (§5.2). `probe()` (probe.go) is the generic existence/type/TTL
  resolver used by EXISTS/TYPE/DEL/SCAN, with lazy TTL filtering.

## Concurrency model

redcon runs one goroutine per connection with no serialization, and go-ds-crdt
processes local writes synchronously. A **sharded per-key lock manager**
(`locks.go`) makes read-modify-write sequences (counters, NX checks, list
end-pushes, type checks) atomic *within a node*. Lock on the logical
`(db, key)` via `lockKey(db, key)`. Cross-node correctness comes from the CRDT
encoding, not these locks.

## The engine (engine/)

Wraps go-ds-crdt. Single DAG by default; with `NumPartitions > 1` it routes keys
to N named partition DAGs by the leading `/{P}/` bucket, each with its own
namespace and broadcaster. The active datastore of each partition is
mutex-guarded and **swappable** so `RotatePartition`/`Rotate` can do global-purge
compaction (snapshot live → fresh genesis namespace → purge old). Broadcasters
come from the no-op (single node), the in-process `MemNetwork` (tests), or the
libp2p `Cluster` (production).

`crdtstore` never touches `engine` directly for data ops — it goes through
**txn-aware Store wrappers** (`put/get/has/del/query` in `txn.go`) so MULTI/EXEC
can route writes into one batch + a read-your-writes overlay. **When adding
backend code, use `s.put/s.get/s.has/s.del/s.query`, not `s.eng.*`.**

## How to add a command

Most commands are mechanical because the storage seam already exists.

1. **Backend (only if a new capability is needed):** add/extend a method on the
   relevant repo in `crdtstore/r<type>.go`. Reuse `readSlots`/`writeSlot`,
   `probe`, `hashLiveFields`, `zsetLiveScores`, the counter/locks helpers. Route
   data ops through `s.put/get/has/del/query`. Hold `s.locks.Lock(lockKey(...))`
   for read-modify-write.
2. **Interface:** if the method is new, add it to the `redisapi.R*` interface.
3. **Command:** add a file under `command/<type>/`. Port from redka where one
   exists (adapt the three import paths: `internal/core` → `redgeo/core`,
   `redsrv/internal/parser` → `redgeo/parser`, `redsrv/internal/redis` →
   `redgeo/redisapi`). Each command is a struct embedding `redis.BaseCmd`, a
   `Parse*` constructor (use the `parser` pipeline), and a `Run(w redis.Writer,
   red redis.Redka)` that calls `red.<Type>().<Method>(...)` and writes the
   reply.
4. **Register:** add a `case` in `command/command.go`.
5. **Test:** add an end-to-end case in `server/` and, for anything with
   conflict semantics, a convergence case in `crdtstore/`.

RESP3-aware replies: use `w.WriteMap` (HGETALL/HELLO), `w.WriteDouble` (scores),
`w.WriteBool`, and `w.WriteNull` — they emit RESP3 when the connection
negotiated proto 3 and fall back to RESP2 otherwise (handled by the forked
redcon).

## Fork dependencies

redgeo consumes two redis-geo forks via `replace` to **local sibling paths**.
A fresh clone won't build until they're checked out next to this repo, or the
`replace`s are repinned to fork commits (DESIGN §7.1, §12.6):

| Dependency | Path | Fork branch | Why |
|---|---|---|---|
| go-ds-crdt | `../go-ds-crdt` | `lazy-broadcast-batch-ch` | Lazy-allocate `broadcastBatchCh` so 256 idle DAGs cost ~1.6 MiB not ~500 MiB (§11) |
| redcon | `../redcon` | `resp3` | Native RESP3 writers + pub/sub push frames (§12.4) |

For reproducible/CI builds, merge those branches or repin the `replace`s to
commit hashes.

## Gotchas

- **go-ds-crdt `Query` results are not guaranteed sorted.** Anything order-
  sensitive (lists, scans, ranks) must sort explicitly. Lists sort by their
  order-preserving `posKey`.
- **Deleting a slot register is a write, not a `ds.Delete`.** To delete a
  string/hash-field/zset value you write a `deleted`-tagged slot with a fresh
  HLC (so it wins the max-HLC read). `ds.Delete` only tombstones one replica's
  slot and another replica's `present` slot could still win. Set members and
  list elements *are* deleted with `ds.Delete` (OR-Set remove).
- **Counters and strings don't mix** — enforced; `SET` on a counter and `INCR`
  on a plain string both error.
- **`nowMS` (crdtstore) is a package var** — override it in tests for
  deterministic TTL/expiry; don't sleep.
- **The partition bucket count (256) is effectively immutable** once data
  exists — changing it rehashes every key.
- Run `go test ./... -race` before pushing; the engine and locks are concurrent.

## Conventions

- Format with `gofmt`; keep `go vet ./...` clean.
- Match the surrounding code's comment density and naming; reference DESIGN
  sections (§x.y) in comments for non-obvious encodings.
- Commit messages: imperative subject; we sign off (`git commit -s`). One
  logical change per commit.
