# redgeo — Design & Implementation Plan

An **active/active, geo-distributed, Redis-compatible** server in Go, backed by
[`go-ds-crdt`](https://github.com/ipfs/go-ds-crdt) as the storage layer,
[`redcon`](https://github.com/tidwall/redcon) for the RESP wire protocol, and
[`redka`](https://github.com/nalgeon/redka) as the blueprint for command
implementation.

> Working name: **redgeo** (`github.com/redis-geo/redgeo`). Rename freely.

---

## 1. Goals & non-goals

### Goals
- A Redis-protocol (RESP2, RESP3 later) server that multiple geographically
  distributed nodes can all accept **writes** to simultaneously (multi-master).
- **Eventual consistency** with deterministic, conflict-free convergence across
  replicas, using CRDT semantics under the hood.
- Reuse redka's command/parsing/RESP layer by reimplementing its storage seam.
- Be **honest** about which commands are conflict-free vs. which carry weaker
  semantics under concurrency, and document it per command.

### Non-goals (initially)
- Strong consistency, linearizability, or `WATCH`/CAS optimistic locking.
- Redis Cluster slot protocol, scripting (`EVAL`), Streams, Geo, Bitmaps,
  HyperLogLog. (redka doesn't have these either.)
- Byte-for-byte Redis behavior on every edge case. We target the common subset
  with documented semantics.

---

## 2. The storage substrate — what go-ds-crdt actually gives us

This drives every design decision, so state it plainly. `go-ds-crdt` is **one
CRDT type**: an **Add-Wins Observed-Remove Set used as a `key → []byte`
register**. It implements `ds.Datastore` + `ds.Batching`.

| Capability | Reality |
|---|---|
| Conflict resolution | **Highest Merkle-DAG height wins** (a logical clock, *not* wall-clock), tie-broken by value-byte comparison. Per key. |
| Concurrent same-key writes | One side **silently loses** (deterministically). No multi-value register exposed. |
| Add vs. Remove on same key | **Add wins** (observed-remove: only observed block-IDs are tombstoned). |
| Atomic read-modify-write / CAS | ❌ none. Writes are blind `Put`/`Delete`. |
| TTL / expiry | ❌ not implemented. |
| Native counters/lists/sets/hashes | ❌ none. The library never merges value internals. |
| Query | Prefix scan is pushed down (efficient). Ordering/limit/filter are **naive in-memory** full scans (`NaiveQueryApply`). |
| Batch | Accumulates into one delta → one DAG node → one atomic broadcast. No isolation/rollback. |
| Deletes | Tombstones, propagated like adds. **Never GC'd** without manual `PurgeDAG`/compaction. |
| Local read-your-writes | ✅ holds (local `Put` processed synchronously before return). Cross-replica: eventual only. |
| Replication | `Broadcaster` (libp2p gossipsub provided) + `ipld.DAGService` (IPFS-Lite). Anti-entropy via periodic rebroadcast + DAG repair. |
| Named DAGs | Independent head-sets processed in parallel → a keyspace-partitioning lever. |

**Constructor:**
```go
crdt.New(store ds.Datastore, namespace ds.Key, dagSyncer ipld.DAGService,
         bcast Broadcaster, opts *Options) (*Datastore, error)
```
`Options.PutHook(k, v)` / `DeleteHook(k)` fire on prevalent add/remove (local or
remote) — our hook for keyspace notifications and local index maintenance.

### The single most important consequence
We **cannot** treat go-ds-crdt as a transactional KV store and bolt Redis on top.
We must design CRDT encodings that:
1. **Decompose** every Redis structure into many flat keys, each chosen so that
   *concurrent writers touch different keys* whenever possible, and
2. fall back to **embedded CRDT codecs in the value** (e.g. counters) when (1)
   isn't achievable.

When two replicas genuinely write the *same* key, we accept register semantics
(height-wins) — and offer an optional multi-value/HLC register for keys where
intuitive last-writer-wins matters (§6.7).

---

## 3. Reuse strategy — fork redka's command layer

redka's command code lives under `internal/` (`redsrv/internal/command`,
`redsrv/internal/redis`, `redsrv/internal/parser`) and `internal/core`. Go's
`internal/` rule means **we cannot import these across modules** — we must
**copy them into our module** (a vendored fork). What we copy vs. replace:

| redka package | Action |
|---|---|
| `internal/core` (TypeID, Key, Value, conversions, error sentinels) | **Copy as-is.** Backend-agnostic. |
| `redsrv/internal/parser` | **Copy as-is.** Pure arg parsing. |
| `redsrv/internal/command/**` (one file per command) | **Copy as-is.** Depends only on the `redis.R*` interfaces + `redis.Writer`/`redis.Cmd`. |
| `redsrv/internal/redis/redis.go` (Cmd/Writer interfaces) | **Copy as-is.** |
| `redsrv/internal/redis/redka.go` (the six `R*` interfaces + `Redka` struct) | **Copy, then re-point** its constructors at our backend. |
| `redsrv/handlers.go`, `state.go`, `server.go` | **Copy & adapt** (middleware chain, MULTI queue, redcon wiring). |
| `internal/rstring`, `rhash`, `rlist`, `rset`, `rzset`, `rkey` (SQLite repos) | **Replace** with our `crdtstore` package. |
| `internal/sqlx` | **Drop.** Replaced by the CRDT engine. |

The seam we implement is the six interfaces in `redis/redka.go`:
`RStr, RHash, RList, RSet, RZSet, RKey`. The entire copied command layer depends
only on these. **Implement them against go-ds-crdt and the whole RESP/command
machinery comes for free.**

> Caveat: those interfaces currently leak a few concrete result structs
> (`rhash.ScanResult`, `rstring.SetCmd`, `rzset.SetItem`, …). We'll lift those
> result types into our own package (or into `core`) so the interfaces are
> backend-neutral. This is a mechanical refactor done once during the fork.

---

## 4. Architecture

```
            ┌─────────────────────────────────────────────────────────┐
 RESP/TCP   │  redcon server (1 goroutine/conn, RESP2[/3], pipelining) │
 clients ──▶│  per-conn state: selected DB, RESP ver, MULTI queue, subs │
            └───────────────────────────┬─────────────────────────────┘
                                         │ redis.Cmd.Run(w, Redka)
            ┌────────────────────────────▼─────────────────────────────┐
 (forked    │  command layer  (parse → multi → handle)                  │
  redka)    │  depends ONLY on redis.{RStr,RHash,RList,RSet,RZSet,RKey} │
            └────────────────────────────┬─────────────────────────────┘
                                         │  ← THE SEAM we implement
            ┌────────────────────────────▼─────────────────────────────┐
 crdtstore  │  six R* impls → flat-key encoding + CRDT value codecs     │
            │  per-key local lock manager (RMW), meta model, sweeper    │
            └────────────────────────────┬─────────────────────────────┘
            ┌────────────────────────────▼─────────────────────────────┐
 engine     │  go-ds-crdt Datastore (PutHook/DeleteHook, Batch, Query)  │
            │  namespace + key codec; backing store = Pebble            │
            └────────────────────────────┬─────────────────────────────┘
            ┌────────────────────────────▼─────────────────────────────┐
 replication│  libp2p host + DHT + gossipsub + IPFS-Lite (DAGService)   │
            │  PubSubBroadcaster on a shared topic; anti-entropy        │
            └───────────────────────────────────────────────────────────┘
```

Packages in our module:
```
redgeo/
  cmd/redgeo/main.go              binary: flags, libp2p bootstrap, server start
  server/                         redcon wiring, conn state, middleware (forked)
  command/                        forked redka command implementations
  redisapi/                       forked redis.{Cmd,Writer,R*} interfaces + Redka
  core/                           forked redka core types + lifted result structs
  crdtstore/                      ★ our backend: six R* impls
    keys.go                       key codec (§5)
    meta.go                       per-key metadata register (§5.3)
    str.go hash.go set.go         R* impls
    zset.go list.go counter.go
    locks.go                      sharded per-key lock manager
    expire.go                     TTL sweeper
    codec.go                      value codecs (counter, score, meta)
  engine/                         go-ds-crdt setup, Pebble, replication mesh
```

### Concurrency model
redcon runs **one goroutine per connection with no serialization**. go-ds-crdt
processes local writes synchronously (local read-your-writes). We therefore add
a **sharded per-key lock manager** (`crdtstore/locks.go`) used to make
read-modify-write sequences (counters, list end-pushes, type checks, NX) atomic
*within a node*. Cross-node, correctness comes from the CRDT encoding, not locks.

---

## 5. Key & value encoding

All keys live under one go-ds-crdt namespace (e.g. `New(store, ds.NewKey("/redgeo"), …)`).
Keys are path-like `ds.Key`s; prefix scans are the efficient query primitive, so
the encoding is designed around **prefix locality**.

Notation: `{db}` = logical DB index (Redis `SELECT`, default 0). `{key}` = the
Redis key, percent/length-escaped so `/` in user keys can't break the path
(see §5.4). `{field}`/`{member}` likewise escaped.

### 5.1 Layout

All single-value registers use the **per-replica LWW slot** encoding (§6.7):
one key *per writing replica*, holding `(hlc, tag, value)`. Each replica writes
only its own slot, so there are **never concurrent writes to the same key** —
the store's own height-wins resolution is never even exercised; reads pick the
slot with the **max HLC** (true last-writer-wins, including deletes). Slot count
is bounded by the number of replicas (not by write volume → no unbounded
tombstones from overwrites).

| Purpose | Key | Value |
|---|---|---|
| Key metadata | `/m/{db}/{key}/{replicaID}` | LWW slot: `(hlc, tag, KeyMeta)` (§5.3) |
| String value | `/d/{db}/{key}/v/{replicaID}` | LWW slot: `(hlc, tag, bytes)` |
| Counter component | `/d/{db}/{key}/c/{replicaID}` | int64/float64 — single-writer per key |
| Hash field | `/d/{db}/{key}/h/{field}/{replicaID}` | LWW slot: `(hlc, tag, bytes)` |
| Set member | `/d/{db}/{key}/e/{member}` | `∅` (presence only) |
| ZSet member→score | `/d/{db}/{key}/z/{member}/{replicaID}` | LWW slot: `(hlc, tag, score)` |
| List element | `/d/{db}/{key}/l/{posKey}` | raw bytes (element) |

`tag ∈ {present, deleted}`. A `deleted` slot with a higher HLC beats an older
`present` slot, giving intuitive **LWW-with-delete** for registers — unlike sets,
which intentionally use add-wins.

- **Hash** = a per-replica-slot register per field → a true **LWW-Map**.
- **Set** = one presence key per member → maps directly onto go-ds-crdt's native
  **Add-Wins OR-Set** (no HLC slot needed; add-wins is the correct set
  semantics). SADD = `Put`, SREM = `Delete`. *This is the natural fit.*
- **ZSet** = LWW-Map of member→score (per-replica slots); ranges computed by
  prefix-scan + in-memory sort by score (v1). Optional ordered secondary index
  `…/zi/{scoreEnc}/{member}` for large zsets (later; §6.6).
- **List** = fractional-index sequence (§6.5); `posKey` is order-preserving.
- **Counter** = per-replica component keys; the total is the **sum** of all
  `…/c/*` (§6.4). Same single-writer-per-key principle as the LWW slot, but the
  components are summed instead of max-HLC-picked.

### 5.2 Collection existence
A collection (hash/set/zset/list) **exists iff it has ≥1 live member** — derived
from a prefix scan, not from the presence of a meta record. This matches OR-Set
semantics and dodges a "does the key exist" race. `KeyMeta` carries **type +
TTL + bookkeeping only**; it is not the source of truth for existence of
collections. For strings, the `/d/{db}/{key}/v/*` (or counter components) key is
the truth.

### 5.3 KeyMeta
```go
type KeyMeta struct {
    Type    core.TypeID // 1 str, 2 list, 3 set, 4 hash, 5 zset
    ETimeMS int64       // absolute expiry, unix ms; 0 = no expiry
    Epoch   uint32      // bumped on full DEL+recreate to fence stale members (§6.3)
}
```
Stored as a **per-replica LWW slot** at `/m/{db}/{key}/{replicaID}` (the `(hlc,
tag, KeyMeta)` envelope of §6.7 carries the HLC and present/deleted tag, so it
isn't a `KeyMeta` field). Serialized compactly (protobuf or a hand-rolled
fixed-layout; protobuf preferred for forward-compat). Reads pick the max-HLC
slot, so the effective type/TTL is the wall-clock-latest writer's.

### 5.4 Escaping
Redis keys/fields/members are arbitrary binary. `ds.Key` is `/`-delimited path
text. Encode each user-supplied segment as **`<len>:<bytes>`** (length-prefixed)
or hex/base32 so that `/`, control bytes, and empty segments are unambiguous and
can never collide or break prefix scans. Decision: length-prefixed raw within a
single path segment using a reversible escape; finalize in `keys.go`.

---

## 6. Per-feature CRDT design & semantics

### 6.1 Strings (Tier 1 — HLC-LWW register, correct)
- `SET k v` → write the local LWW slot `/d/{db}/{key}/v/{R}` + meta(type=str).
  `GET` → max-HLC slot read. `MSET`/`MGET` via `Batch`. `STRLEN` from the winning
  slot. `DEL` → write a `deleted`-tagged slot. `EXISTS`/`TYPE` via meta + slots.
- **Semantics:** concurrent `SET` to the same key → **true last-writer-wins by
  HLC** (§6.7), deterministic and intuitive across all replicas.
- `SETNX` / `SET NX` / `SET XX` / `GETSET`: the existence check is **node-local
  only**; cross-replica concurrent NX can both observe "absent" and both succeed
  (the later HLC then wins the value). Document as best-effort.

### 6.2 Hashes (Tier 1 — LWW-Map, correct)
- `HSET`/`HMSET`/`HSETNX` → write LWW slot per field `/d/{db}/{key}/h/{field}/{R}`;
  `HDEL` → `deleted`-tagged slot per field.
- `HGET`/`HMGET` → max-HLC slot read per field; `HGETALL`/`HKEYS`/`HVALS`/`HLEN`
  → prefix scan of `/d/.../h/`, reducing each field's slots to its winner.
  `HLEN` = count of fields whose winning slot is `present`.
- **Semantics:** per-field HLC-LWW register. Concurrent writes to *different*
  fields always merge cleanly; same field → last-writer-wins by HLC.

### 6.3 Sets (Tier 1 — native OR-Set, the best fit)
- `SADD` → `Put(/d/.../e/{member}, ∅)`; `SREM` → `Delete`. `SISMEMBER` → `Has`.
  `SMEMBERS`/`SCARD`/`SSCAN` → prefix scan. `SUNION`/`SINTER`/`SDIFF` computed in
  app from scans; `*STORE` writes results via `Batch` (the store step is a
  non-atomic blind overwrite — document).
- **Semantics:** Add-Wins OR-Set — concurrent `SADD x` / `SREM x` → **x stays**
  (add wins). This is *correct conflict-free* behavior and the showcase type.
- **Epoch fencing:** `FLUSHDB`/full-key `DEL` should not let a slow in-flight
  `SADD` resurrect a member into a "deleted" set inconsistently. We bump
  `KeyMeta.Epoch` and *could* include epoch in the member key prefix
  (`/d/{db}/{key}/{epoch}/e/{member}`) to fence stale writers. v1: rely on
  OR-Set add-wins (a concurrent re-add surviving a delete is arguably correct);
  epoch is a hardening option for full-key DEL semantics.

### 6.4 Counters — `INCR`/`INCRBY`/`DECR`/`HINCRBY`/`ZINCRBY`/`INCRBYFLOAT` (Tier 1 *with codec*)
**This is the key win that moves counters from "broken" to "correct."**
- Encode a counter as a **CRDT PN-counter via per-replica component keys**:
  `/d/{db}/{key}/c/{replicaID}` holds *this replica's* net contribution.
- Replica `R` does `INCRBY n`: lock key locally, `cur = Get(.../c/R)`,
  `Put(.../c/R, cur+n)`. **Only R ever writes `…/c/R`** → no cross-replica
  same-key write → height-wins register is exactly correct → the global value
  is `sum(all components)`, computed on read. This is a textbook state-based
  PN-counter and **converges correctly under concurrency**.
- `HINCRBY` field counters: `/d/{db}/{key}/h/{field}/c/{replicaID}`. `ZINCRBY`:
  per-replica score deltas similarly.
- **Returned value** is the locally-known sum (may exclude un-synced remote
  increments) — eventual consistency. Document.
- **Decision: counters and plain strings do not mix.** A key is *either* a plain
  string (LWW register, §6.1) *or* a counter (component encoding), recorded as a
  distinct flavor in `KeyMeta`. `INCR` on a key that exists as a plain string
  returns the Redis error `value is not an integer or out of range`; `SET` /
  `APPEND` on a counter key likewise errors (or, to match Redis's "SET always
  wins" leniency, is a config choice — default: reject). This eliminates the
  cross-replica `SET`-vs-`INCR` race entirely, so **pure counters are fully
  correct** under concurrency. Cost: a slight deviation from Redis, which treats
  integers as just strings. Documented.

### 6.5 Lists (Tier 2 — converges; index ops race)
- **Fractional indexing (LSEQ-style).** Each element key is
  `…/l/{posEnc}` where `posEnc` is an **order-preserving** encoding of a
  fractional position plus a `{replicaID}/{seq}` tiebreaker so concurrent pushes
  at the same logical position both survive in deterministic order.
- `RPUSH`: pos = `max + 1`; `LPUSH`: pos = `min − 1` (min/max from first/last key
  in scan; node-local lock around it). `LINSERT`: midpoint between neighbors.
  These mirror redka's `REAL pos` scheme — which is itself a sequence-CRDT
  technique, so it ports naturally and **pushes commute across replicas.**
- `LRANGE`/`LINDEX`/`LLEN` via ordered prefix scan; logical indices derived by
  enumeration order. `LSET` = overwrite element value (register). `LREM`/`LPOP`/
  `RPOP`/`LTRIM` = tombstone element keys (OR-Set remove, idempotent).
- **Semantics:** concurrent pushes converge. **Index-based ops** (`LSET i`,
  positional `LPOP count`, `LTRIM`) race because "the element at index i" differs
  per replica before convergence. Document; lists are the weakest type (Redis
  lists are not a clean CRDT).

### 6.6 Sorted sets (Tier 2 — score LWW-Map)
- `ZADD` → write LWW score slot `/d/.../z/{member}/{R}`; `ZSCORE` → max-HLC slot
  read; `ZREM` → `deleted`-tagged slot.
- Range/rank ops (`ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREVRANGE`, `ZCOUNT`, …)
  → prefix scan of `/d/.../z/`, reduce each member's slots to its winning score,
  then sort by `(score, member)` in memory (v1).
- For large zsets add ordered index keys `…/zi/{scoreEnc}/{member}` (scoreEnc =
  order-preserving float encoding) so ranges become prefix scans without a full
  in-memory sort. Maintained alongside the primary score key; tombstoned on
  `ZREM`/score change.
- **Semantics:** member→score is per-member LWW register; ranking is eventually
  consistent. `ZADD GT/LT/NX/XX` flags are node-local. `ZINCRBY` uses the counter
  codec on the score (§6.4).

### 6.7 Register conflict policy — per-replica LWW slots (cross-cutting, v1)
**Decision: true HLC-LWW from v1** for every single-value register (strings,
hash fields, zset scores, key metadata).

The raw store resolves same-key conflicts by **DAG height**, which can pick a
*causally-older wall-clock* write — counter-intuitive for users expecting
"latest write wins." We avoid relying on it entirely with the **per-replica LWW
slot**:

- A logical register at `{base}` is stored as **N slots**, one per writing
  replica: `{base}/{replicaID}` → `(hlc, tag, value)`, where `tag ∈
  {present, deleted}` and `hlc` is a **hybrid logical clock** stamp.
- **Write/delete** on replica `R`: under the node-local key lock, advance the
  local HLC and overwrite *only* `{base}/{R}`. Because each replica owns its own
  slot, **no two replicas ever write the same key** — the store's height-wins
  path is never exercised; convergence is trivial.
- **Read:** prefix-scan `{base}/`, pick the slot with the **max `(hlc,
  replicaID)`**. If that slot's `tag == deleted` (or no slot), the register is
  absent. This is exact last-writer-wins **including deletes** — more intuitive
  than the store's add-wins for scalar values.

Properties: slot count is **bounded by the number of replicas**, not by write
volume, so repeated overwrites don't accumulate tombstones (each replica reuses
its one slot). Read cost is O(#replicas) point-reads / a tiny prefix scan —
cheap for a geo deployment of a handful of regions. The **HLC** is seeded from
wall-clock and monotonically advanced per the standard HLC algorithm; the
`replicaID` (from the libp2p peer ID, §8) breaks exact-tie HLCs deterministically.

Sets keep **add-wins OR-Set** (not LWW) — that is the semantically correct
behavior for set membership and needs no HLC slot. Counters keep **summed
per-replica components** (§6.4).

### 6.8 TTL / expiry (Tier 2)
- `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`SETEX`/`PSETEX`/`PERSIST`/`TTL`/`PTTL`:
  store absolute `ETimeMS` in `KeyMeta`. `PERSIST` → `ETimeMS=0`.
- **Lazy expiry:** every read filters `ETimeMS == 0 || ETimeMS > now` (redka's
  model). Expired keys are invisible immediately.
- **Active sweeper:** background ticker scans `/m/{db}/` for `ETimeMS <= now` and
  issues CRDT `Delete`s of the meta + all element keys. The delete is itself a
  CRDT op so it propagates.
- **Semantics:** TTL is an HLC-LWW scalar in the meta slot. Race: replica A
  expires+deletes while B refreshed the TTL — B's higher-HLC meta write wins →
  key survives. Acceptable; document.

### 6.9 Keys / generic (Tier 1, with scan caveats)
- `DEL`/`UNLINK`/`EXISTS`/`TYPE`/`RANDOMKEY`/`RENAME`/`RENAMENX`/`PERSIST`.
- `KEYS pattern` / `SCAN cursor [MATCH] [COUNT]`: prefix-scan `/m/{db}/`; cursor
  is an **opaque last-key** (not Redis's reverse-binary-iteration cursor, but
  compatible contract: resumable, eventually complete). `MATCH` applied in app,
  `COUNT` is a hint. Ordering is naive — fine for scan.
- `RENAME` = copy element keys under new name + tombstone old (non-atomic across
  replicas; batched locally).
- `DEL` of a collection = tombstone meta + every element key (one `Batch`).

### 6.10 Transactions — `MULTI`/`EXEC`/`DISCARD` (Tier 3, weak)
- Reuse redka's middleware MULTI queue. On `EXEC`, run all queued commands and
  commit their writes through a single go-ds-crdt **`Batch`** → lands as one
  atomic delta (atomic *propagation*).
- **No isolation, no rollback, no `WATCH`.** Note this is *close to real Redis*,
  which also doesn't roll back runtime errors in a transaction. We lose redka's
  (stronger-than-Redis) SQL rollback. `WATCH`/`UNWATCH` unsupported.

### 6.11 Server / connection (Tier 4 — storage-orthogonal)
`PING`/`ECHO`/`SELECT`/`HELLO`(RESP3)/`COMMAND`/`CONFIG GET`/`INFO`/`DBSIZE`/
`FLUSHDB`/`FLUSHALL`/`LOLWUT`. Most are trivial; `INFO` reports replication/CRDT
stats (heads, max height, queued jobs via `InternalStats`).
- **Decision: all 16 logical DBs.** `SELECT 0..15` supported via the `{db}`
  segment in every key (§5.1); per-connection selected-DB lives in conn state.
  `DBSIZE`/`FLUSHDB` are scoped to the selected `{db}` prefix; `FLUSHALL` spans
  all 16. Each DB is an independent keyspace within the one CRDT namespace.

### 6.12 Pub/Sub (Tier 4, two modes)
- **Local pub/sub:** redcon's built-in `PubSub` (works out of the box).
- **Geo pub/sub:** publish across replicas over a dedicated libp2p gossipsub
  topic (separate from the CRDT data topic), or via `PutHook`-driven keyspace
  notifications. v1: local only; geo as a follow-up.

### Coverage summary
| Tier | Behavior | Commands |
|---|---|---|
| **1 — conflict-free correct** | Converges with intuitive HLC-LWW / OR-Set / counter semantics | strings, hashes, sets, counters (dedicated), keys/scan, server |
| **2 — converges, weaker semantics** | Eventually consistent; some ops race | zsets, lists, TTL, NX/XX existence checks |
| **3 — best-effort / degraded** | No isolation/rollback/CAS | MULTI/EXEC, `*STORE` |
| **out** | Not implemented | WATCH, scripting, streams, geo, bitmaps, HLL, cluster |

Estimated: **~60–70%** of common string/hash/set/key surface with *correct*
active/active semantics; **~15%** more with documented weaker semantics.

---

## 7. Replication mesh (engine)

Per the go-ds-crdt `globaldb` example, each node wires:
1. A persistent thread-safe backing store — **Pebble** (recommended).
2. A **libp2p host** + **DHT** (peer discovery / bootstrap) + **gossipsub**.
3. **IPFS-Lite** as the `ipld.DAGService` (block exchange).
4. A **`PubSubBroadcaster`** on a shared topic.
5. `crdt.New(pebble, ds.NewKey("/redgeo"), ipfsLite, broadcaster, opts)` with
   `PutHook`/`DeleteHook` wired to keyspace notifications + local indexes.

Scaling lever: shard the keyspace across **named DAGs** (e.g. by DB index or key
hash) for parallel head processing. v1 = single DAG.

Anti-entropy (rebroadcast + DAG repair) is built in; new/partitioned nodes catch
up automatically. We must call `Sync()` appropriately given Pebble is async.

---

## 8. Hard problems / risks (tracked explicitly)

1. **Tombstone growth.** Deletes/expiry grow the DAG forever. Need a compaction
   strategy; `PurgeDAG` requires quiescence on that DAG. Mitigation: partition
   churny keyspaces into named DAGs that can be purged independently; schedule
   compaction windows. **Open.**
2. **Counter/string duality** — resolved: counters and strings are distinct
   flavors and may not mix (§6.4), so this is no longer a correctness risk, only
   a documented Redis deviation.
3. **`len`/`DBSIZE` accuracy.** Counts come from scans (O(n)) or maintained
   counters that can drift. Decision: compute on demand for correctness; cache
   with `PutHook` invalidation for hot paths.
4. **Replica identity & HLC.** Need a stable per-node `replicaID` and a hybrid
   logical clock. Derive `replicaID` from the libp2p peer ID; persist it.
5. **SCAN cursor semantics** differ from Redis's; ensure clients tolerate an
   opaque resumable cursor.
6. **Read-after-write across replicas** is not guaranteed. Product-level
   expectation to surface to users.
7. **Memory/scan cost** for large collections (no pushed-down ordering/limit).
   Mitigate with secondary index keyspaces (zset `zi`, §6.6) where needed.

---

## 9. Phased implementation plan

| Phase | Deliverable | Commands / work |
|---|---|---|
| **0. Scaffold** | Module, fork redka command layer, single-node engine (in-mem + Pebble), redcon server, conn state. End-to-end `PING`. | wiring, lock manager, key codec skeleton |
| **1. Strings + keys + server** | Blind string ops, key mgmt, server cmds, KeyMeta model, SCAN/KEYS. | SET/GET/GETSET/MGET/MSET/STRLEN, DEL/EXISTS/TYPE/KEYS/SCAN/RANDOMKEY/RENAME, PING/ECHO/SELECT/INFO/DBSIZE/FLUSHDB |
| **2. Hashes + sets** | LWW-Map + native OR-Set — the natural-fit types. | HSET/HGET/HGETALL/HDEL/HKEYS/HVALS/HLEN/HEXISTS/HMGET/HSCAN; SADD/SREM/SISMEMBER/SMEMBERS/SCARD/SUNION/SINTER/SDIFF/SSCAN |
| **3. TTL** | Lazy filter + active sweeper. | EXPIRE/PEXPIRE/EXPIREAT/TTL/PTTL/PERSIST/SETEX/PSETEX |
| **4. Counters** | PN-counter component codec + local locks. | INCR/INCRBY/DECR/DECRBY/INCRBYFLOAT/HINCRBY/HINCRBYFLOAT |
| **5. Sorted sets** | Score LWW-Map + in-memory range; optional `zi` index. | ZADD/ZSCORE/ZREM/ZRANGE/ZRANGEBYSCORE/ZRANK/ZCARD/ZCOUNT/ZINCRBY/ZSCAN + REV variants |
| **6. Lists** | Fractional-index sequence. | LPUSH/RPUSH/LPOP/RPOP/LRANGE/LINDEX/LLEN/LSET/LREM/LINSERT/LTRIM/RPOPLPUSH |
| **7. MULTI + pub/sub** | Batch-backed MULTI/EXEC/DISCARD; local pub/sub. | MULTI/EXEC/DISCARD; (P)SUBSCRIBE/PUBLISH |
| **8. Replication** | libp2p mesh, multi-node, convergence + partition/heal tests. | engine hardening, anti-entropy validation |
| **9. Hardening** | Compaction/GC, RESP3/HELLO, metrics, geo pub/sub. | ops & polish |

### Testing strategy
- **Unit:** per-command behavior against a single in-mem engine (reuse redka's
  command test vectors where copyable).
- **Convergence:** spin up N in-process replicas sharing an in-mem broadcaster;
  apply concurrent/conflicting op sequences; partition & heal; **assert all
  replicas converge to identical state** and that Tier-1 commands match expected
  CRDT outcomes (e.g. add-wins for sets, sum-correct for counters).
- **Compatibility:** drive with `redis-cli` / a Redis client lib; diff against
  real Redis for single-node behavior on the supported subset.
- **Soak:** tombstone growth & compaction under churn.

---

## 10. Key decisions locked in this doc
- Reuse via **forking** redka's `internal` command layer (can't import it).
- Decompose every type into **flat per-element keys**; lean on prefix scans.
- **Sets → native OR-Set; hashes → LWW-Map; counters → per-replica component
  PN-counter** (the three correctness pillars).
- **All single-value registers use per-replica HLC-LWW slots from v1** (§6.7) —
  true last-writer-wins (incl. deletes), bounded by replica count, never
  exercising the store's height-wins path. *(decided)*
- **Counters and plain strings are distinct flavors and may not mix** (§6.4) —
  makes pure counters fully correct; a documented Redis deviation. *(decided)*
- **All 16 logical DBs** via the `{db}` key segment (§6.11). *(decided)*
- TTL via **meta scalar + lazy filter + sweeper**.
- MULTI via **go-ds-crdt Batch** (atomic propagation, no isolation/rollback);
  no WATCH.
- Backing store **Pebble**; replication via **libp2p + IPFS-Lite + gossipsub**.

## 11. Open decisions (still need a call before/while building)
1. **Compaction strategy** — named-DAG partitioning vs. scheduled global purge.
2. **SCAN cursor** — opaque-last-key contract acceptable for target clients?
3. **Module path / project name** and whether to vendor go-ds-crdt via the
   `redis-geo` fork or track upstream `ipfs/go-ds-crdt`.
