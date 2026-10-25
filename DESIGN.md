# redgeo — Design & Implementation Plan

An **active/active, geo-distributed, Redis-compatible** server in Go, backed by
[`go-ds-crdt`](https://github.com/ipfs/go-ds-crdt) as the storage layer,
[`redcon`](https://github.com/tidwall/redcon) for the RESP wire protocol, and
[`redka`](https://github.com/nalgeon/redka) as the blueprint for command
implementation.

> Module: **`github.com/redis-geo/redgeo`** (decided).

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
- Redis Cluster slot protocol. (redka
  doesn't have these either.) HyperLogLog, bitmaps and geo were added later:
  HLL as per-replica max-merged sketches, bitmaps as per-chunk LWW slots (a
  concurrent flip inside the same 512-byte chunk can be lost), geo as a zset of
  geohash scores.
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

Notation: `{P}` = the **partition bucket** = `bucket(db, key)`, a fixed-width
hash (e.g. 8-bit → 256 buckets) reserved as the leading path segment for
compaction (§5.5 / §8). `{db}` = logical DB index (Redis `SELECT`, default 0).
`{key}` = the Redis key, percent/length-escaped so `/` in user keys can't break
the path (see §5.4). `{field}`/`{member}` likewise escaped. All sub-keys of one
logical Redis key share the same `{P}` (it's a pure function of `db`+`key`).

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
| Key metadata | `/{P}/m/{db}/{key}/{replicaID}` | LWW slot: `(hlc, tag, KeyMeta)` (§5.3) |
| String value | `/{P}/d/{db}/{key}/v/{replicaID}` | LWW slot: `(hlc, tag, bytes)` |
| Counter component | `/{P}/d/{db}/{key}/c/{replicaID}` | int64/float64 — single-writer per key |
| Hash field | `/{P}/d/{db}/{key}/h/{field}/{replicaID}` | LWW slot: `(hlc, tag, bytes)` |
| Set member | `/{P}/d/{db}/{key}/e/{member}` | `∅` (presence only) |
| ZSet member→score | `/{P}/d/{db}/{key}/z/{member}/{replicaID}` | LWW slot: `(hlc, tag, score)` |
| List element | `/{P}/d/{db}/{key}/l/{posKey}` | raw bytes (element) |

Every write touching logical key `(db, key)` is also published into the **named
DAG `p{P}`** (§5.5), so each partition's causal history is independent and can be
compacted on its own.

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

### 5.5 Partitioning & compaction (decided)
go-ds-crdt **never GC's** anything: every write/delete is a permanent block in
the delta-DAG, and deletes leave permanent tombstones. The only reclamation
primitive is `PurgeDAG(dagName)`, which nukes an **entire** named DAG — heads,
blocks, set entries, processed markers, *including live data*. So compaction is
really **DAG rotation**: snapshot the current live key→value state, write it as
the genesis of a fresh DAG, cut reads/writes over, then `PurgeDAG` the old DAG.

**The hard constraint (applies to any strategy): causal stability.** Tombstones
are load-bearing — a tombstone is what keeps a deleted key deleted. Purge a
tombstone while *any* replica still holds the matching add un-synced (lagging or
partitioned) and anti-entropy will **resurrect the deleted key** on reconnect.
So before purging you must establish a **stability watermark**: a cut every
replica has provably synced past. Our HLC slots soften the register case (a
resurrected stale slot loses on HLC), but set membership and the DAG blocks
themselves still require the watermark. Building this watermark (track per-
replica sync progress → derive a safe cut) is a prerequisite for *any*
compaction and is easy to get subtly wrong — build it early.

**Decision — partition-ready encoding now; global purge v1; rolling named-DAG
compaction later:**

1. **Reserve `{P}` from day one (cheap insurance).** Keys lead with a partition
   bucket `{P} = bucket(db, key)`, fixed at **256 buckets** (decided — see §11;
   `NumWorkers=1` per partition DAG), and each partition's writes go to named
   DAG `p{P}`. This costs nothing now and avoids a full key-rewrite when we turn
   on partitioned compaction. Bucket count is effectively immutable once data
   exists, so it was picked up front (idle cost of 256 DAGs measured — §11).
2. **v1 = scheduled global purge.** Simplest correct option: in a maintenance
   window, confirm the global stability watermark, snapshot live state, rotate
   all DAGs, purge. Fine while the dataset is small; preserves cross-key
   atomicity. Downside: cluster-wide write stall, cost grows with data size.
3. **Target = rolling per-partition rotation.** Compact one `p{P}` at a time:
   only that partition's keys quiesce briefly, the rest of the keyspace keeps
   serving, storage stays bounded continuously, and a botched rotation damages
   one partition not the whole store. Same rotate-and-purge primitive, scoped
   per DAG, gated on a per-partition watermark.

Trade-offs (full comparison): partitioning costs more machinery (partition
router, per-partition watermarks, and cross-partition `MULTI`/`RENAME`/`*STORE`
lose single-DAG atomicity) and risks hot-bucket skew, but it's the only option
compatible with always-on geo at scale. Global purge is simpler and keeps
atomicity but stalls the whole cluster and doesn't scale with dataset size — so
it's the v1 stepping-stone, not the destination.

---

## 6. Per-feature CRDT design & semantics

> Key paths below omit the leading `/{P}/` partition segment (§5.5) for brevity;
> every key is implicitly under its partition bucket and published to DAG `p{P}`.

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
- `KEYS pattern` / `SCAN cursor [MATCH] [COUNT]`: meta keys live under
  `/{P}/m/{db}/`, so a db-wide scan iterates all `{P}` buckets (256 prefix scans,
  or one full scan filtered by `db`). `MATCH` applied in app, `COUNT` is a hint.
  Ordering is naive — fine for scan.
- **Cursor = numeric handle + per-node resume table (decided).** The wire cursor
  is a **decimal `uint64` string** (with `0` = start *and* end sentinel), because
  major clients parse it numerically — go-redis (`strconv.ParseUint`), redis-py
  (`int(cursor)`) — so a raw opaque blob would break them. On `SCAN 0` the node
  allocates a non-zero `uint64` token, scans from the start, fills `COUNT`, and
  stores the resume position `(P, last-key-within-P)` in an in-memory,
  idle-TTL'd map keyed by the token; subsequent `SCAN <token>` resumes exactly
  from `(P, last-key)`. Exact last-key resume gives Redis's guarantee (a key
  present for the whole scan is returned ≥1×) and is robust to concurrent
  mutation. The resume table is **per node**, shared across that node's
  connections (clients pin a pool to one endpoint, so this matches how Redis
  cursors are used). A cursor presented to a *different* node (rare mid-scan
  failover) doesn't resolve → return an error / restart; document. `HSCAN`/
  `SSCAN`/`ZSCAN` use the same mechanism scoped to one key's element prefix.
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
- **Geo pub/sub (implemented, §12.5):** with `-p2p`, publish across replicas
  over a dedicated libp2p gossipsub topic (separate from the CRDT data topics).
  `PutHook`-driven keyspace notifications remain a possible follow-up.

### Coverage summary
| Tier | Behavior | Commands |
|---|---|---|
| **1 — conflict-free correct** | Converges with intuitive HLC-LWW / OR-Set / counter semantics | strings, hashes, sets, counters (dedicated), keys/scan, server |
| **2 — converges, weaker semantics** | Eventually consistent; some ops race | zsets, lists, TTL, NX/XX existence checks |
| **3 — best-effort / degraded** | No isolation/rollback/CAS | MULTI/EXEC, `*STORE` |
| **out** | Not implemented | WATCH, RediSearch/TimeSeries/Bloom, cluster |

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

### 7.1 Consuming the go-ds-crdt fork
**Decision: depend on the `redis-geo` fork of go-ds-crdt**, not upstream
`ipfs/go-ds-crdt`. This lets us patch the storage layer for redgeo's needs
(e.g. compaction hooks, custom `Delta`/`MerkleCRDT` factories, named-DAG
partitioning) without waiting on upstream.

Important wrinkle: the fork's repo is `github.com/redis-geo/go-ds-crdt` **but its
`go.mod` still declares `module github.com/ipfs/go-ds-crdt`**. So we keep
importing it under the canonical path and redirect with a `replace` in redgeo's
`go.mod`:

```go.mod
require github.com/ipfs/go-ds-crdt v0.6.8

// Local development (everything is under GOPATH/src here):
replace github.com/ipfs/go-ds-crdt => ../go-ds-crdt

// Reproducible / CI builds — pin to a fork commit instead:
// replace github.com/ipfs/go-ds-crdt => github.com/redis-geo/go-ds-crdt <commit-or-tag>
```

Code imports stay `import "github.com/ipfs/go-ds-crdt"` unchanged; only the
`replace` selects the fork. If we later diverge enough to want our own import
path, we'd rename the module in the fork (`module github.com/redis-geo/go-ds-crdt`)
and drop the `replace` — but that also means rewriting the fork's internal
imports, so we defer it. Decision: **`replace` to the fork; local path for dev,
pinned fork commit for CI.** The fork should be tagged so CI builds are
reproducible.

---

## 8. Hard problems / risks (tracked explicitly)

1. **Tombstone / DAG growth.** Deletes/expiry/overwrites grow the store forever;
   `PurgeDAG` is the only reclamation and it rotates an entire DAG. **Resolved
   strategy (§5.5):** partition-ready `{P}` encoding now → global purge in v1 →
   rolling per-partition DAG rotation later, all gated on a **causal-stability
   watermark** (the genuinely tricky sub-component — build & test it early).
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
| **9. Hardening** | Causal-stability watermark + global-purge compaction (rolling per-partition rotation deferred), RESP3/HELLO, metrics, geo pub/sub. | ops & polish |

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
- **Consume go-ds-crdt via the `redis-geo` fork**, not upstream (§7.1). *(decided)*
- **Compaction:** partition-ready `{P}` key encoding now → **global purge v1** →
  **rolling per-partition named-DAG rotation** later, gated on a causal-stability
  watermark (§5.5). *(decided)*
- **Module path** `github.com/redis-geo/redgeo`. *(decided)*
- **SCAN cursor:** numeric `uint64` wire cursor backed by a per-node TTL'd
  resume table mapping token → `(P, last-key)` (§6.9). *(decided)*

## 11. Open decisions (still need a call before/while building)

*(none currently — partition bucket count resolved below)*

### Resolved — partition bucket count = 256 (2026-06-30, measured)

Locked at **256 buckets, `NumWorkers = 1` per partition DAG.** Effectively
immutable once data exists (changing it rehashes every key to a different
partition), so it was settled before any code lands.

Decision was gated on the idle per-DAG cost of running 256 named DAGs (one
`crdt.Datastore` per partition) on a node. Probe (`dagprobe/`, shared backing
store, no-op broadcaster, idle hold) measured:

- **Goroutines:** 6/DAG at `NumWorkers=1` (10/DAG at the default 5). 256
  partitions = ~1536 parked goroutines — cheap; the runtime handles it fine.
- **Heap (unpatched go-ds-crdt):** ~2.0 MiB/DAG → **~500 MiB** for 256 idle
  partitions. That was the only real obstacle to 256.
- **Root cause:** ~99% of that heap is a hardcoded `make(chan
  broadcastBatchHead, 32000)` (~2 MiB) allocated per `Datastore`,
  **unconditionally** — but only ever read/written when `BroadcastBatchDelay >
  0` (off by default). Under redgeo's default config it is allocated-but-never-
  touched dead weight.
- **Fix landed in the redis-geo fork:** allocate that channel only when
  `BroadcastBatchDelay > 0`. Full go-ds-crdt suite + batching test pass.
  Post-patch idle cost: **~6.6 KiB/DAG → ~1.6 MiB total** for 256 partitions.

Net: with the fork patch, 256 partitions cost ~1.6 MiB + ~1536 goroutines idle
per node — negligible — so the rolling-rotation granularity argument wins and
there's no reason to drop to 128/64. **This decision depends on the fork
carrying the `broadcastBatchCh` patch** (or `BroadcastBatchDelay` staying 0);
if redgeo ever enables broadcast batching, revisit the channel size, since each
partition would then allocate the full buffer again.

---

## 12. Implementation status & residual limitations

**Status (2026-06-30):** phases 0–9 of §9 are implemented, plus all six items
that were initially deferred (hash-field PN-counters, resumable SCAN cursor,
Batch-backed MULTI, RESP3 scalar types, global-purge compaction, per-partition
named DAGs). Build + vet + tests are green; the binary is exercised end-to-end
over RESP. The packages are `core`, `parser`, `redisapi`, `restypes`, `hlc`,
`engine`, `crdtstore`, `command/*`, `server`, `cmd/redgeo`.

The items below are **residual limitations** — deliberate trade-offs or
honestly-incomplete edges that remain after that work. They are documented here
so they are not mistaken for bugs or for "done."

### 12.1 Cross-partition `MULTI`/`EXEC` is not atomic across partitions
`EXEC` commits its queued writes through one batch with a read-your-writes
overlay (§6.10). With per-partition named DAGs (§5.5), that batch fans out to a
sub-batch **per partition** — atomic *within* each partition's DAG, but **not**
across partitions: a transaction touching keys in two partitions lands as two
deltas. This is the cross-partition atomicity loss already called out in §5.5;
single-DAG deployments (`NumPartitions <= 1`) keep full per-transaction
atomicity. `MULTI` also has no isolation or rollback (Redis-compatible).

### 12.2 Compaction: cluster-coordinated rotation *(resolved)*
go-ds-crdt never garbage-collects, so reclaiming tombstones means rotating a
partition to a fresh generation seeded with only its live state (§5.5). In a
cluster that is safe only if, at the instant of the cut, **every replica holds
the same blocks and nobody mints new deltas**; otherwise a lagging replica's
un-seen adds resurrect deleted keys against the purged tombstones. Package
`rotation` establishes that with a commit protocol over a control channel (the
`redgeo/ctl/v1` gossipsub topic, separate from data and pub/sub):

1. **Prepare.** The coordinator freezes writes on its partition and asks every
   known member to do the same (draining in-flight writes). It sends the union
   of all DAG heads seen; each member replies with its heads and which of those
   blocks it has *not processed*. The coordinator proceeds only after two
   consecutive full rounds with nothing missing anywhere and no new heads — not
   by comparing head sets, because go-ds-crdt's head bookkeeping is path
   dependent (equal state can carry different heads).
2. **Switch.** The coordinator stages generation g+1 seeded from its (now
   identical) live state and activates it; members activate an empty g+1 and wait
   for the seed to replicate to them. Every broadcast carries its generation, and
   a replica ignores other generations' traffic — otherwise one already on g+1
   would re-import the entire old DAG from a peer still on g.
3. **Done.** When every member is ready the coordinator announces done; all
   purge generation g and unfreeze. Generation g is kept until then, so any
   failure or timeout before done rolls every replica back with nothing lost.

Other properties:
- **All members must take part.** Membership is the replicated registry
  (`/00/r/{replica}`, written by `RegisterSelf` at startup) plus control-channel
  hellos, persisted locally. An unreachable member blocks compaction
  (`ErrMemberUnavailable`) instead of being left to resurrect old state; retire a
  replica for good with `REDGEO.FORGET`.
- **Writes pause, they are not lost.** While a partition is frozen its writes
  wait (up to 15 s, then `LOADING`); a `MULTI`/`EVAL` that straddles a rotation
  fails with `TRYAGAIN` and can be retried. Reads continue except for the instant
  between activating a seeded generation and the seed arriving. Rolling mode
  (`REDGEO.COMPACT ALL`, `-compact-every`) rotates partitions one at a time.
- **Crash safety.** A partition's generation, and a pending previous generation,
  are persisted outside the CRDT namespace; a node that restarts mid-rotation
  stays frozen and asks its peers (a peer past its generation means commit, one
  behind means roll back, every member also in doubt means roll back).
- **Limits.** A node that never registered and never announced itself is
  unknown to the protocol. A partitioned minority cannot compact. There is no
  automatic retry of a failed round; `-compact-every` simply tries again at the
  next interval, and only the lowest replica ID acts. The single-node watermark
  (`crdtstore.Watermark`) remains as a library gate; the cluster protocol does
  not use it.

Operate it with `REDGEO.COMPACT [partition|ALL]`, `REDGEO.MEMBERS` and
`REDGEO.FORGET <replica>`.

### 12.3 `HSCAN`/`SSCAN`/`ZSCAN` are paginated but O(n) per page *(resolved)*
Collection scans use the same per-node resume table as keyspace `SCAN` (§6.9):
the cursor token maps to the last member returned, with members ordered by name
(not score or insertion) so resume positions survive concurrent writes. `COUNT`
(default 10) bounds the matching members per page. Each page still reads the
whole collection, so cost per page is O(n) in the collection size — the reply is
bounded, the work is not. Cursors resolve only on the issuing node; an unknown
or expired cursor restarts from the beginning.

### 12.3a HyperLogLog and the Redis HLL format
`PFADD/PFCOUNT/PFMERGE` use Redis' own hash (MurmurHash64A, seed `0xadc83b19`),
register layout and Ertl estimator, so an element lands in the same register as
in Redis. Internally each replica stores its own sketch and reads max-merge them,
which is what makes concurrent `PFADD`s converge. At the string interface the key
is a Redis HLL: `GET` returns the merged sketch as a `HYLL` string, `STRLEN`/
`GETBIT`/`BITCOUNT` read it, and `SET` of a well-formed `HYLL` string stores an
HLL, so a sketch moves between redgeo and Redis with `GET`/`SET` and keeps working
with `PF*`. `SET` of anything else overwrites an HLL, as in Redis; `SETBIT` on one
is refused.

**Encodings.** `GET` returns the *sparse* encoding while the sketch qualifies
(every register ≤ 32 and the string ≤ 3000 bytes, Redis' `hll-sparse-max-bytes`
default) and the *dense* 12304-byte form otherwise; the cached cardinality is
written as "invalid" so Redis recomputes it. Redis stores the encoding as state
(sparse → dense is one-way); here "sparse" is a function of the register
contents, plus a per-key *dense pin* that `PFDEBUG TODENSE` and `SET` of a dense
value set (and `PFADD`/`RENAME` preserve). A sparse value restored with `SET` is
re-encoded canonically, so `GET` may return different — equivalent — bytes than
were stored (a dense one round-trips byte for byte).

**`PFDEBUG`** supports `GETREG` (all 16384 registers; like Redis it converts the
key to dense), `DECODE` (`z:N` ZERO, `Z:N` XZERO, `v:value,run` VAL), `ENCODING`
and `TODENSE`. **`PFSELFTEST`** runs the implementation against synthetic data —
estimate accuracy from 1 to 100000 elements, dense/sparse/component round trips,
merge commutativity/associativity/idempotence, re-add stability and hash spread —
and replies `OK` or an error naming the failed check; it touches no keys.
Differences: a restored value is merged into this replica's component, so
overwriting a sketch cluster-wide races like any other `SET`/`DEL` (§6.1), and
sketches written by the pre-0.1 build used a different hash and must be rebuilt.

### 12.3f Probabilistic sketches (Bloom, count-min, top-k)
All three keep **per-replica components that merge on read**, so concurrent
writers on different replicas never touch the same key and the result does not
depend on arrival order. Parameters are an HLC-LWW slot, and the data lives under
a path derived from a hash of them (`/s/h{hash}/…`): replicas that reserve
identical parameters — including the defaults used for auto-creation — share one
namespace and merge; if two replicas reserve *different* parameters
concurrently, the LWW winner's data is used everywhere and the loser's inserts
are ignored (never merged in as garbage). `DEL` removes every component;
`RENAME` collapses the merged state into the new key. `TYPE` reports the
RedisBloom names (`MBbloom--`, `CMSk-TYPE`, `TopK-TYPE`).

**Bloom filter** (`BF.RESERVE/ADD/MADD/EXISTS/MEXISTS/INSERT/INFO/CARD`) is
scalable like RedisBloom's: layer *i* has capacity `C·E^i` and error rate
`P·0.5^i`, so geometry follows from the parameters and all replicas agree. Bits
turn on only, so replicas merge by OR (4 KiB chunks, one key per replica per
chunk). An insert goes to the first layer that is not full; layer fill is the sum
of per-replica insert counts, which counts an item added concurrently on two
replicas twice — the filter may grow a little early, never late. Not
implemented: `BF.SCANDUMP`/`BF.LOADCHUNK` (the hash and layout are redgeo's own, so
dumps do not interoperate with RedisBloom).

**Count-min sketch** (`CMS.INITBYDIM/INITBYPROB/INCRBY/QUERY/MERGE/INFO`): a
depth×width matrix where each cell is a per-replica counter component
(`c/{cell}/{replica}`, only non-zero cells exist) and a cell's value is the sum
over replicas — concurrent increments on different replicas add up exactly as if
applied to one sketch, so estimates never undercount. Components are *signed* so
that `CMS.MERGE` can replace the destination with the weighted sum of its sources
by writing `own = target − (everyone else's)`; a merge therefore replicates like
any other write, and an increment racing a merge on another replica is kept on
top of the merged value. Weights may be negative (estimates are clamped at 0).
`INITBYPROB` uses `width = ⌈2/ε⌉`, `depth = ⌈log₂(1/δ)⌉`. Each `INCRBY` reads
and rewrites `depth` cells (one small key each), so it costs O(depth) queries.

### 12.3b Streams: IDs, existence, cost
Streams (`XADD/XLEN/XRANGE/XREVRANGE/XREAD/XDEL/XTRIM`) store each entry as a
per-replica HLC-LWW slot keyed by ID (`/d/{db}/{key}/x/{ms}-{seq}/{replica}`),
so concurrent `XADD`s never write the same key. Deviations and costs:
- **Auto IDs are `ms-seq` with a replica discriminator in the low 16 bits of
  `seq`** (`seq = counter<<16 | disc(replica)`), so two replicas minting an ID in
  the same millisecond cannot collide. IDs are strictly increasing and valid, but
  `seq` is not 0,1,2… as in Redis. Explicit IDs are checked against the locally
  known top ID only; two replicas adding the same explicit ID resolve LWW.
- Each replica keeps a max-merged last-ID register (`/xl/{replica}`); it makes
  `XADD` O(replicas) and defines existence, so a stream survives deleting all its
  entries (as in Redis) and IDs never go backwards.
- `XLEN`/`XRANGE`/`XREAD` scan the whole stream (O(n)); `~` trimming is exact.
- `XREAD BLOCK` polls the store (20 ms) and caps `BLOCK 0` at one minute.
- **Consumer groups** (`XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER`,
  `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO`) keep all
  state as per-replica slots under `/xg/{group}/`: existence, pending entries
  and consumers are HLC-LWW slots (`XACK`/`DESTROY` write deleted-tagged slots);
  the last-delivered ID is a max-merged `(epoch, id)` register, where
  `XGROUP SETID` bumps the epoch so the group can move backwards. **Delivery is
  at-least-once across replicas**: two regions that read `>` from the same
  last-delivered ID before they sync both hand out the same entries, and the later
  delivery wins the pending record. Within one node the key lock makes delivery
  exactly-once. `XACK` and pending state converge; consumers on different nodes
  should therefore be idempotent, or all readers of a group should use one
  region. Not tracked: `ENTRIESREAD`/lag, `XINFO STREAM FULL`, `XSETID`, and the
  `LASTID` option of `XCLAIM` (accepted, ignored). `XAUTOCLAIM` scans the whole
  pending list rather than bounding attempts.
- A stream with groups is renamed with them; `DEL` removes them.

### 12.3c Lua scripting
`EVAL`/`EVALSHA`/`EVAL_RO`/`SCRIPT LOAD|EXISTS|FLUSH` run scripts in a sandboxed
gopher-lua VM (`script/`, `command/script/`): base, table, string and math
libraries plus a small `cjson`; no `os`/`io`/`require`/`dofile`. `redis.call` /
`redis.pcall` dispatch through the normal command layer with Redis' Lua↔RESP
conversion rules. Semantics:
- Scripts run **only on the node that received them**; their effects replicate
  as ordinary CRDT writes. They are never replayed, so — unlike CRDB — no
  determinism restrictions apply (`TIME`, `math.random` are fine).
- Like `EXEC`, an `EVAL` runs on a transaction-bound store: its writes commit as
  one batch with read-your-writes, with **no isolation from other connections and
  no rollback** if the script errors partway (Redis also keeps partial effects).
  Under `MULTI` the script is queued and runs at `EXEC`.
- A script is aborted after 5 s (`Timeout`); there is no `SCRIPT KILL`
  (it replies `NOTBUSY`) and no `lua-time-limit` knob.
- The script cache is per node, as in Redis; clients recover from `NOSCRIPT` by
  reloading. `redis.call` rejects connection/transaction commands, `PUBLISH` and
  nested `EVAL`; there is no `SCRIPT DEBUG`, `FUNCTION`, `bit`, `struct` or
  `cmsgpack`.

### 12.3d JSON documents
`JSON.SET/GET/MGET/MSET/DEL/FORGET/TYPE/NUMINCRBY/NUMMULTBY/STRAPPEND/STRLEN/
ARRAPPEND/ARRINSERT/ARRPOP/ARRTRIM/ARRINDEX/ARRLEN/OBJKEYS/OBJLEN/TOGGLE/CLEAR`
implement a RedisJSON subset. A document is a **tree of nodes, each a
per-replica HLC-LWW slot keyed by its path** (`/d/{db}/{key}/j/{seg}/…/_/{replica}`;
a segment is `f`+name for an object field or `a`+posKey for an array element,
using the list's order-preserving position keys). Because different paths are
different keys, concurrent edits to *different* parts of one document merge;
edits to the *same* path are last-writer-wins by HLC, like any register.
Commands load the document into the pure `jsondoc` tree, mutate it under the key
lock, and the backend persists only the difference (changed/new nodes as
present slots, vanished nodes as deleted-tagged slots — so a replaced or deleted
subtree cannot reappear when the node is re-created).
- **Concurrent array edits** merge element-wise (each element has its own
  identity); index-based operations (`ARRPOP i`, `ARRINSERT i`, `ARRTRIM`) race
  before convergence, as list index ops do (§6.5).
- **Concurrent delete vs. edit inside the deleted subtree:** the delete wins the
  nodes it saw; a node the deleter never saw survives under a deleted parent and
  stays hidden while the parent is gone.
- **`NUMINCRBY`/`NUMMULTBY` are LWW read-modify-write**, not PN-counters:
  concurrent increments of one number on different replicas lose one. Use a
  counter key (`INCR`/`HINCRBY`) where increments must sum.
- **Paths:** `$`, `.name`, `['name']`, `[n]`, `[-n]`, `.*`, `[*]`, `..name`,
  `..*`, plus legacy `.a.b`/`a.b`. Not supported: filters `[?()]`, slices
  `[a:b]`, unions, `JSON.RESP`, `JSON.DEBUG`, `JSON.MERGE`, and `JSON.GET`
  formatting options (accepted, ignored). Object fields are returned sorted by
  name (insertion order is not kept); numbers are preserved verbatim.
- **Cost:** every command reads the whole document (O(size)); `TYPE` reports
  `ReJSON-RL`.

### 12.3e Security and observability
Client `AUTH` is a single shared password (`-requirepass`) compared in constant
time; there are no per-user ACLs, no command restrictions and no brute-force
throttling. TLS covers the RESP listener only. Cluster membership is **not**
access-controlled: any libp2p peer that can reach the gossip topics and holds
the cluster's topic names can join and read/write replicated data, so run the
mesh on a private network (or behind a libp2p private-network key) until peer
admission is added. Metrics are cheap counters (no keyspace scans); command
labels are capped at 512 distinct names, the rest folded into `other`.

### 12.4 RESP3 — fully supported via the forked redcon *(resolved)*
HELLO negotiates RESP2/RESP3; under proto 3 the connection emits native RESP3
types — null (`_`), map (`%`), double (`,`), boolean (`#`) — and pub/sub
delivers subscribe/unsubscribe confirmations and messages as **push frames**
(`>`). This is implemented in the **redis-geo fork of redcon** (`../redcon`,
consumed via a `replace`): the connection carries a negotiated `proto` (set by
`SetProto` on HELLO), the `Writer` gained proto-aware `WriteNull/WriteMap/
WriteDouble/WriteBool/WritePush`, and `PubSub` uses push frames when the
subscriber is RESP3. RESP2 clients are unaffected (every method falls back to
the RESP2 encoding). This supersedes the earlier "scalar-only / no push"
limitation. Like the go-ds-crdt fork, the redcon fork is a `replace`-to-local
dependency (§12.6) that must be pinned for CI.

### 12.5 Geo pub/sub is at-most-once *(resolved)*
With `-p2p`, `PUBLISH` is also broadcast on a dedicated gossipsub topic
(`redgeo/pubsub/v1`, separate from the CRDT data topics) and delivered to the
local `(P)SUBSCRIBE`rs of every other replica (`server/bus.go`). Semantics match
Redis pub/sub, not the CRDT store: delivery is **at-most-once** (nothing is
persisted, so a partitioned or restarted node misses messages) and unordered
across publishers, and `PUBLISH` replies with the **local** subscriber count
only. Each frame carries its origin replica ID so a node drops gossipsub's echo
of its own messages. Without `-p2p` pub/sub stays node-local.

### 12.6 Fork dependencies are not merged/pinned
redgeo consumes **two redis-geo forks** via `replace` to local paths for
development, so a fresh clone won't build until they are checked out as siblings
**or** the `replace`s are repinned to fork commits (the §7.1 "CI" form):
- **go-ds-crdt** (`../go-ds-crdt`, §7.1) — the 256-partition viability depends
  on its lazy-`broadcastBatchCh` patch (§11), on fork branch
  `lazy-broadcast-batch-ch`, **not merged to the fork `master`**.
- **redcon** (`../redcon`, §12.4) — native RESP3 support (proto-aware writers +
  pub/sub push frames). Needs the same pin-for-CI treatment.

This must be resolved before any reproducible/CI build.

### 12.7 Hash-field counter numeric type is float-backed
Hash-field PN-counter components are stored as decimal `float64` and summed as
float (§6.4), formatting integer-valued totals without a decimal point. A
`HINCRBY` performed after a `HINCRBYFLOAT` on the same field can therefore
truncate/observe float rounding rather than erroring as Redis would. String
counters keep distinct int/float flavors and don't have this edge.

### 12.8 Smaller documented edges (unchanged from earlier sections)
- **NX/XX/SETNX and `ZADD GT/LT/NX/XX`** existence checks are node-local only —
  concurrent cross-replica NX can both succeed (§6.1, §6.6).
- **List index ops** (`LSET i`, positional pops, `LTRIM`) race before
  convergence; lists are the weakest type (§6.5).
- **`len`/`DBSIZE`/`SCAN`** counts come from O(n) scans (§8.3).
- **Rotation broadcaster reuse:** during a partition's rotation swap the old and
  new datastore briefly share the same broadcaster; harmless for single-node /
  coordinated rotation, noted for the cluster-orchestration work (§12.2).
