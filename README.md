# redgeo

An **active/active, geo-distributed, Redis-compatible** server in Go.

Multiple geographically distributed nodes can all accept **writes** at the same
time (multi-master) and converge to the same state without coordination, using
[CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
semantics under the hood. redgeo speaks the Redis wire protocol (RESP2 and
RESP3), so existing Redis clients and `redis-cli` work against it.

It is built on three pieces:

- **[go-ds-crdt](https://github.com/ipfs/go-ds-crdt)** — the CRDT storage layer
  (a Merkle-DAG Add-Wins OR-Set used as a `key → bytes` register).
- **[redcon](https://github.com/tidwall/redcon)** — the RESP wire protocol.
- **[redka](https://github.com/nalgeon/redka)** — the blueprint for the Redis
  command layer (forked, since its command code lives under `internal/`).

> Design and rationale: **[DESIGN.md](DESIGN.md)**.
> Working on the code: **[DEV.md](DEV.md)** · Testing: **[TEST.md](TEST.md)**.

## Status

Experimental. The full phased plan (DESIGN §9) is implemented — strings, hashes,
sets, sorted sets, lists, counters, TTL, keys/SCAN, MULTI/EXEC, pub/sub, libp2p
replication, and watermark-gated compaction — with documented residual
limitations in **DESIGN §12**.

## Quick start

Requires Go 1.25+. redgeo currently consumes two local forks via `replace`
directives, so check them out as siblings of this repo first (see
[DEV.md](DEV.md#fork-dependencies)):

```
github.com/redis-geo/
  redgeo/        # this repo
  go-ds-crdt/    # fork (branch: lazy-broadcast-batch-ch)
  redcon/        # fork (branch: resp3)
```

Build and run a single in-memory node:

```sh
go run ./cmd/redgeo            # listens on :6380, in-memory (ephemeral)
```

Talk to it with any Redis client:

```sh
redis-cli -p 6380
127.0.0.1:6380> SET hello world
OK
127.0.0.1:6380> GET hello
"world"
127.0.0.1:6380> SADD s a b c
(integer) 3
127.0.0.1:6380> INCR counter
(integer) 1
127.0.0.1:6380> HELLO 3            # negotiate RESP3
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:6380` | RESP listen address |
| `-data` | `""` | Data directory (empty = in-memory, ephemeral) |
| `-replica` | derived | Stable replica ID (persisted under `-data`) |
| `-p2p` | `false` | Enable the libp2p replication mesh (multi-node) |
| `-p2p-listen` | `/ip4/0.0.0.0/tcp/0` | libp2p listen multiaddr |
| `-bootstrap` | `""` | Comma-separated bootstrap peer multiaddrs |
| `-partitions` | `1` | Number of named partition DAGs (multi-node; `256` = one per bucket) |

### Persistent, single node

```sh
go run ./cmd/redgeo -data ./node-data -addr :6380
```

### Multi-node (active/active)

Start node A, note the multiaddr it logs, then point node B at it:

```sh
# node A
go run ./cmd/redgeo -data ./a -addr :6380 -p2p -p2p-listen /ip4/0.0.0.0/tcp/4001

# node B (bootstrap to A's printed /ip4/.../p2p/<peer-id>)
go run ./cmd/redgeo -data ./b -addr :6381 -p2p -bootstrap /ip4/127.0.0.1/tcp/4001/p2p/<peer-id>
```

Writes on either node replicate to the other and converge. Set `-partitions 256`
on all nodes for per-partition DAGs (DESIGN §5.5, §11) — this is immutable once
data exists, so decide it up front.

## Supported commands & semantics

redgeo implements the common string/hash/set/zset/list/key/server surface
(~95 commands). It is **honest about consistency**: each command falls into a
tier (DESIGN §6).

| Tier | Behavior | Commands |
|---|---|---|
| **1 — conflict-free correct** | Converges with intuitive HLC-LWW / OR-Set / counter semantics | strings, hashes, sets, dedicated counters, keys/SCAN, server |
| **2 — converges, weaker** | Eventually consistent; some ops race before convergence | sorted sets, lists, TTL, NX/XX existence checks |
| **3 — best-effort** | No isolation/rollback/CAS | MULTI/EXEC, `*STORE` |
| **out** | Not implemented | WATCH, scripting (EVAL), streams, geo, bitmaps, HLL, cluster |

Key semantic choices (see DESIGN §6 for the full list):

- **Strings / hash fields / zset scores** use per-replica HLC last-writer-wins
  slots — concurrent writes to the same key resolve to the latest wall-clock
  writer, deterministically, including deletes.
- **Sets** are native Add-Wins OR-Sets — concurrent `SADD x` / `SREM x`
  resolves to `x` present.
- **Counters** (`INCR`/`HINCRBY`/…) are CRDT PN-counters via per-replica
  components — concurrent increments **sum** correctly. A key is *either* a
  plain string *or* a counter; mixing them errors (a documented Redis
  deviation that makes counters fully correct under concurrency).
- **TTL** is lazy-filtered on read plus a background sweeper.
- **MULTI/EXEC** commits atomically as one CRDT delta with read-your-writes,
  but has no isolation or rollback (close to real Redis).

## Non-goals

Strong consistency, linearizability, `WATCH`/CAS, the Redis Cluster slot
protocol, scripting, Streams, and byte-for-byte behavior on every edge case.
redgeo targets the common subset with documented semantics.

## License

See the upstream projects' licenses. redgeo forks redka's command layer and the
go-ds-crdt / redcon storage and wire layers.
