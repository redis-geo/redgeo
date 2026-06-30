# Testing redgeo

## Run everything

```sh
go test ./...
go vet ./...
gofmt -l .        # should print nothing
```

All tests are hermetic (in-memory engine, in-process replicas, ephemeral TCP
ports) — no external services, no network, no Redis install required.

## Where the tests live

| Package | What it covers |
|---|---|
| `hlc` | Hybrid logical clock ordering, codec, monotonicity, `Observe` |
| `crdtstore` | CRDT encodings, key codec, TTL sweeper, counters, watermark, compaction, and **multi-replica convergence** |
| `server` | End-to-end RESP behavior per command family, MULTI/EXEC, pub/sub, SCAN pagination, RESP3 types |

### The interesting ones

- **Convergence / partition-heal** (`crdtstore/cluster_test.go`,
  `partition_test.go`) — spin up N in-process replicas over an in-memory gossip
  network, apply concurrent/partitioned writes, heal, and assert all replicas
  converge:
  - `TestCounterConvergence` — PN-counter increments on both sides of a
    partition sum correctly after heal.
  - `TestSetAddWins` — concurrent `SADD`/`SREM` resolves add-wins.
  - `TestRegisterLWWConverges` — concurrent `SET`s converge to one value.
  - `TestHashCounterConvergence`, `TestPartitionedClusterConvergence`.
- **TTL** (`crdtstore/expire_test.go`) — injects a frozen clock to test lazy
  expiry and the active sweeper deterministically (no sleeping).
- **Compaction** (`crdtstore/watermark_test.go`) — churns sets/deletes to build
  DAG height, then asserts rotation reclaims it while live keys survive.
- **RESP3 / pub/sub** (`server/resp3_test.go`, `resp3_pubsub_test.go`) — a
  `HELLO 3` connection gets map/double/null replies and push-framed pub/sub; a
  RESP2 connection gets the legacy encodings.

## Run a subset

```sh
go test ./crdtstore/...                       # backend + convergence
go test ./server/...                          # RESP end-to-end
go test ./crdtstore/... -run Convergence -v   # by name
go test ./... -race                           # race detector (recommended before pushing)
go test ./... -count=1                         # bypass the test cache
```

The convergence tests poll for convergence with a deadline (`eventually`), using
a short anti-entropy `RebroadcastInterval` (200ms) so a healed partition catches
up quickly. They typically finish in 1–3s.

## Manual testing against a running server

```sh
go run ./cmd/redgeo -addr :6380 &
redis-cli -p 6380 PING
redis-cli -3 -p 6380 HGETALL h     # -3 negotiates RESP3
```

When scripting raw RESP by hand, **send properly framed RESP arrays** rather
than piping many inline commands through `nc` — inline pipelining over `nc` is
unreliable (buffering / partial reads) and will look like corruption that isn't
real. Use `redis-cli`, a client library, or the Go test client
(`server/phase1_test.go`'s `dialC`/`readReply`) which frames RESP correctly.

## Two-node convergence by hand

```sh
go run ./cmd/redgeo -data ./a -addr :6380 -p2p -p2p-listen /ip4/0.0.0.0/tcp/4001 &
# copy A's printed /ip4/.../p2p/<peer-id>
go run ./cmd/redgeo -data ./b -addr :6381 -p2p -bootstrap <A-multiaddr> &

redis-cli -p 6380 SET k hello
sleep 1
redis-cli -p 6381 GET k        # -> "hello"
```

## Writing new tests

- Backend behavior: build a store with `newTestStore(t)` (in-memory engine,
  replica `r1`) and drive it through `store.Redka(db)`.
- Convergence: `newCluster(t, n)` (single DAG) or `newPartitionedCluster(t, n,
  parts)`; use `net.SetPartitioned(i, true/false)` to split/heal and
  `eventually(t, msg, cond)` to await convergence.
- End-to-end RESP: `startTestServer(t)` returns an address; `dialC(t, addr)`
  gives a client whose `do(t, args...)` returns a parsed `reply` (handles RESP2
  and RESP3 framing).
- Time-dependent logic (TTL/HLC): override the package var `nowMS` (crdtstore)
  or use `hlc.NewWithClock` to inject a deterministic clock — don't sleep.
