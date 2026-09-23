# replay

Deterministic market data replay engine. Ingests historical tick/L2 data and serves it to N concurrent subscribers as a bit-for-bit reproducible event stream at 1× to 10,000× speed.

**Determinism is the product.** Every design decision defers to it. Throughput is second.

## Documentation

Concept and reference docs live in `docs/`. Start with [`docs/CLAUDE.md`](./docs/CLAUDE.md) for the index.

- Terminology and resolved ambiguities → `CONTEXT.md` (glossary only)
- Anything else (binary format spec, determinism guarantees, backpressure semantics, clock model) → a file under `docs/`

`BENCHMARKS.md` is a deliverable, not documentation. It is updated in the same commit as any change that moves a measured number.

When you make a change that affects a doc, update it in the same MR. Docs explain the **why** — the code shows the **what**.

## Tech Stack

- **Language**: Go 1.23+
- **Dependencies**: kept minimal. stdlib first. `google/go-cmp` for tests, `grpc-go` for the streaming API, `prometheus/client_golang` for metrics. The server module (this one) carries exactly these three chosen dependencies, plus `google.golang.org/protobuf`, which is a direct require only because `grpc-go`'s own code generator emits `protoreflect`/`protoimpl` imports into `api/replay.pb.go` — it is a consequence of choosing `grpc-go`, not a fourth independent choice. `cmd/convert` and `cmd/catalogue` are separate Go modules with their own dependencies — see `docs/no-database.md`.
- **Storage**: memory-mapped custom binary format (hot tier), Parquet (archive tier), SQLite (run catalogue and benchmark history only)
- **CI**: GitHub Actions — `-race`, `go vet`, determinism test, benchmark regression

## Project Structure

```
api/                   # replay.proto and the gRPC code generated from it
cmd/
  replayd/             # the server
  convert/             # Parquet → hot-tier format (separate Go module)
  catalogue/           # run manifest → SQLite ingestion (separate Go module)
  lint-determinism/    # the project's own determinism analyzer (separate Go module)
internal/
  store/               # mmap reader, writer, binary format
  merge/               # k-way merge, total ordering
  clock/               # Clock interface, SimClock
  fanout/              # subscriber management, backpressure, pacing
  book/                # orderbook snapshot + delta reconstruction
  allocgate/           # the zero-allocation benchmark gate
  synth/               # synthetic dataset generator, shared by the determinism suites
bench/                 # load harness, reproducible make targets
docs/                  # concept and reference docs, indexed by docs/CLAUDE.md
profiles/              # local .prof files only, never committed -- see BENCHMARKS.md
testdata/
```

Four Go modules, listed in `go.work`: the root module, plus `cmd/convert`,
`cmd/catalogue` and `cmd/lint-determinism`, each with its own dependencies.

## Common Commands

```bash
make test              # full suite with -race
make bench             # go test -bench -benchmem, writes bench/results/
make determinism       # replay at 1/4/16/64 workers, assert identical hash
make profile           # cpu + mem profile, writes profiles/
make lint              # go vet + staticcheck + cmd/lint-determinism
make fuzz              # fuzz the binary decoder
```

## Testing

Table-driven tests by default. Name subtests for the condition, not the mechanism: `"blocks_when_slowest_subscriber_lags"`, not `"test_channel_full"`. Separate arrange/act from assert with a blank line.

Three test tiers, all in `make test`:

1. **Unit** — pure functions, format encode/decode, merge ordering
2. **Determinism** — `TestDeterminism` replays the same dataset at 1, 4, 16 and 64 workers and asserts an identical output hash, at both the merged stream and every Block subscriber. Also runs a chaos variant that injects random scheduling delays into reader goroutines, and a seek-suffix variant that asserts `replay(from=T)` is an exact suffix of `replay(from=0)`. **This test is the project's core assertion. It must never be skipped, weakened, or made tolerant.**
3. **Allocation** — benchmarks assert zero allocations and zero allocated bytes on the hot path, through `internal/allocgate`. It measures with `testing.AllocsPerRun` and its own `MemStats` byte pass, never with a nested `testing.Benchmark` call: that deadlocks on the `testing` package's own mutex, and a float64 average cannot round a genuine allocation down to zero the way `AllocsPerOp()`'s integer division can — see the package's doc comment. A change that introduces an allocation there fails CI.

Fuzz target on the binary decoder: `make fuzz`.

## Code Guidelines

Do not add code that isn't used.

Concrete types on the hot path. No `interface{}`, no boxing, no reflection in the merge or fan-out loops — the allocation benchmarks will catch it, but don't write it in the first place.

Errors are returned, not logged-and-swallowed. The only place that decides what to do with an error is `cmd/`.

### Comments and docstrings

Default to no comments. Exported identifiers get a one-line doc comment in Go convention. Add an inline comment only for a non-obvious constraint, a workaround for a specific bug, or behaviour that would surprise a reader — and in this repo that mostly means *documenting why something is required for determinism*, which is the one category of comment worth writing here.

Keep doc comments to one to three sentences: what it does, how it fails. Design rationale belongs in `docs/`, linked.

**Don't encode facts that live elsewhere.** Don't restate the format spec or the backpressure semantics in a comment; link to `docs/format.md` or `docs/backpressure.md`.

**Match the comment density of the surrounding code.**

---

## Project invariants

Violating any of these breaks the only claim this project makes.

**Nothing in an output-affecting path may depend on Go's nondeterminism.**
- Never `range` over a map where iteration order can influence output. Sort keys, or use a slice
- Never `select` over multiple ready channels in a path that determines event ordering — the runtime picks pseudo-randomly
- No goroutine start order assumptions
- No wall-clock reads
- If you need a set, and it's on an ordered path, it's a sorted slice

**`time.Now()` appears exactly once in the codebase**, inside `clock.RealClock`. Everything else takes a `Clock`. This includes tests, timeouts, retries, and metrics timestamps on the replay path. A grep for `time.Now` outside `internal/clock` is a CI failure. This rule applies to first-party code in this repository — `grpc-go`, `prometheus/client_golang`, and the Go runtime itself call `time.Now` internally and are out of this repository's control; see `docs/determinism.md`.

**Total ordering is `(exchange_ts, venue_id, sequence_number, instrument_id)`.** Ties are never broken by arrival order, goroutine identity, or anything else observable at runtime. `instrument_id` is the final tie-break, and it is part of the key because one venue can emit the same sequence number for two instruments at the same timestamp — the first three fields alone are not unique. If two events genuinely collide on all four, the format is wrong and that's a data problem to fix upstream.

**Zero allocations on the hot path in steady state.** Enforced by benchmark assertion. `sync.Pool` for reusable buffers, struct-of-arrays for batches, decode in place from the mmap. Scoped to the in-process path, up to the fan-out boundary — gRPC's own codec and transport allocate regardless.

**Never add a database.** No Postgres, no Redis, no Mongo, no vector store. SQLite for the run catalogue is the only persistence beyond the data files, and it is not touched during a replay. `replayd` writes a run manifest as a plain file; `cmd/catalogue` reads manifests as a separate, later, offline step and is the only thing that ever opens SQLite. Introducing a network hop into a nanosecond-scale hot path is not a trade-off to weigh — it's out of scope. See `docs/no-database.md`.

**No optimization without a profile first, and no optimization without a measurement after.** The sequence is: profile → hypothesise → change → measure → write it up in `BENCHMARKS.md`. A performance change with no committed before/after numbers gets reverted, however obviously correct it looks. Optimizing based on intuition is the specific failure mode this rule exists to prevent.

**Backpressure mode is explicit at subscribe time.** `Block` or `Drop`, never a default that silently picks one. `Drop` increments a gap counter the subscriber can read; a dropped event that nobody can detect is a correctness bug. See `docs/backpressure.md` for exactly how gaps are made undetectable-drops impossible by construction.

**`-race` stays clean.** Not "mostly clean". Any new concurrency needs a race-detector run before review.

## Common failure modes here

- Making `TestDeterminism` pass by reducing worker counts or hashing less of the stream
- Adding a `context.WithTimeout` on the replay path that reads real time
- Introducing a map into the merge for "clarity"
- Optimizing the merge without checking whether the bottleneck moved to fan-out
- Adding a dependency to solve something stdlib already does

---

## Behavioural guidelines

Specific to this repo: if a change touches ordering, concurrency, or the clock, say out loud how it preserves determinism before writing it.

In this repo the verification step is almost always one of `make determinism`, `make test` (with `-race`), or `make bench` with a committed before/after. If a task can't be tied to one of those, say so before starting.
