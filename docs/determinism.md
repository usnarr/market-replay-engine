# Determinism guarantees

Scope: what "deterministic" means for this project, what is deliberately outside that claim, where the determinism boundary sits, and the first-party-only scoping of the `time.Now` invariant.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## The claim

For one dataset and one seek position, the canonical hash of the merged event stream is always the same value.

It does not depend on the merge worker count, on `GOMAXPROCS`, on goroutine scheduling, on ring capacity, on how many subscribers are attached or in which mode, on the replay speed, or on the machine and architecture the run executes on. The hash is the `canonical_v1` projection in [`format.md`](./format.md), which covers the record's fields and a snapshot's decoded levels, and deliberately omits where a snapshot happens to sit in a file.

Per subscriber, two further statements hold:

- A **Block** subscriber's received stream hashes to exactly the merged stream's own hash. Block promises no loss, and this hash is how that promise is checked rather than merely stated.
- A **Drop** subscriber's received records plus its reported gaps account for every record of the merged stream exactly once. An undetectable dropped event is impossible by construction; see [`backpressure.md`](./backpressure.md).

## What the claim does not cover

Four things are outside it, each on purpose:

1. **Delivery timing.** A `Clock` changes only *when* a record is delivered, never which records exist or in what order. See [`clock.md`](./clock.md).
2. **Which records a Drop subscriber misses under a real clock.** That follows from timing, so it follows real scheduling. The *accounting* above still holds exactly; the particular gaps do not repeat.
3. **Gap framing.** A gap lives in `Delivery`, never in the hash. If it were hashed, a seek would change the hash for a reason unrelated to content, and the seek-suffix property would break.
4. **Out-of-band subscriber stops.** Eviction by the stalled-Block-subscriber watchdog, and cancellation through `Ring.Unsubscribe`, both end one subscriber early. Neither changes any record's content, order or emit index, and every surviving subscriber's stream is identical to a run where neither happened.

`Live()` as a start position is the one `StartAt` this project never claims is reproducible: it means "wherever the write index is right now."

## What proves it

`make determinism` runs every test whose name starts with `TestDeterminism`, in `internal/merge` and `internal/fanout`. Between them those tests vary the worker count over 1, 4, 16 and 64, vary `GOMAXPROCS`, rotate which loser-tree leaf each venue lands on, vary the decode batch size from one record per batch to one batch larger than the dataset, vary the feed-channel depth down to the shallowest a venue can make progress with, vary ring capacity from 2 (constant lapping) to larger than the dataset (no lapping at all), vary the Block/Drop subscriber mix, inject random scheduling delays into both the reader goroutines and the subscriber goroutines (`TestDeterminismChaos`), and assert an identical hash at 1× and 10,000× under `SimClock` (`TestDeterminismPacing`, whose exact reach is spelled out in [`clock.md`](./clock.md)).

The seek-suffix property, that `replay(from=T)` is an exact suffix of `replay(from=0)`, is proved by `TestDeterminismSeekSuffix`, `TestDeterminismSeekSuffixWithTheWorkerPool` and `TestDeterminismSeekSuffixAcrossVenueOrder` in `internal/merge`. These were named `TestSeekSuffix*` until they were renamed to fit the `TestDeterminism…` prefix, which is what makes `make determinism`'s `-run TestDeterminism` pattern actually cover them, alongside `make test`.

This suite is the project's core assertion. It is never skipped, weakened or made tolerant.

## The determinism boundary is the in-process fan-out interface

The boundary is `internal/fanout.Subscriber.Next`. Everything up to and including it is inside the claim. Neither core suite opens a socket.

This is a deliberate line, not an oversight. gRPC and the transport stack below it introduce real sources of nondeterminism that this project does not control, and folding them into the core suite would do one of two harmful things: make "the determinism test" flaky for reasons that have nothing to do with merge or fan-out, or force the test to hash less of the stream so that it keeps passing. Both outcomes weaken the one claim this project makes.

The wire format is therefore **not** inside the determinism contract, and `16-open-questions.md`'s Q9 is answered no.

What covers the transport instead is a separate test, `TestIntegrationGRPCHashMatchesInProcess` in `cmd/replayd`. It starts a real gRPC server, connects a real client, and asserts that the canonical hash the client computes from the messages it received equals the in-process merged hash for the same dataset. It catches truncation, reordering, and codec or schema mistakes. Those are a different class of bug from a merge-ordering or fan-out-lapping bug: different causes, different fixes, and a different place to look. Its name deliberately does not begin with `TestDeterminism`, so `make determinism`'s `-run TestDeterminism` never sweeps it into the core suite.

Hashing there happens at **message** granularity, never at the wire or frame level. HTTP/2 `DATA` frame boundaries move with flow-control window state and are not a stable unit to hash.

## `time.Now` appears exactly once — in first-party code

The invariant in the root `CLAUDE.md` is scoped to this repository's own source. Stated precisely, there are two separate categories, and confusing them is the mistake this section exists to prevent.

### The one first-party exception

`internal/clock/realclock.go` is the only file in this repository allowed to call `time.Now` and its relatives. `cmd/lint-determinism`'s `no-time-now` rule enforces this by an explicit file-path suffix allowlist of exactly that one file, and flags `time.Now`, `time.Since`, `time.After`, `time.Tick`, `time.Sleep`, `time.NewTimer` and `time.NewTicker` everywhere else, test files included.

Every other package takes a `clock.Clock` and calls it. That is not a second exception: `RealClock` stays the one call site however many packages hold a value of it. `cmd/lint-determinism/suppress.go`'s `Allowlist` is empty, and adding an entry to it is a reviewed change, not a local decision.

### The three third-party exceptions, out of scope

These three dependencies read real time inside their own code. This repository cannot stop them, and the rule was never about them:

1. **`grpc-go`** — the HTTP/2 BDP estimator times its own ping round trips, and the transport reads time for keepalives and deadlines.
2. **`github.com/prometheus/client_golang`** — timestamps a scrape.
3. **The Go runtime and standard library** — the scheduler, timers, and the net package.

Dropping them is not an option that leaves the project better off. The runtime is not a choice at all, and the other two are approved dependencies precisely because each does something the standard library alone does not: the root `CLAUDE.md`'s "don't add a dependency to solve something stdlib already does" is not an argument for hand-rolling an HTTP/2 stack.

The reason this is a boundary and not a hole: none of these three can reach hashed content. Content and order are fully determined before the fan-out boundary above, by code this repository owns and the linter checks. What a third-party clock read can affect is delivery timing and out-of-band outcomes, and both of those are already outside the claim, stated above, for reasons that have nothing to do with gRPC.

### The one third-party read worth removing anyway

`grpc-go`'s BDP estimator is the exception worth acting on, because it sits closest to something that matters. Left to size the flow-control window dynamically, it would make the point at which a Block subscriber's backpressure engages depend on an estimator this project's `Clock` cannot see.

`cmd/replayd` therefore pins the window sizes explicitly at both ends — `grpc.InitialWindowSize`, `grpc.InitialConnWindowSize`, `grpc.ReadBufferSize`, `grpc.WriteBufferSize` on the server, and the matching dial options on the client. Passing an explicit initial window sets grpc-go's `StaticWindowSize`, which is what stops the estimator from being constructed at all. grpc-go ignores an initial window below 64 KiB, so the pinned value has to clear that threshold to mean anything; a test asserts it does.

Even pinned, `SendMsg` returning means "handed to the transport's write buffer", not "the remote peer read it". A Block subscriber over gRPC is backpressured against the ring, not against the client's own read rate.

## Where real time legitimately decides something inside this repository

Two places, both reading through `clock.Clock` rather than `time.Now`, and both outside the claim by design:

- **The stalled-Block-subscriber watchdog.** "No progress for a real duration" has no meaning under a simulated clock, so this genuinely needs real time. Which subscriber is evicted, and when, depends on real scheduling. It never changes what a surviving subscriber receives. A `SimClock`-driven determinism run leaves `WatchdogTimeout` at zero, which disables it. See [`backpressure.md`](./backpressure.md).
- **Release batching in the pacer.** At high speed, an intended inter-event delay falls below any scheduler's resolution, so records due within one measured window are released together. This changes timing only. See [`clock.md`](./clock.md).

The `pacing_slip` gauge and the pacer's own `Slip()` are observations of real time, never corrections applied to the schedule: the delivery schedule is anchored once and never re-anchored.
