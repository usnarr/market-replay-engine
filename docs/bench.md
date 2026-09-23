# Benchmarks, the allocation gate, and the profile-first rule

Scope: why `internal/allocgate` measures the way it does, why bytes are asserted separately from count, why the gate never runs under `-race`, the profile-first rule and the failure mode it exists to prevent, and `BENCHMARKS.md`'s append-only protocol.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## What the gate claims, and where it stops

**Zero allocations on the hot path in steady state**, enforced by benchmark assertion rather than by review.

The claim is scoped to the in-process path **up to the fan-out boundary**. gRPC's own codec, compressor and transport allocate regardless of anything this project does, so a claim that covered them would be a claim about grpc-go. `internal/store`, `internal/merge` and `internal/fanout` carry the gate on their per-record paths. `internal/book` sits outside the boundary and carries no gate — see [`book.md`](./book.md) for why that is a scoping decision rather than an omission, and `BENCHMARKS.md`'s end-to-end section for the same reasoning applied to the load harness.

## Why not the obvious implementation

The gate was first designed as a wrapper that ran the code under test inside its own nested `testing.Benchmark` call, so it could assert the raw `MemAllocs` and `MemBytes` counts directly:

```go
res := testing.Benchmark(func(b *testing.B) { ... })   // deadlocks
```

**That design deadlocks, and it was caught by running it rather than by reasoning about it.** `testing.(*B).runN` serializes every benchmark run in a process through one package-level, non-reentrant mutex. A benchmark that calls `testing.Benchmark` from inside its own already-running measurement tries to take that same mutex a second time on the same call stack, and waits for itself forever.

The shipped gate uses `testing.AllocsPerRun`, which has nothing to do with the benchmark-timing machinery and so never touches that mutex.

## Why a float64 average is not the weakness it looks like

`AllocsPerRun` returns a `float64` average rather than a raw count, which looks like a step backwards: the whole point of the nested-benchmark design was to avoid a *divided* value.

The rounding hazard is real, but it belongs to integer division, not to averaging. `AllocsPerOp()` divides an integer total by an integer iteration count, so one genuine allocation across a large `b.N` rounds to a reported zero. A `float64` average over a fixed number of runs cannot: one allocation in 200 runs is `0.005`, which is not `0.0`, and the gate compares against exactly zero.

So the shipped gate gets the same protection the nested design was reaching for, for a different reason, without the deadlock.

## Why bytes are a second, separate pass

The invariant names both a count and a byte total, and `AllocsPerRun` reports only a count. `bytesPerRun` is therefore a second pass over the same function, shaped like `AllocsPerRun` — warm up once, read `runtime.MemStats`, run N times, read again — with `TotalAlloc` in place of `Mallocs`.

It is genuinely a second pass rather than a wider reading of the first, for two reasons:

1. `AllocsPerRun` exposes nothing to read bytes from.
2. `AllocsPerRun` **pins `GOMAXPROCS` to 1** for its own measurement. An allocation that appears only when two goroutines really run at once — a contended pool miss, a retry path that boxes — never shows up in its count. The byte pass leaves `GOMAXPROCS` alone, so the code runs under the benchmark's real parallelism.

There is deliberately **no second GC before the final reading**. A collection there could run a finalizer inside the measured window and charge its allocation to the code under test, and `ReadMemStats` already flushes each P's allocation cache, so a second GC would buy nothing and add noise.

### The exposure this accepts

`TotalAlloc` counts the whole process, so another goroutine allocating during the window is charged to the function under test. Every call site calls the gate with no other goroutine of its own running, and `AllocsPerRun`'s own `Mallocs` reading has carried exactly the same exposure since the gate was written.

This exposure is not theoretical, and it has one known trigger: **`-cpuprofile`**. The CPU profiler's sampling goroutine allocates inside the measured window, which charges a few bytes to the code under test. Measured on this project: every `-cpuprofile` run of `internal/fanout`'s benchmarks fails `BenchmarkRingWrite/no_blob` with 24 to 64 bytes over 200 runs, while the same run under `-memprofile` alone passes every time. `make bench` passes no profiling flag, so the gate is unaffected where it is enforced; `make profile` checks that each profile file was written instead of checking the benchmark's exit status, and its own comment explains the consequence for reading a committed CPU profile.

## Why the gate skips under `-race`

The race detector's own instrumentation allocates. A gated benchmark under `-race` would fail regardless of the code under test, so the gate skips outright rather than reporting a result that means nothing.

This costs nothing in coverage, because of how the targets divide the work:

- `make test` runs with `-race` and passes no `-bench` flag, so it never executes a benchmark body at all.
- `make bench` runs without `-race`, and is what actually enforces the gate.

The skip exists for the developer who runs `go test -race -bench=.` by hand.

## How to call it

The gate measures a function that must already be warm: build every input, output buffer and object under test **before** calling it, and write results to a package-level concrete sink so the compiler cannot prove the work is dead.

It does not touch the benchmark's own timer or iteration count. `AllocsPerRun` runs a fixed number of iterations independently of `b.N`, so the gate costs the same whether `b.N` is 1 or a million — which is why it can sit beside the throughput loop rather than replacing it.

## No optimization without a profile first, and a measurement after

The sequence is always: **profile → hypothesise → change → measure → write it up in `BENCHMARKS.md`.**

A performance change with no committed before/after numbers gets reverted, however obviously correct it looks. The specific failure mode this exists to prevent is **optimizing based on intuition** — changing code because it looks slow, rather than because a profile says it is, and then having no way to tell whether the change helped.

The rule applies to this project's own development, not only to a future contributor. Where a plan file already argued a design out (the loser tree over a heap, the shared ring over a channel per subscriber), that argument stands on its own and needs no profile. Any *further* tuning beyond it goes through the full sequence.

A change that is required functionality rather than speculative tuning does not need the profile half — it was going to be built either way — but it still takes the measurement half. `BENCHMARKS.md`'s release-batching row is an example, and says so in place.

`make profile` writes a CPU and a memory profile per hot-path package. Nothing under `profiles/` is committed: `go tool pprof -svg`'s own rendered output includes a "Build ID" line naming the compiled test binary's path under the local machine's temp directory, which runs through the invoking user's own home directory by construction — not a source-path leak `-trimpath` reaches, since it names where the binary was built, not where its source lives. Read a `.prof` locally with `go tool pprof` instead of publishing a rendering of it.

## `BENCHMARKS.md` is a deliverable, not documentation

It is updated **in the same commit** as any change that moves a measured number, never as a follow-up. A follow-up commit means there was a window in which the repository's own numbers were wrong, which defeats keeping them next to the code.

Its structure is one table per named benchmark — merge throughput, fan-out throughput, encode/decode throughput, pacing accuracy, end-to-end replay rate — with one row per committed change:

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|

Each table is an **append-only history, never a current-state snapshot.** A row is added, not overwritten. That is the whole point: a regression stays visible in the context of what came before it, instead of being silently replaced by the number that replaced it.

Every section states the machine its numbers came from. Numbers from a different machine are not comparable, which is also why no CI baseline is committed from a developer's machine.

## The load harness

`bench/` holds the end-to-end load harness: `bench.Run` assembles the whole in-process stack over a dataset directory — store readers, one cursor per venue, a merger, a ring, and a configured subscriber mix — and reports what every subscriber received, so a caller can check the stream as well as time it.

It is a plain library and imports no testing package, because the harness is the thing being measured. It is on the linter's ordered path: it chooses the order venues enter the merge tree and the order subscribers register, and both are output-affecting for the stream it measures. Both orders come from file content — venue id, then the first record's key — matching the rule `cmd/replayd` follows for the same reason.
