# Benchmarks

This file is a deliverable, not documentation. It is updated in the same commit as any change that moves a measured number.

The sequence for any performance change is: profile → hypothesise → change → measure → write it up here. A performance change with no committed before/after numbers gets reverted, however obviously correct it looks.

Each table below is append-only history, one row per committed change, not a single current-state snapshot — a regression stays visible in context instead of being silently overwritten.

<!--
## <benchmark name>

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| YYYY-MM-DD | abc1234 | one-line description | ... | ... | profiles/<name>.svg |
-->

## Regression tolerance

CI's `bench` job runs `make bench BENCHCOUNT=6` and compares against a committed
`bench/baseline.txt` with `benchstat`, posting the comparison to the job summary.
This is **warn-only**: it never fails the build on a throughput regression, only on
the allocation gate (`make bench`'s own exit code, via `internal/allocgate.AssertZero`
— see this file's own per-section rows for which benchmarks are gated). A hard
throughput tolerance is not set yet; per this file's own rule, tune it empirically
once real CI-runner numbers exist, not by guessing a tight number up front and then
fighting CI noise instead of real regressions.

`bench/baseline.txt` is not committed yet. Every number in this file so far is from
a Windows development machine (see each section's own environment line), which is
not comparable to `ubuntu-latest`, where the CI job actually runs — committing a
Windows-sourced baseline for a Linux-runner comparison would compare the wrong
things and call it a regression. Establish it by taking one `bench-results` artifact
from a real `bench` job run and committing it as `bench/baseline.txt`.

## Merge throughput

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-21 | (this commit) | M7: first benchmarks for the loser tree, the comparator, and Merger.Next | — | see below | — |

```
BenchmarkLoserTreePop/k_2-8          8.156 ns/op   0 B/op   0 allocs/op
BenchmarkLoserTreePop/k_4-8         10.340 ns/op   0 B/op   0 allocs/op
BenchmarkLoserTreePop/k_8-8         11.970 ns/op   0 B/op   0 allocs/op
BenchmarkLoserTreePop/k_16-8        13.940 ns/op   0 B/op   0 allocs/op
BenchmarkLoserTreePop/k_64-8        18.780 ns/op   0 B/op   0 allocs/op
BenchmarkCompareKey-8                0.220 ns/op   0 B/op   0 allocs/op
BenchmarkMergerNext/workers_1-8     52.060 ns/op   0 B/op   0 allocs/op
BenchmarkMergerNext/workers_4-8     56.610 ns/op   0 B/op   0 allocs/op
BenchmarkMergerNext/workers_16-8    58.330 ns/op   0 B/op   0 allocs/op
```

`BenchmarkLoserTreePop` grows roughly with log2(k), matching the tree's own design
note (half a binary min-heap's comparisons per pop). `BenchmarkMergerNext` replays
`internal/synth`'s standard dataset end to end, rebuilding the merger outside the
timer whenever it exhausts, so this is steady-state per-record throughput, not one
dataset pass amortized over a much larger `b.N`; the small rise from 1 to 16 workers
is reader-goroutine coordination overhead on a dataset small enough that decode
itself is not the bottleneck at any worker count tested. `BenchmarkLoserTreePop` is
now gated with `internal/allocgate.AssertZero`; the other two report zero
allocations but are not gated, since neither is one of the specific targets
`plans/13-bench-and-profiles.md` names.

## Fan-out throughput

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-20 | (this commit) | M6: shared ring, atomic-word slot | — | see below | — |

```
BenchmarkRingWrite/no_blob-8               56.87 ns/op   0 B/op   0 allocs/op
BenchmarkRingWrite/with_a_snapshot_blob-8 100.20 ns/op   0 B/op   0 allocs/op
BenchmarkRingRead-8                        15.39 ns/op   0 B/op   0 allocs/op
```

The ring's slot payload is atomic words, not a plain struct: a Drop subscriber may
copy a slot the writer is concurrently overwriting, and a plain field there is a
genuine data race, not a false positive, under `go test -race`. The measured cost of
that choice is the ten sequentially-consistent stores `BenchmarkRingWrite/no_blob`
pays per record (`store.EncodeRecord` plus eight word stores plus the two `seq`
publishes) against a plain-struct assignment's one. Reads pay nothing extra on amd64:
`atomic.Uint64.Load` compiles to a plain `MOV`. See `docs/backpressure.md`.

Two profile-gated optimisations are deliberately not built yet, per the project's
profile-first rule: dropping to 7 slot words (wire bytes 56-63 of a `store.Record`
are always reserved zeros), and skipping the atomic path entirely when the
subscriber set holds no Drop subscriber (the Block barrier then already guarantees
exclusivity). Revisit only if a profile shows ring writes are the pipeline's
bottleneck.

`internal/allocgate.AssertZero` now gates `BenchmarkRingWrite/no_blob` and
`BenchmarkRingRead` — the two targets `plans/13-bench-and-profiles.md` names for
this package, "ring write and ring read including the lapping protocol" (`read` is
the same load-copy-reload path a lapped read takes). Verified the gate actually
catches a regression: a deliberately introduced allocation in the write path failed
the benchmark with the expected message, reverted before committing.

## Encode/decode throughput

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz. Numbers from a different machine are not comparable to these.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-19 | (this commit) | M2 baseline: record codec, reader access, blob decode, canonical hash | — | see below | — |

```
BenchmarkEncodeRecord-8          4.157 ns/op   15395 MB/s   0 B/op   0 allocs/op
BenchmarkDecodeRecordFields-8    6.298 ns/op   10162 MB/s   0 B/op   0 allocs/op
BenchmarkDecodeRecord-8         12.600 ns/op    5081 MB/s   0 B/op   0 allocs/op
BenchmarkRecordAt/sequential-8   9.169 ns/op    6980 MB/s   0 B/op   0 allocs/op
BenchmarkRecordAt/random-8      17.820 ns/op    3591 MB/s   0 B/op   0 allocs/op
BenchmarkBlob/payload-8         45.120 ns/op   17908 MB/s   0 B/op   0 allocs/op
BenchmarkBlob/append_levels-8  149.600 ns/op                0 B/op   0 allocs/op
BenchmarkCanonicalV1/delta-8    17.130 ns/op    2335 MB/s   0 B/op   0 allocs/op
BenchmarkCanonicalV1/snapshot-8 58.760 ns/op   14398 MB/s   0 B/op   0 allocs/op
BenchmarkCanonicalV1/delta_sha256-8  44.590 ns/op  897 MB/s  0 B/op  0 allocs/op
```

`decodeRecordFields` is the reader's hot path and skips validation; `decodeRecord` pays for the reserved-byte and structural checks and is used only on input that has not been through a block checksum.

SHA-256 runs at 897 MB/s against CRC-32C's 2335 MB/s on the same 40-byte projection — a 2.6× cost per record. That is why CRC-32C is the default canonical hash and SHA-256 is opt-in, for attestation only.

`internal/allocgate.AssertZero` now gates `BenchmarkEncodeRecord`, `BenchmarkDecodeRecordFields`, and `BenchmarkCanonicalV1/delta`, the three targets `plans/13-bench-and-profiles.md` names for this package. Every hot-path benchmark in this section reports zero allocations, gated or not.

### Block size

`block_size_records` was 4096 in the plan, as a guess. This measurement set it to **1024**.

| Block size | VerifyBlock | Throughput | Metadata | SeekTime |
|---|---|---|---|---|
| 64 | 146.1 ns | 28029 MB/s | 0.488% | 376.6 ns |
| 256 | 595.7 ns | 27503 MB/s | 0.122% | 1297 ns |
| **1024** | **2525 ns** | **25958 MB/s** | **0.031%** | **4944 ns** |
| 4096 | 9454 ns | 27730 MB/s | 0.008% | 19270 ns |
| 16384 | 40686 ns | 25772 MB/s | 0.002% | — |

Verification throughput is flat at 26-28 GB/s across a 256× range of block sizes, so it does not constrain the choice at all — CRC-32C is hardware accelerated and there is no per-call overhead worth amortising past 64 records. The real trade-off is the other two columns, which move in opposite directions: metadata is 20 bytes per block (one footer entry plus one time index entry), while seek cost is linear in the block size, because the index narrows the answer to one block and the rest is a scan.

1024 records is 64 KiB. Metadata is 0.03% of the file, seek is under 5 microseconds, and the block matches Windows' 64 KiB mapping allocation granularity.

## Pacing accuracy

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz.

### Pacer wait cost

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-21 | (this commit) | release-batching: cache releaseUntil so a run of records inside one window costs one clock read, not one per record | 13.14 ns/op | 1.542 ns/op | — |

```
Before (single-reused-timer pacer, no batching): BenchmarkPacerWait-8            90133396   13.140 ns/op   0 B/op   0 allocs/op
After  (released_within_the_window, batched):    BenchmarkPacerWait/released...  147162432   1.542 ns/op   0 B/op   0 allocs/op
BenchmarkPacerWait/arms_the_timer_once_per_wait-8       148   2211673 ns/op   0 B/op   0 allocs/op
```

This change is justified by the plan's own design rationale (`plans/08-pacing.md`,
Q7: 1x-10,000x is a target pacing rate with graceful batch-and-release degradation,
not a per-event scheduling guarantee) rather than by a live `go tool pprof` run in
this session — release-batching is required functionality, not speculative tuning,
so it does not need the profile-first sequence the project reserves for ad-hoc
performance changes. The before/after numbers above are the measurement half of
that rule, taken regardless.

`arms_the_timer_once_per_wait` genuinely waits on every call (deadlines spaced two
release windows apart), so its cost is dominated by the measured window itself
(~1.1ms on this machine) rather than by the pacer's own logic — reported, not
gated: `RealClock.NewTimer` wraps `time.AfterFunc`, whose fire path is `go
arg.(func())()` (see `internal/clock/realclock.go`), so a fire may allocate a
goroutine when the runtime's free-goroutine list is cold. This run happened to
report zero, which is not a guarantee.

### Observed accuracy

`BenchmarkPacingAccuracy` paces 1000 synthetic records at 10x real time and checks
the result against the schedule. Per Q7 (`plans/16-open-questions.md`), only two
things are hard-asserted: no record releases more than one measured window early
(release-batching's own documented bound, not zero — see `Pacer.Wait`), and the
observed span from first to last release tracks the scheduled span within
`rateToleranceThousandths`. Per-event lateness is reported, never asserted, since
Q7's answer is precisely that no per-event guarantee exists.

35 runs total (`-benchtime=1x`, so each run is exactly one pass): 20 to establish
the tolerance, 15 more after fixing it, to confirm the fixed value still holds.
Zero failures across all 35. Distribution from the first 20 runs:

| Metric | Min | Median | Max |
|---|---|---|---|
| `max_late_ns` | 1.11ms | 1.58ms | 4.77ms |
| `mean_late_ns` | −4.48ms | 0.18ms | 0.21ms |
| `p99_late_ns` | 0.77ms | 1.09ms | 1.13ms |
| `window_ns` (measured) | 0.73ms | 0.81ms | 10.00ms |
| `rate_err_pct` | 0.0% | 0.0% | 0.7% |

One run (of 20) hit a real outlier: a scheduling noise spike during `measureWindow`'s
own probes measured a worst-case sample large enough to saturate `maxWindow`
(10ms), for that run only — `measureWindow` deliberately takes the *worst* of 8
samples, precisely so it never under-estimates a clock's resolution, so an
occasional noisy sample inflating the window is the accepted cost of that choice,
not a bug. That same run's `mean_late_ns` went negative (more early releases than
late ones, all still within the one-window bound) and `rate_err_pct` peaked at
0.7% — still comfortably under the chosen tolerance.

`rateToleranceThousandths = 20` (2.0%), set with roughly 28× margin above the
observed 0.7% worst case. If this benchmark starts flaking on a noisier machine or
CI runner, widen the tolerance and record the new distribution here — never delete
the assertion it backs.

## End-to-end replay rate

Environment for every row below: `go1.23.4 windows/amd64`, `GOMAXPROCS=8`, 11th Gen Intel Core i7-11370H @ 3.30GHz.

| Date | Commit | Change | Before | After | Profile |
|---|---|---|---|---|---|
| 2026-09-22 | 7400143 | M7: first end-to-end rate, measured over the new `bench` load harness | — | see below | — |

```
BenchmarkEndToEndReplay/unpaced_max_rate-8   12    95130225 ns/op   4204762 records/s   1434078 B/op   592 allocs/op
BenchmarkEndToEndReplay/paced_100x-8         10   100325790 ns/op   3987011 records/s   1437276 B/op   593 allocs/op
```

The `Commit` column names `7400143`, the commit that added the harness being measured,
because a row cannot carry the hash of the commit it lands in. Three `-count=3` samples
spanned 94.74-95.15 ms/op unpaced and 100.25-100.44 ms/op paced, so the numbers above
are representative rather than a lucky pass.

One iteration is one whole replay of a 400,000-record synthetic dataset (four venues,
six files — `benchShape` in `bench/harness_bench_test.go`) through the entire in-process
stack: memory-mapped files, one cursor per venue, a four-worker `merge.Merger`, a
1024-slot `fanout.Ring`, and four subscribers (two Block, two Drop) each draining on its
own goroutine. So `ns/op` is the cost of a complete replay and `records/s` is the
end-to-end rate.

This measures the in-process path, up to and including fan-out delivery. It does not
cross a socket, so it is not a `cmd/replayd` gRPC number; the determinism boundary is
drawn at the same place for the same reason (see `docs/determinism.md`). A transport-level
row belongs in this section too once one is measured, alongside this one rather than
replacing it.

The 5.2 ms the paced case adds is almost entirely `fanout.NewPacer`'s one-time
release-batching window measurement, which costs 4.22 ms on this machine: eight probe
sleeps through `RealClock.SleepUntil`, whose resolution works out to roughly 0.5 ms here.
The remaining ~1.0 ms over 400,000 records is about 2.5 ns per record, consistent with
`BenchmarkPacerWait`'s own 1.542 ns/op fast path in the Pacing accuracy section above.

At 100x this dataset never actually waits. `internal/synth` advances `exchange_ts` by 0
to 3 nanoseconds per record, so the whole dataset spans well under a millisecond of
source time, and every record is already due by the time the pacer is asked about it.
The paced row therefore measures the pacer's per-record cost on the release-batching
fast path, not real-time sleeping. A row that measures genuine waiting needs a dataset
whose timestamps span real seconds; `internal/synth`'s fixture is deliberately dense,
because its own job is the determinism suite.

`BenchmarkEndToEndReplay` is not allocation-gated, and must not be. The zero-allocation
invariant is scoped to the in-process path *up to* the fan-out boundary, and this
benchmark spans past it: it opens files, builds a tree, and starts a goroutine per
subscriber, all of which allocate once per replay. The ~1.4 MB and ~590 allocations per
iteration are that per-replay setup, not a per-record cost — 592 allocations against
400,000 records is one per ~680 records. The per-record paths the invariant does cover
are gated in `internal/store`, `internal/merge` and `internal/fanout`, and each reports
0 B/op in the sections above. See `docs/book.md`'s "Off the hot path: no allocation gate"
for the same reasoning applied to `internal/book`.
