# Clock and pacing

Scope: the `Clock` interface, `RealClock` as the sole `time.Now` call site, `SimClock`'s deterministic timer ordering, the content/timing separation invariant, the rational (non-float) speed representation, and the release-batching policy for sub-scheduler-resolution gaps at high speed.

See the root [`CLAUDE.md`](../CLAUDE.md) and the documentation index at [`CLAUDE.md`](./CLAUDE.md).

## The `Clock` interface

`internal/clock.Clock` is the only way any other package reads time or schedules a timer:

```go
type Clock interface {
	Now() int64
	SleepUntil(deadline int64)
	NewTimer(d int64) Timer
}

type Timer interface {
	C() <-chan int64
	Reset(d int64) bool
	Stop() bool
}
```

Every time value is a UnixNano `int64`, not a `time.Time`. `time.Time` is 24 bytes, carries an optional monotonic reading, and makes equality and `go-cmp` comparisons subtly wrong — two `time.Time` values with equal wall and monotonic components can compare unequal after either one round-trips through serialization that drops the monotonic reading. An `int64` nanosecond count has none of that: it is exactly comparable, exactly hashable, and it is the same representation `exchange_ts` already uses in the binary format (see `format.md`).

## Content and timing are separate concerns

A `Clock` implementation changes only *when* a record is delivered. It never changes which records exist, their order, or their fields. This split is what lets the determinism suite run entirely under `SimClock` — which never sleeps in real time — while a separate, much smaller set of `RealClock`-based tests proves the pacing arithmetic is correct. See `TestDeterminism` in `internal/merge` and `internal/fanout` for the content side of this split, once those packages exist.

## `RealClock`

`internal/clock.RealClock` is the **only** place `time.Now()` appears in the repository. `cmd/lint-determinism`'s `no-time-now` rule enforces this by allowlisting exactly one file, `internal/clock/realclock.go`, and flagging `time.Now`, `time.Since`, `time.After`, `time.Tick`, `time.Sleep`, `time.NewTimer`, and `time.NewTicker` everywhere else, including test files in this same package.

`RealClock.NewTimer` wraps `time.AfterFunc`, not `time.NewTimer`. A bare `*time.Timer`'s channel carries `time.Time`, not `int64`, and converting it would need a forwarding goroutine that then has to reimplement `Stop`/`Reset` bookkeeping and the stale-value drain the standard library already provides for `AfterFunc` timers. The callback sends the real fire time into a capacity-1 channel; `Reset` and `Stop` drain that channel non-blockingly before delegating to the underlying `*time.Timer`. `Stop`'s return value has the same caveat as the standard library's: it reports whether it prevented the callback from running, but a value can still be pending on the channel if the callback had already fired.

## `SimClock`

Used by every test, and by the determinism suite. Two properties, deliberately in tension:

1. **`SleepUntil` returns immediately and never advances `now`.** `SimClock` never sleeps in real time — this is what lets `make determinism` run in milliseconds instead of real elapsed replay time. It also never moves the clock as a side effect of a sleep call: if it did, concurrent sleepers would advance simulated time in goroutine-scheduling order, which is exactly the nondeterminism this package exists to remove. `Advance(d)` is the only way `now` moves.
2. **Pacing is not exercised by the core determinism test.** This is stated as a written invariant, not a gap: pacing must not be able to affect record content or order, only the wall-clock timing of delivery.

`Advance(d)` adds `d` to `now`, then fires every timer whose deadline is now due, in a fixed, deterministic order:

- Order timers by deadline, ascending.
- Break a tie between two timers with the identical deadline by a **monotonic registration ID**, assigned when `NewTimer` was called, ascending. Never by goroutine identity, never by map iteration.

A fired `SimClock` timer sends **its own deadline**, not `SimClock`'s current `now`, on its channel — so the value a listener observes does not depend on how far a single `Advance` call overshot that timer's deadline. `RealClock` cannot offer this and sends the real fire time instead; this is a deliberate, documented difference between the two implementations, not an inconsistency to "fix."

`SimClock` keeps its pending timers in a sorted slice, not a heap and not a map: `container/heap` is banned repository-wide (see the root `CLAUDE.md`), and a map would introduce unordered iteration into a package other code is likely to copy from as a pattern. The timer count in any given test is small, so a linear insert into a sorted slice is simpler than a heap and fast enough.

`Reset` keeps a timer's original registration ID, so rescheduling it does not change its tie-break position relative to timers created around the same time.

If `SimClock` is ever extended so multiple goroutines can call `SleepUntil` concurrently and get woken by the same `Advance` call, the wake order across those goroutines is still not allowed to be observable in content — only delivery timing may depend on it. Nobody should "fix" a flaky pacing test by making `SimClock`'s wake order match goroutine start order.

## Taking a `Clock`, and the one place a value depends on real time

Every package that needs time takes a `Clock` and reads it through the interface — this is not itself a departure from "`time.Now` appears exactly once," because `RealClock` stays the one call site regardless of how many packages hold a value of it. `internal/fanout.Config.Clock` (defaulting to `RealClock{}` when left unset) is the first consumer: it drives the `pacing_slip` gauge and the stalled-Block-subscriber watchdog, neither of which ever reaches hashed content.

The watchdog is worth calling out precisely, because it is the one place in the project where a run's *out-of-band* outcome — which subscriber, if any, gets evicted, and when — genuinely depends on real elapsed time and cannot be simulated. `SimClock`'s `Now()` never moves on its own, so "no progress for a real duration" has no meaning under it; a determinism run configured with `WatchdogTimeout: 0` (the default) disables the watchdog entirely, which is exactly right, because content and order must never depend on wall-clock scheduling and eviction is the one thing here that legitimately does. See `docs/backpressure.md` for the watchdog's own design and why eviction is checked before every read, not just once.

This is not a lint-suppressed exception — `cmd/lint-determinism`'s `no-time-now` rule matches a selector on the `time` package directly (`time.Now`, `time.Sleep`, and so on); calling `Now()` on a `clock.Clock` value never matches it, watchdog included, so `cmd/lint-determinism/suppress.go`'s `Allowlist` stays empty. The exception here is a design discipline, stated in this file and in code comments at the call site, not a gap in the tooling.

## Speed is an exact rational, never a float

`internal/fanout.Speed` is a replay-speed multiplier, `num/den int64`, never a `float64`. Two compounding reasons:

1. **Architecture-dependent rounding.** The Go language spec permits fusing floating-point operations across statements unless an explicit conversion forces intermediate rounding. Go's compiler emits fused multiply-add (FMA) instructions on arm64, ppc64, s390x, and riscv64, but not on amd64. The same source expression could therefore produce a different result on an Apple Silicon development machine than on an amd64 CI runner.
2. **Content is safe, but Drop-subscriber output is not.** Pacing only affects *timing* of delivery, and delivery timing is explicitly outside the canonical hash (see `format.md` and `backpressure.md`). But timing determines exactly *when* a Drop subscriber gets lapped, so a float-driven timing difference would make Drop-subscriber gap output architecture-dependent even though merge content stays architecture-independent — a real, user-visible nondeterminism, not a theoretical one.

`NewSpeed(num, den)` reduces to lowest terms by GCD before accepting a value, so `NewSpeed(2, 1) == NewSpeed(10000, 5000)`: `cmd/replayd`'s run manifest records `Num()`/`Den()`, never a float, and two requests for "the same speed" written two different ways must produce a byte-identical manifest. It rejects a non-positive `num` or `den`, a reduced term above `maxSpeedTerm` (2^31), or a ratio outside `[1/1024, 1,000,000]`× as `ErrInvalidSpeed` — two orders of magnitude of speed-up headroom past the project's stated 1×–10,000× range, and a slow-motion range no real use case needs beyond. Both bounds exist to keep the arithmetic below inside `int64` for any realistic dataset span, not to police what an operator "should" ask for.

`Speed.DeliveryTime(t0, base, ts)` computes `t0 + (ts - base) * den / num` with a 128-bit intermediate product and no float anywhere:

```go
hi, lo := bits.Mul64(uint64(ts-base), uint64(den))
if hi >= uint64(num) {
	return math.MaxInt64 // saturate: this schedule does not fit in an int64
}
q, _ := bits.Div64(hi, lo, uint64(num))
```

`bits.Div64` panics for `y == 0` (division by zero) or `y <= hi` (quotient overflow). The single comparison `hi >= uint64(num)` closes both at once: for the overflow case it is exactly that predicate, and for `num == 0` (reachable only through the zero `Speed{}` value, since `NewSpeed` rejects it) `hi >= 0` is always true for an unsigned `hi`, so the saturating return fires before `bits.Div64` is ever called. Do not "simplify" this guard by adding a separate `num > 0` check first — it would be redundant with what this one comparison already covers. A `ts` at or before `base` returns `t0` unchanged; a schedule too far in the future saturates at `math.MaxInt64` rather than wrapping, on both the quotient and the final addition to `t0`.

There is no float input to this package's API at all, and there is no float boundary anywhere else in the project either. `cmd/replayd`'s gRPC `Subscribe` request carries `speed_num` and `speed_den` as two `int64` fields (`api/replay.proto`), handed straight to `NewSpeed`, which validates them once at that one boundary. A `double` on the wire would put the architecture-dependent rounding hazard above back one layer out, where it would be harder to see and no less real — the schema is where a float looks most like a harmless convenience.

## The delivery schedule is absolute and is never re-anchored

`Pacer.Start(base)` anchors the schedule once: the record with exchange timestamp `base` is due at `Now()`, and every later record's delivery time is computed relative to that one anchor pair `(t0, base)`. No other method moves it, not even implicitly.

The tempting bug is re-anchoring after the emit loop has fallen behind — "we're 5 seconds late, reset `t0` to now." That would make the delivery schedule depend on how far behind the run happened to get, which depends on subscriber speed (a slow Block subscriber holding the writer back, per `backpressure.md`'s Block/Drop coupling), which would make Drop-subscriber lapping timing non-reproducible run to run. `Pacer.Slip()` is the observable instead: it reports how far behind schedule the most recent release was, in nanoseconds, never a correction applied to the schedule itself.

For a seek: `replay(from=T)` anchors `Pacer.Start` on the first record at or after `T`. This is consistent with the seek-suffix invariant `internal/merge` establishes, because pacing is not part of the hashed canonical projection — see `backpressure.md`'s note on why a gap lives in delivery framing, not hashed content, for the same reasoning applied to a different field.

## Release batching at sub-scheduler-resolution gaps

At 10,000×, an inter-event gap of 1 microsecond in source data becomes 100 nanoseconds of intended real-time delay — below the resolution any `time.Sleep` or OS timer can reliably provide. **Q7's answer** (`16-open-questions.md`): 1×–10,000× is a **target pacing rate** with graceful batch-and-release degradation, not a per-event scheduling guarantee.

`measureWindow` measures the release-batching window once, at `Pacer` construction, by probing `Clock.SleepUntil` — never `Clock.NewTimer`, and that choice is load-bearing: a timer probe under `SimClock` would never fire, because nothing calls `Advance`, and the measurement would deadlock. It takes the worst of 8 samples (never the best or an average, so it never under-estimates a clock's actual resolution), floors at 1 microsecond, and caps at 10 milliseconds.

A clock whose `Now()` does not move across any probe — `SimClock` — has no autonomous time of its own, so there is no delay it can actually deliver, and the window is **unbounded**, not zero. A pacer's contract is "never wait for a delay this clock cannot deliver"; under such a clock, that is every delay, so every record releases immediately and the timer is never armed. Returning a literal zero would claim the opposite — perfect resolution — and `Pacer.Wait` would arm a `SimClock` timer nobody will ever `Advance`, hanging forever. This is the precise mechanism behind the content-invariance test described below.

**What a batch is:** the maximal run of consecutive records `Wait` releases without touching the clock again. `Wait` caches `releaseUntil`; a deadline at or before it releases on the fast path with zero clock reads, so a run of records due within one window costs one clock read for the whole run, never one per record. A record is never released more than one window early — this is the actual bound `BenchmarkPacingAccuracy` (`BENCHMARKS.md`) checks, not zero. The pacer holds no queue and no lookahead: it can only delay the record the emit loop already selected in canonical order, never reorder, drop, duplicate, or coalesce it, which is also why it lives beside the emit loop (`internal/fanout`) rather than as a separate buffering stage.

A long wait for a record still due far in the future is sliced into segments no longer than `maxSlice` (20ms), so `Pacer.Stop` is observed within one slice without a second `select` clause on a done channel — this package bans a multi-clause `select` (see `cmd/lint-determinism`'s `no-multi-select`, and `internal/merge/merge.go`'s `refill` for the same rule applied to the merge stage). `Stop` itself touches only an atomic flag, never the timer: a concurrent `Reset` from `Wait`'s own loop drains the timer's channel as part of resetting it, and `Stop` racing that drain against `Wait`'s own receive could make the wake `Wait` is about to observe disappear into `Stop`'s drain instead.

Catch-up after a Block subscriber stalls the writer (see `backpressure.md`) falls out of this design for free: every deadline is already in the past by the time the writer resumes, so the pacer releases at full speed with no special-casing, and `Slip()` is how an operator sees it happening.

## Pacing under `SimClock`, and what the determinism suite actually proves

Because `SimClock` has no autonomous time, `measureWindow` returns the unbounded window, so no record ever waits and the pacer's timer is never armed, at any configured speed. `internal/fanout`'s `TestDeterminismPacing` replays the same dataset at 1× and 10,000× under `SimClock` and asserts an identical canonical hash.

State plainly what that proves and what it does not, so nobody mistakes one for the other: it proves the pacer is wired into the emit loop in a way that cannot leak the numeric speed value into content, order, or count — under `SimClock`, both runs execute the identical code path, and the only difference between them is the `(num, den)` flowing through `DeliveryTime`, whose result the release check then discards. It does **not** prove that 10,000× under a real clock produces the same content as 1× under real waiting, because neither run in that test waits at all. That is a structural property of `SimClock`, not a gap in the test's design — real-clock timing accuracy is deliberately a separate concern, covered by `BenchmarkPacingAccuracy` and `BENCHMARKS.md`'s Pacing accuracy section instead, in the bench/integration tier rather than the core determinism suite.
