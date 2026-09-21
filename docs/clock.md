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
