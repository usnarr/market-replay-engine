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

`RealClock` and `SimClock`, the two implementations, are documented below once each one lands in code.
