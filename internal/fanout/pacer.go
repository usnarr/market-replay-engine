package fanout

import (
	"math"
	"sync/atomic"

	"replay/internal/clock"
)

const (
	// pacerInitialTimerDelay is an arbitrary duration used only to
	// construct the pacer's one reused timer, which is stopped
	// immediately afterward — see NewPacer. Its value never matters.
	pacerInitialTimerDelay = 3_600_000_000_000 // 1 hour, in nanoseconds

	// defaultMaxSlice bounds how long a single wait segment runs before
	// Wait re-checks whether it has been told to stop. It is what keeps
	// Stop's effect bounded even during a long wait, without a second
	// select clause on a done channel — see Wait's own doc comment.
	defaultMaxSlice = 20_000_000 // 20ms

	// probeDelay is the smallest delay worth asking a clock about when
	// measuring its release-batching window. It is 1 microsecond, not 1
	// nanosecond: RealClock.SleepUntil recomputes deadline-Now() and
	// returns immediately once that is already non-positive, so a
	// too-small probe would measure only the cost of two Now() calls
	// and nothing about the clock's actual resolution.
	probeDelay = 1_000 // 1us

	windowProbes = 8

	// minWindow floors the measured window at the cost of arming a
	// timer and taking one channel receive — below that, a pacer would
	// spend more time scheduling than waiting.
	minWindow = 1_000 // 1us

	// maxWindow caps it. At 10ms, one release already covers 10ms of
	// the scheduled stream at 1x, which is visible burstiness; a loaded
	// machine must not be able to push it further.
	maxWindow = 10_000_000 // 10ms

	// unboundedWindow is the window of a clock with no autonomous time
	// of its own — see measureWindow's own doc comment.
	unboundedWindow = math.MaxInt64
)

// measureWindow returns the release-batching window for clk, measured
// through the Clock interface and never from time.Now.
//
// It probes SleepUntil, not NewTimer, and that choice is load-bearing:
// under SimClock, SleepUntil returns immediately, while a timer probe
// would never fire because nothing calls Advance, and this measurement
// would deadlock.
//
// A clock whose Now does not move across any probe has no autonomous
// time of its own, so there is no delay it can actually deliver, and
// the window is unbounded rather than zero: a pacer must never wait for
// a delay its clock cannot deliver, and under such a clock that is
// every delay, so every record releases immediately and the timer is
// never armed. Returning a literal zero would claim the opposite —
// perfect resolution — and Wait would arm a SimClock timer nobody will
// ever Advance, hanging forever. See docs/clock.md.
func measureWindow(clk clock.Clock) int64 {
	var worst int64
	for i := 0; i < windowProbes; i++ {
		start := clk.Now()
		clk.SleepUntil(start + probeDelay)
		if d := clk.Now() - start; d > worst {
			worst = d
		}
	}
	switch {
	case worst <= 0:
		return unboundedWindow
	case worst < minWindow:
		return minWindow
	case worst > maxWindow:
		return maxWindow
	}
	return worst
}

// addClampedInt64 adds two non-negative nanosecond values, saturating at
// math.MaxInt64 instead of wrapping. It exists because the window of a
// clock with no autonomous time of its own is unboundedWindow
// (math.MaxInt64) — see measureWindow.
func addClampedInt64(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

// Pacer converts a record's exchange timestamp into a delivery deadline
// and waits for it, using speed to convert source-time spans into
// wall-clock (or simulated) ones. One Pacer serves one emit loop; Wait
// and Start are not safe for concurrent use, but Stop is — it is the
// one method meant to be called from another goroutine. See
// docs/clock.md.
type Pacer struct {
	clk   clock.Clock
	speed Speed

	// timer is created once, in NewPacer, and reused for every wait via
	// Reset. See docs/clock.md for why this is required, not just an
	// optimisation: a timer created per record is this project's
	// specifically named allocation risk at high replay speed.
	timer clock.Timer

	maxSlice int64
	window   int64

	t0           int64
	base         int64
	releaseUntil int64

	slip atomic.Int64
	stop atomic.Bool
}

// NewPacer returns a Pacer driven by clk at speed. It creates the one
// timer this Pacer will ever use and immediately stops it, leaving both
// RealClock's and SimClock's implementations in the same clean, idle,
// drained state a fresh Reset expects to find (see docs/clock.md) — this
// is construction-time setup, not part of any per-record cost. It also
// measures clk's release-batching window (see measureWindow), which
// costs a handful of short sleeps, also at construction time only.
func NewPacer(clk clock.Clock, speed Speed) *Pacer {
	return newPacerWindow(clk, speed, measureWindow(clk))
}

// newPacerWindow is NewPacer with the release-batching window supplied
// directly instead of measured. Tests use it to make batch boundaries
// exact; production code always goes through NewPacer, so the window
// actually reflects the clock driving it.
func newPacerWindow(clk clock.Clock, speed Speed, window int64) *Pacer {
	t := clk.NewTimer(pacerInitialTimerDelay)
	t.Stop()
	return &Pacer{clk: clk, speed: speed, timer: t, maxSlice: defaultMaxSlice, window: window}
}

// Window reports the measured release-batching window.
func (p *Pacer) Window() int64 { return p.window }

// Slip reports how far behind the intended schedule the most recent
// release was, in nanoseconds, or zero when it was on schedule or early.
// It is the source for a pacing_slip-style gauge once the emit loop
// wires one up to it; see internal/fanout's existing Block-barrier
// pacing_slip for the sibling gauge this one is meant to sit beside.
func (p *Pacer) Slip() int64 { return p.slip.Load() }

// Start anchors the schedule: the record with exchange timestamp base is
// due now, and every later record's delivery time is computed relative
// to it. Call it once, from the emit goroutine, before the first Wait.
//
// The anchor is never moved afterward, by any other method — not even
// implicitly. Re-anchoring after the emit loop falls behind would make
// the schedule depend on how far behind it got, which depends on
// subscriber speed, which would make Drop-subscriber lapping timing
// non-reproducible run to run. See docs/clock.md.
func (p *Pacer) Start(base int64) {
	p.t0 = p.clk.Now()
	p.base = base
	p.releaseUntil = p.t0
}

// Wait blocks until the record with exchange timestamp ts is due, then
// reports true. It reports false if Stop was called while it waited,
// and the caller must then stop emitting.
//
// Records whose scheduled delivery time falls inside one release-
// batching window (see measureWindow) are released together without
// Wait touching the clock again: the fast path compares deadline
// against a cached releaseUntil and returns immediately, so a run of
// consecutive records due within the same window costs one clock read
// for the whole run, not one per record. A record is never released
// more than one window early. This is the specification's own
// documented degradation at speeds where an inter-event gap becomes
// smaller than any scheduler can honour — see docs/clock.md — not an
// approximation to be tightened later.
//
// The wait is sliced into segments no longer than maxSlice, so Stop is
// observed within one slice without a second select clause on a done
// channel — this package bans a multi-clause select (see
// cmd/lint-determinism's no-multi-select, and internal/merge/merge.go's
// refill for the same rule applied to the merge stage), and slicing
// costs nothing in accuracy: every slice is recomputed from the
// absolute deadline, so overshoot never accumulates across slices.
func (p *Pacer) Wait(ts int64) bool {
	deadline := p.speed.DeliveryTime(p.t0, p.base, ts)
	if deadline <= p.releaseUntil {
		return true
	}

	for {
		if p.stop.Load() {
			return false
		}
		now := p.clk.Now()
		p.releaseUntil = addClampedInt64(now, p.window)
		if deadline <= p.releaseUntil {
			if now > deadline {
				p.slip.Store(now - deadline)
			} else {
				p.slip.Store(0)
			}
			return true
		}

		d := deadline - now
		if d > p.maxSlice {
			d = p.maxSlice
		}
		p.timer.Reset(d)
		<-p.timer.C()
	}
}

// Stop causes the current or next call to Wait to return false at the
// next opportunity — within one maxSlice of a call already blocked. It
// is safe to call from another goroutine, and deliberately touches only
// the atomic flag, never the timer: a concurrent Reset (from Wait's own
// loop, on the goroutine actually using this Pacer) drains the timer's
// channel as part of resetting it, and Stop racing that drain against
// Wait's own receive could make the wake Wait is about to observe
// disappear into Stop's drain instead.
func (p *Pacer) Stop() {
	p.stop.Store(true)
}
