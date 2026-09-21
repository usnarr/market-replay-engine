package fanout

import (
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
)

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

	t0   int64
	base int64

	stop atomic.Bool
}

// NewPacer returns a Pacer driven by clk at speed. It creates the one
// timer this Pacer will ever use and immediately stops it, leaving both
// RealClock's and SimClock's implementations in the same clean, idle,
// drained state a fresh Reset expects to find (see docs/clock.md) —
// this is construction-time setup, not part of any per-record cost.
func NewPacer(clk clock.Clock, speed Speed) *Pacer {
	t := clk.NewTimer(pacerInitialTimerDelay)
	t.Stop()
	return &Pacer{clk: clk, speed: speed, timer: t, maxSlice: defaultMaxSlice}
}

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
}

// Wait blocks until the record with exchange timestamp ts is due, then
// reports true. It reports false if Stop was called while it waited,
// and the caller must then stop emitting.
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
	for {
		if p.stop.Load() {
			return false
		}
		now := p.clk.Now()
		if now >= deadline {
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
