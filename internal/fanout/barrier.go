package fanout

import (
	"math"
	"runtime"
)

// minBlockCursor returns the smallest cursor among every registered
// Block subscriber, or math.MaxUint64 if there are none. An empty Block
// set must never hold the writer back, and MaxUint64 makes that true by
// arithmetic rather than by a separate branch: no real emit index will
// ever reach it.
//
// This walks a plain slice, never a map: iteration order over
// subscribers is not part of any one subscriber's content (each
// subscriber's output is a function of its own cursor alone), so this is
// the one case a range over a collection whose membership grows over
// wall-clock time is fine — see docs/backpressure.md.
func (r *Ring) minBlockCursor() uint64 {
	min := uint64(math.MaxUint64)
	for _, s := range r.blocking {
		if c := s.Cursor(); c < min {
			min = c
		}
	}
	return min
}

// waitForBlockBarrier blocks until it is safe to write emit index n:
// every Block subscriber has moved its cursor past the index the slot
// for n currently holds. The first capacity writes never wait, because
// their slots hold nothing yet.
//
// The wait is a Gosched spin, never a select: this project's rule
// against a multi-clause select in this package (see
// cmd/lint-determinism's no-multi-select, and internal/merge/merge.go's
// refill for the same rule applied to the merge stage) means there is no
// channel-based wake to use instead, and a plain spin that yields the
// processor is the simplest thing that is still correct. Revisit only
// with a profile showing it matters.
//
// A departing Block subscriber releases a writer parked here because
// applyControl runs on every spin iteration, not because of anything
// special about the wait itself. A stalled one that never departs on
// its own is watchdogTimeout's job, below.
//
// The clock is read only on the slow path — never when the barrier
// passes immediately — so this costs nothing in steady state; see
// PacingSlipNanos.
func (r *Ring) waitForBlockBarrier(n uint64) {
	capacity := uint64(r.Capacity())
	if n < capacity {
		return
	}
	threshold := n - capacity
	if r.minBlockCursor() > threshold {
		return
	}

	start := r.clk.Now()
	windowStart := start
	for r.minBlockCursor() <= threshold {
		r.applyControl()
		if r.watchdogTimeout > 0 {
			now := r.clk.Now()
			if now-windowStart >= r.watchdogTimeout {
				r.evictStalledBlockSubscriber(threshold)
				windowStart = now
			}
		}
		runtime.Gosched()
	}
	r.slipNs.Add(r.clk.Now() - start)
}

// evictStalledBlockSubscriber is the one documented exception to
// "time.Now appears exactly once, inside RealClock": a Block subscriber
// that stops reading entirely would otherwise stall the writer forever,
// and detecting "no progress for a real duration" is not something a
// simulated clock can stand in for — it is inherently about wall-clock
// time, the same way internal/clock's own package doc carves out no
// exception for content or order, only for this kind of out-of-band
// effect. See docs/clock.md and docs/backpressure.md.
//
// This reads through r.clk exactly like PacingSlipNanos does — not a new
// time.Now call site, the same RealClock the rest of the package already
// takes — and its only effect is to abort/disconnect one subscriber,
// never to change any record's content, order, or emit index. Evicting
// is out of band and, unlike everything else in this package, is
// therefore the one place a run's outcome is not fully reproducible
// under RealClock: which subscriber gets evicted, and when, depends on
// real scheduling. It never affects what any surviving subscriber
// receives.
//
// It evicts the Block subscriber with the smallest cursor — the one
// actually responsible for this wait — never by iterating in an order
// that could matter, since exactly one victim is chosen by comparison,
// not by position.
func (r *Ring) evictStalledBlockSubscriber(threshold uint64) {
	var victim *Subscriber
	min := uint64(math.MaxUint64)
	for _, s := range r.blocking {
		if c := s.Cursor(); c <= threshold && c < min {
			min = c
			victim = s
		}
	}
	if victim == nil {
		return
	}
	victim.evicted.Store(true)
	r.unsubscribeNow(victim)
}
