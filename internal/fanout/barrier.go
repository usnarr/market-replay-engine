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
// The wait is a Gosched spin, never a clock read and never a select:
// this project's rule against a multi-clause select in this package
// (see cmd/lint-determinism's no-multi-select, and
// internal/merge/merge.go's refill for the same rule applied to the
// merge stage) means there is no channel-based wake to use instead, and
// a plain spin that yields the processor is the simplest thing that is
// still correct. Revisit only with a profile showing it matters.
//
// This commit assumes the Block subscriber set is fixed once Subscribe
// has returned: a later commit's subscriber lifecycle is what makes a
// departing Block subscriber release a writer parked here.
func (r *Ring) waitForBlockBarrier(n uint64) {
	capacity := uint64(r.Capacity())
	if n < capacity {
		return
	}
	threshold := n - capacity
	for r.minBlockCursor() <= threshold {
		r.applyControl()
		runtime.Gosched()
	}
}
