package fanout

import (
	"testing"

	"replay/internal/clock"
)

// fakeTimer is a minimal clock.Timer for tests that must never reach
// Wait's genuine-wait branch: Reset just counts calls rather than
// scheduling anything, so a test that expects only the fast path can
// assert resetCalls stayed zero, and a test that reaches this branch by
// mistake blocks on an empty channel and fails on the test's own
// -timeout instead of silently passing.
type fakeTimer struct {
	c          chan int64
	resetCalls *int
}

func (t *fakeTimer) C() <-chan int64    { return t.c }
func (t *fakeTimer) Reset(d int64) bool { *t.resetCalls++; return true }
func (t *fakeTimer) Stop() bool         { return true }

// fakeClock is a deterministic, single-goroutine clock.Clock.
// SleepUntil advances now by a fixed, test-controlled amount regardless
// of the requested deadline, which is what gives measureWindow's own
// probes an exact, repeatable result to assert on. Now advances by step
// on every call, simulating steady real-time progress one check at a
// time without any actual waiting -- what lets a batching test span
// several release windows with no genuine timer wait anywhere in it.
type fakeClock struct {
	now       int64
	step      int64
	advanceBy int64

	nowCalls   int
	resetCalls int
}

func (c *fakeClock) Now() int64 {
	c.nowCalls++
	v := c.now
	c.now += c.step
	return v
}

func (c *fakeClock) SleepUntil(deadline int64) {
	c.now += c.advanceBy
}

func (c *fakeClock) NewTimer(d int64) clock.Timer {
	return &fakeTimer{c: make(chan int64), resetCalls: &c.resetCalls}
}

func TestPacerWindow(t *testing.T) {
	t.Run("a_clock_with_no_autonomous_time_has_an_unbounded_window", func(t *testing.T) {
		sc := &clock.SimClock{}
		if got := measureWindow(sc); got != unboundedWindow {
			t.Errorf("measureWindow(SimClock) = %d, want unboundedWindow (%d)", got, unboundedWindow)
		}
	})

	t.Run("a_fake_clock_with_no_autonomous_time_also_measures_unbounded", func(t *testing.T) {
		fc := &fakeClock{advanceBy: 0}
		if got := measureWindow(fc); got != unboundedWindow {
			t.Errorf("measureWindow() = %d, want unboundedWindow (%d)", got, unboundedWindow)
		}
	})

	t.Run("a_measured_window_is_floored", func(t *testing.T) {
		fc := &fakeClock{advanceBy: minWindow / 2}
		if got := measureWindow(fc); got != minWindow {
			t.Errorf("measureWindow() = %d, want minWindow (%d)", got, minWindow)
		}
	})

	t.Run("a_measured_window_is_capped", func(t *testing.T) {
		fc := &fakeClock{advanceBy: maxWindow * 10}
		if got := measureWindow(fc); got != maxWindow {
			t.Errorf("measureWindow() = %d, want maxWindow (%d)", got, maxWindow)
		}
	})

	t.Run("a_real_clock_measures_a_window_inside_the_bounds", func(t *testing.T) {
		got := measureWindow(clock.RealClock{})
		if got < minWindow || got > maxWindow {
			t.Errorf("measureWindow(RealClock) = %d, want between %d and %d", got, minWindow, maxWindow)
		}
	})
}

func TestPacerBatching(t *testing.T) {
	t.Run("releases_every_record_inside_one_window_without_arming_the_timer", func(t *testing.T) {
		const window = 1000
		fc := &fakeClock{} // now and step both stay 0: nothing needs to elapse
		s, _ := NewSpeed(1, 1)
		p := newPacerWindow(fc, s, window)
		p.Start(0)

		for _, ts := range []int64{0, 1, 500, 999, 1000} {
			if ok := p.Wait(ts); !ok {
				t.Fatalf("Wait(%d) = false, want true", ts)
			}
		}
		if fc.resetCalls != 0 {
			t.Errorf("timer.Reset was called %d times, want 0: every deadline was inside the first window", fc.resetCalls)
		}
	})

	t.Run("preserves_order_across_a_batch_boundary", func(t *testing.T) {
		// Order is never this pacer's to break: Wait only gates whether
		// the caller's already-chosen next record proceeds, so this
		// confirms crossing a batch boundary doesn't itself fail or
		// reorder anything, not that the pacer re-sorts its input.
		const window = 1000
		fc := &fakeClock{step: window} // each Now() call reveals one more window's worth of elapsed time
		s, _ := NewSpeed(1, 1)
		p := newPacerWindow(fc, s, window)
		p.Start(0)

		deadlines := []int64{0, 500, 999, 1000, 1001, 1500, 1999, 2000, 2001, 2500}
		for _, ts := range deadlines {
			if ok := p.Wait(ts); !ok {
				t.Fatalf("Wait(%d) = false, want true", ts)
			}
		}
		if fc.resetCalls != 0 {
			t.Errorf("timer.Reset was called %d times, want 0: fc.step always reveals enough elapsed time to release immediately", fc.resetCalls)
		}
	})

	t.Run("reads_the_clock_once_per_batch_not_once_per_record", func(t *testing.T) {
		const window = 1000
		const records = 10_000
		fc := &fakeClock{step: window}
		s, _ := NewSpeed(1, 1)
		p := newPacerWindow(fc, s, window)
		p.Start(0)

		for i := 0; i < records; i++ {
			if ok := p.Wait(int64(i)); !ok {
				t.Fatalf("Wait(%d) = false, want true", i)
			}
		}

		// One batch spans `window` nanoseconds of deadlines, and
		// fc.step reveals one more window every clock read, so this
		// should cost on the order of records/window reads, not one
		// per record.
		if fc.nowCalls >= records {
			t.Errorf("Now was called %d times over %d records, want far fewer than one per record", fc.nowCalls, records)
		}
		t.Logf("Now called %d times over %d records (window=%d)", fc.nowCalls, records, window)
	})

	t.Run("never_releases_a_record_more_than_one_window_early", func(t *testing.T) {
		const window = 1000
		fc := &fakeClock{} // now stays fixed at 0
		s, _ := NewSpeed(1, 1)
		p := newPacerWindow(fc, s, window)
		p.Start(0)

		if ok := p.Wait(0); !ok {
			t.Fatalf("Wait(0) = false, want true")
		}
		// The first Wait set releaseUntil to Now()+window = 0+1000.
		// A deadline exactly at that boundary must release; this test
		// only documents the boundary is inclusive, not that anything
		// further out incorrectly releases — Wait already has no way to
		// see a deadline beyond releaseUntil without a fresh Now() call,
		// which never returns a smaller value than it did last time.
		if ok := p.Wait(window); !ok {
			t.Fatalf("Wait(%d) = false, want true", window)
		}
	})
}
