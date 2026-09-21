package fanout

import (
	"sync/atomic"
	"testing"

	"replay/internal/allocgate"
	"replay/internal/clock"
)

// countingClock wraps a real clock.Clock and counts how many times
// NewTimer was called on it, so a test can confirm a pacer creates its
// timer exactly once, however many waits it serves.
type countingClock struct {
	clock.Clock
	newTimerCalls atomic.Int64
}

func (c *countingClock) NewTimer(d int64) clock.Timer {
	c.newTimerCalls.Add(1)
	return c.Clock.NewTimer(d)
}

func TestPacer(t *testing.T) {
	t.Run("releases_a_record_already_due_without_waiting", func(t *testing.T) {
		sc := &clock.SimClock{}
		s, _ := NewSpeed(1, 1)
		p := NewPacer(sc, s)
		p.Start(1000)

		if ok := p.Wait(1000); !ok {
			t.Fatalf("Wait(1000) = false, want true: the anchor's own timestamp is always due")
		}
		if ok := p.Wait(500); !ok {
			t.Fatalf("Wait(500) = false, want true: earlier than the anchor is always due")
		}
	})

	t.Run("waits_until_a_future_records_delivery_time", func(t *testing.T) {
		// A small real delay: bounded, and tolerant of ordinary
		// scheduling jitter, the same style internal/clock's own
		// realclock_test.go uses for "waits for deadline" cases.
		const delayNs = 5_000_000 // 5ms
		rc := clock.RealClock{}
		s, _ := NewSpeed(1, 1)
		p := NewPacer(rc, s)
		start := rc.Now()
		p.Start(start)

		if ok := p.Wait(start + delayNs); !ok {
			t.Fatalf("Wait() = false, want true")
		}
		if elapsed := rc.Now() - start; elapsed < delayNs {
			t.Errorf("Wait() returned after %dns, want at least %dns", elapsed, delayNs)
		}
	})

	t.Run("a_reused_timer_is_never_replaced", func(t *testing.T) {
		cc := &countingClock{Clock: clock.RealClock{}}
		s, _ := NewSpeed(1, 1)
		p := NewPacer(cc, s)
		base := cc.Now()
		p.Start(base)

		for i := 0; i < 50; i++ {
			// Every deadline already due (at or before base): no
			// blocking, but this still exercises repeated Wait calls
			// against one Pacer.
			if ok := p.Wait(base - int64(i)); !ok {
				t.Fatalf("Wait(%d) = false, want true", base-int64(i))
			}
		}
		// One future deadline, short enough to keep the test fast, to
		// exercise the Reset path itself at least once.
		if ok := p.Wait(base + 2_000_000); !ok {
			t.Fatalf("Wait() = false, want true")
		}

		if got := cc.newTimerCalls.Load(); got != 1 {
			t.Errorf("NewTimer was called %d times, want exactly 1", got)
		}
	})

	t.Run("stop_abandons_a_wait_and_reports_false", func(t *testing.T) {
		rc := clock.RealClock{}
		s, _ := NewSpeed(1, 1)
		p := NewPacer(rc, s)
		p.Start(rc.Now())

		done := make(chan bool, 1)
		go func() {
			// Far enough in the future that this Wait call is certainly
			// still blocked when Stop runs below.
			done <- p.Wait(rc.Now() + 10_000_000_000)
		}()

		p.Stop()
		if ok := <-done; ok {
			t.Fatalf("Wait() = true, want false: Stop was called before the deadline")
		}
	})

	t.Run("stop_before_any_wait_makes_every_later_wait_return_false_immediately", func(t *testing.T) {
		rc := clock.RealClock{}
		s, _ := NewSpeed(1, 1)
		p := NewPacer(rc, s)
		p.Start(rc.Now())
		p.Stop()

		if ok := p.Wait(rc.Now() + 10_000_000_000); ok {
			t.Fatalf("Wait() = true, want false: Stop had already been called")
		}
	})
}

// BenchmarkPacerWait measures the released-immediately path: every
// deadline is already due, so this never touches the timer at all,
// which is the per-record cost that matters at high replay speed. A
// benchmark that actually arms the timer belongs with the
// release-batching commit, once there is a window to report alongside
// it.
func BenchmarkPacerWait(b *testing.B) {
	sc := &clock.SimClock{}
	s, err := NewSpeed(10000, 1)
	if err != nil {
		b.Fatalf("NewSpeed() error = %v, want nil", err)
	}
	p := NewPacer(sc, s)
	p.Start(0)

	wait := func() { sinkBool = p.Wait(-1) } // always due: dt <= 0
	allocgate.AssertZero(b, wait)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wait()
	}
}
