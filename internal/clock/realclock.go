package clock

import "time"

// RealClock implements Clock using the wall clock. This file is the only
// place in the repository allowed to call time.Now and its relatives;
// cmd/lint-determinism's no-time-now rule allowlists exactly this file. See
// docs/clock.md.
type RealClock struct{}

// Now returns time.Now as UnixNano.
func (RealClock) Now() int64 {
	return time.Now().UnixNano()
}

// SleepUntil blocks until Now() >= deadline, or returns immediately if
// deadline has already passed.
func (c RealClock) SleepUntil(deadline int64) {
	d := deadline - c.Now()
	if d <= 0 {
		return
	}
	time.Sleep(time.Duration(d))
}

// NewTimer returns a Timer backed by time.AfterFunc, not time.NewTimer: a
// bare *time.Timer's channel carries time.Time, and converting it to
// <-chan int64 would need a forwarding goroutine that then has to
// reimplement the Stop/Reset bookkeeping and stale-value drain AfterFunc
// timers already get from the standard library.
func (RealClock) NewTimer(d int64) Timer {
	rt := &realTimer{c: make(chan int64, 1)}
	rt.t = time.AfterFunc(time.Duration(d), func() {
		select {
		case rt.c <- time.Now().UnixNano():
		default:
		}
	})
	return rt
}

// realTimer adapts a *time.AfterFunc timer to the Timer interface.
type realTimer struct {
	t *time.Timer
	c chan int64
}

func (rt *realTimer) C() <-chan int64 {
	return rt.c
}

// Reset changes the timer's deadline to d nanoseconds from now. It reports
// whether the timer was active before the call. Like time.Timer.Reset, a
// value can still be pending on C() from a callback that had already
// started before Reset was called; drain it first.
func (rt *realTimer) Reset(d int64) bool {
	select {
	case <-rt.c:
	default:
	}
	return rt.t.Reset(time.Duration(d))
}

// Stop prevents the timer from firing. It reports whether the timer was
// active before the call, with the same caveat time.Timer.Stop documents:
// a value can still be pending on C() if the callback had already started.
func (rt *realTimer) Stop() bool {
	active := rt.t.Stop()
	select {
	case <-rt.c:
	default:
	}
	return active
}
