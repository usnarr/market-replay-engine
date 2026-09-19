// Package clock isolates every wall-clock read behind the Clock interface.
// time.Now appears exactly once in this repository, inside RealClock. Every
// other package that needs time takes a Clock and calls it, so a replay run
// can be driven by SimClock in tests without a single real sleep.
//
// Content and timing are separate concerns. A Clock implementation changes
// only when a record is delivered. It never changes which records exist,
// their order, or their fields. The determinism suite (see docs/clock.md)
// proves content and order under SimClock; a separate, smaller set of
// RealClock-based tests proves the pacing arithmetic on top of that. Do not
// "fix" a flaky pacing test by changing SimClock's wake order to match
// goroutine start order — wake order affects only delivery timing, and must
// never become observable in content.
package clock

// Clock reads time and schedules timers. All times are UnixNano int64
// values, not time.Time: an int64 is exactly comparable and exactly
// hashable, and matches exchange_ts's own representation in the binary
// format (see docs/format.md).
type Clock interface {
	// Now returns the current time as UnixNano.
	Now() int64
	// SleepUntil blocks until Now() >= deadline. It returns immediately if
	// deadline has already passed.
	SleepUntil(deadline int64)
	// NewTimer returns a Timer that fires after d nanoseconds.
	NewTimer(d int64) Timer
}

// Timer fires once after a duration, and can be reused via Reset. See
// RealClock and SimClock for the two implementations' differing fire-value
// semantics.
type Timer interface {
	// C returns the channel the fire time is sent on.
	C() <-chan int64
	// Reset changes the timer's deadline to d nanoseconds from now. It
	// reports whether the timer was active before the call.
	Reset(d int64) bool
	// Stop prevents the timer from firing. It reports whether the timer
	// was active before the call.
	Stop() bool
}
