package clock

import (
	"testing"
	"time"
)

// These tests never call the time package directly: no-time-now in
// cmd/lint-determinism allowlists only realclock.go itself, not this file.
// time.Date is not in the banned function set (it does not read the wall
// clock), so it is safe to use for plausibility bounds.

func TestRealClock(t *testing.T) {
	t.Run("now_is_monotonic", func(t *testing.T) {
		c := RealClock{}

		first := c.Now()
		second := c.Now()

		if second < first {
			t.Errorf("Now() went backwards: first=%d second=%d", first, second)
		}
	})

	t.Run("now_is_a_plausible_wall_clock_time", func(t *testing.T) {
		c := RealClock{}
		lower := mustUnixNano(2020, 1, 1)
		upper := mustUnixNano(2200, 1, 1)

		got := c.Now()

		if got < lower || got > upper {
			t.Errorf("Now() = %d, want between %d and %d", got, lower, upper)
		}
	})

	t.Run("sleep_until_past_deadline_returns_immediately", func(t *testing.T) {
		c := RealClock{}

		c.SleepUntil(c.Now() - int64(1e9))
	})

	t.Run("sleep_until_future_deadline_waits_for_deadline", func(t *testing.T) {
		c := RealClock{}
		deadline := c.Now() + int64(2*1e6) // 2ms

		c.SleepUntil(deadline)

		if got := c.Now(); got < deadline {
			t.Errorf("Now() = %d after SleepUntil(%d), want >= deadline", got, deadline)
		}
	})

	t.Run("new_timer_fires_after_duration", func(t *testing.T) {
		c := RealClock{}
		start := c.Now()
		tm := c.NewTimer(int64(1e6)) // 1ms

		fired := <-tm.C()

		if fired < start {
			t.Errorf("fired value %d is before timer start %d", fired, start)
		}
	})

	t.Run("stop_before_fire_reports_active", func(t *testing.T) {
		c := RealClock{}
		tm := c.NewTimer(int64(1 * 1e9)) // 1s, long enough not to race the Stop call

		active := tm.Stop()

		if !active {
			t.Error("Stop() = false, want true for a timer stopped well before its deadline")
		}
	})

	t.Run("reset_reschedules_a_stopped_timer", func(t *testing.T) {
		c := RealClock{}
		tm := c.NewTimer(int64(1 * 1e9))
		tm.Stop()

		tm.Reset(int64(1e6)) // 1ms

		fired := <-tm.C()
		if fired == 0 {
			t.Error("fired value is zero after Reset")
		}
	})
}

// mustUnixNano returns the UnixNano value for the given UTC calendar date,
// without ever reading the current wall clock.
func mustUnixNano(year int, month, day int) int64 {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).UnixNano()
}
