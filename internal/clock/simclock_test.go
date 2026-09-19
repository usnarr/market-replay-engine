package clock

import "testing"

func TestSimClock(t *testing.T) {
	t.Run("sleep_until_past_deadline_returns_immediately", func(t *testing.T) {
		sc := &SimClock{}

		sc.SleepUntil(sc.Now() - 1)
	})

	t.Run("sleep_until_does_not_advance_the_clock", func(t *testing.T) {
		sc := &SimClock{}
		before := sc.Now()

		sc.SleepUntil(before + 1000)

		if got := sc.Now(); got != before {
			t.Errorf("Now() = %d after SleepUntil, want unchanged %d", got, before)
		}
	})

	t.Run("new_timer_does_not_fire_before_its_deadline", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)

		mustNotFire(t, tm)
	})

	t.Run("new_timer_with_non_positive_duration_fires_immediately", func(t *testing.T) {
		sc := &SimClock{}

		tm := sc.NewTimer(0)

		mustFire(t, tm, sc.Now())
	})

	t.Run("advance_wakes_timer_at_exact_deadline", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)

		sc.Advance(100)

		mustFire(t, tm, 100)
	})

	t.Run("advance_wakes_earlier_timer_before_later_one", func(t *testing.T) {
		sc := &SimClock{}
		early := sc.NewTimer(50)
		late := sc.NewTimer(100)

		sc.Advance(60) // due: early (deadline 50); not yet due: late (deadline 100)

		mustFire(t, early, 50)
		mustNotFire(t, late)
	})

	t.Run("advance_fires_all_timers_due_by_the_new_time", func(t *testing.T) {
		sc := &SimClock{}
		a := sc.NewTimer(10)
		b := sc.NewTimer(20)

		sc.Advance(20)

		mustFire(t, a, 10)
		mustFire(t, b, 20)
	})

	// Two timers with the identical deadline both send that same deadline
	// value on their own channel, so channel content alone cannot show
	// which one Advance's firing loop reached first. Instead this checks
	// the sorted-slice invariant Advance's firing loop is defined to rely
	// on (see simclock.go): insertion order for a tied deadline is by
	// ascending registration ID, never goroutine identity or map order.
	t.Run("advance_breaks_tie_by_registration_order", func(t *testing.T) {
		sc := &SimClock{}
		first := sc.NewTimer(100).(*simTimer)
		second := sc.NewTimer(100).(*simTimer)

		if len(sc.timers) != 2 {
			t.Fatalf("len(sc.timers) = %d, want 2", len(sc.timers))
		}
		if sc.timers[0].id != first.id || sc.timers[1].id != second.id {
			t.Errorf("tied timers sorted as ids [%d %d], want [%d %d]",
				sc.timers[0].id, sc.timers[1].id, first.id, second.id)
		}
	})

	t.Run("stop_before_deadline_reports_active_and_prevents_firing", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)

		active := tm.Stop()

		if !active {
			t.Error("Stop() = false, want true for a timer stopped before its deadline")
		}
		sc.Advance(1000)
		mustNotFire(t, tm)
	})

	t.Run("stop_after_firing_reports_inactive", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)
		sc.Advance(100)

		active := tm.Stop()

		if active {
			t.Error("Stop() = true, want false for a timer that already fired")
		}
	})

	t.Run("reset_reschedules_to_a_new_deadline", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)

		tm.Reset(50)

		sc.Advance(50)
		mustFire(t, tm, 50)
	})

	t.Run("reset_before_deadline_reports_active", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)

		active := tm.Reset(200)

		if !active {
			t.Error("Reset() = false, want true for a timer reset before its deadline")
		}
	})

	t.Run("reset_after_firing_reports_inactive_but_still_reschedules", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100)
		sc.Advance(100)

		active := tm.Reset(50)

		if active {
			t.Error("Reset() = true, want false for a timer that already fired")
		}
		sc.Advance(50)
		mustFire(t, tm, 150)
	})

	t.Run("reset_keeps_the_original_registration_id", func(t *testing.T) {
		sc := &SimClock{}
		tm := sc.NewTimer(100).(*simTimer)
		id := tm.id

		tm.Reset(200)

		if tm.id != id {
			t.Errorf("id changed from %d to %d across Reset", id, tm.id)
		}
	})
}

func mustFire(t *testing.T, tm Timer, want int64) {
	t.Helper()
	select {
	case got := <-tm.C():
		if got != want {
			t.Errorf("fired value = %d, want %d", got, want)
		}
	default:
		t.Fatal("timer did not fire")
	}
}

func mustNotFire(t *testing.T, tm Timer) {
	t.Helper()
	select {
	case got := <-tm.C():
		t.Fatalf("timer fired unexpectedly with value %d", got)
	default:
	}
}
