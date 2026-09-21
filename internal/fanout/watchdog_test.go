package fanout

import (
	"errors"
	"testing"

	"replay/internal/clock"
)

func TestWatchdog(t *testing.T) {
	t.Run("a_block_subscriber_that_stops_reading_is_evicted_after_the_timeout", func(t *testing.T) {
		sc := &clock.SimClock{}
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0, Clock: sc, WatchdogTimeout: 1000})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		stalled, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		for i := 0; i < 2; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}

		write2Done := make(chan struct{})
		go func() {
			defer close(write2Done)
			if _, err := r.Write(seqRecord(2), nil); err != nil {
				t.Errorf("Write(2) error = %v, want nil", err)
			}
		}()
		assertStillBlocked(t, write2Done, "Write(2) returned before the timeout elapsed")

		sc.Advance(1001) // exceeds WatchdogTimeout, the stalled subscriber is never going to read
		<-write2Done     // must complete once the stalled subscriber is evicted

		if got := r.WriteIndex(); got != 3 {
			t.Fatalf("WriteIndex() = %d, want 3", got)
		}

		_, ok, err := stalled.Next()
		if ok {
			t.Fatalf("stalled.Next() ok = true, want false")
		}
		if !errors.Is(err, ErrEvicted) {
			t.Fatalf("stalled.Next() error = %v, want ErrEvicted", err)
		}
	})

	t.Run("the_writer_resumes_once_the_stalled_subscriber_is_evicted", func(t *testing.T) {
		sc := &clock.SimClock{}
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0, Clock: sc, WatchdogTimeout: 1000})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		if _, err := r.Subscribe(ModeBlock, Beginning()); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		for i := 0; i < 2; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}

		// The rest run in a goroutine: index 2 blocks immediately, and
		// nothing ever reads again, so every index from there on is
		// gated by the same single eviction.
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			for i := 2; i < 20; i++ {
				if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
					t.Errorf("Write(%d) error = %v, want nil", i, err)
					return
				}
			}
		}()
		assertStillBlocked(t, writeDone, "the writer finished before the watchdog timeout elapsed")

		// windowStart was captured when the wait began, before this
		// advance, so a single step past the timeout is enough: once
		// index 2's write evicts the only Block subscriber, r.blocking
		// is empty and every later write passes the barrier without
		// touching the clock again.
		sc.Advance(1001)
		<-writeDone

		if got := r.WriteIndex(); got != 20 {
			t.Fatalf("WriteIndex() = %d, want 20", got)
		}
	})

	t.Run("a_block_subscriber_that_keeps_reading_is_never_evicted", func(t *testing.T) {
		sc := &clock.SimClock{}
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0, Clock: sc, WatchdogTimeout: 1000})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		const total = 500
		for i := 0; i < total; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
			sc.Advance(2000) // well past the timeout, but this subscriber is never stalled
			if _, ok, err := sub.Next(); err != nil || !ok {
				t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
			}
		}
	})

	t.Run("a_stalled_drop_subscriber_is_never_evicted", func(t *testing.T) {
		sc := &clock.SimClock{}
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0, Clock: sc, WatchdogTimeout: 1000})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		if _, err := r.Subscribe(ModeDrop, Beginning()); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		// A Drop subscriber is never in r.blocking, so it can never
		// engage the barrier or the watchdog: the writer must run to
		// completion immediately, with no advance needed at all.
		for i := 0; i < 1000; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}
		if got := r.WriteIndex(); got != 1000 {
			t.Fatalf("WriteIndex() = %d, want 1000", got)
		}
	})

	t.Run("a_zero_watchdog_timeout_disables_eviction", func(t *testing.T) {
		sc := &clock.SimClock{}
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0, Clock: sc}) // WatchdogTimeout left at zero
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		for i := 0; i < 2; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}

		write2Done := make(chan struct{})
		go func() {
			defer close(write2Done)
			if _, err := r.Write(seqRecord(2), nil); err != nil {
				t.Errorf("Write(2) error = %v, want nil", err)
			}
		}()

		sc.Advance(1_000_000_000)
		assertStillBlocked(t, write2Done, "Write(2) returned with the watchdog disabled: nothing should ever evict the stalled subscriber")

		// Release the writer goroutine deliberately, so it does not
		// keep spinning for the rest of the test binary's life: with
		// the watchdog disabled, nothing else ever will.
		if _, ok, err := sub.Next(); err != nil || !ok {
			t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}
		<-write2Done
	})
}
