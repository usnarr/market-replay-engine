package fanout

import (
	"sync/atomic"
	"testing"

	"replay/internal/clock"
)

// stepClock is a deterministic clock.Clock for testing the pacing_slip
// gauge: Now advances by a fixed step on every call, with no real time
// and no goroutine needed to drive it — unlike SimClock, whose Now
// never moves without an explicit Advance from another goroutine, which
// a single-threaded gauge test has no natural place to call from.
// SleepUntil and NewTimer are not exercised by this commit.
type stepClock struct {
	n    atomic.Int64
	step int64
}

func (c *stepClock) Now() int64                   { return c.n.Add(c.step) }
func (c *stepClock) SleepUntil(deadline int64)    { panic("stepClock: SleepUntil not used by this test") }
func (c *stepClock) NewTimer(d int64) clock.Timer { panic("stepClock: NewTimer not used by this test") }

func TestPacingSlip(t *testing.T) {
	t.Run("the_gauge_stays_zero_when_no_block_subscriber_lags", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0, Clock: &stepClock{step: 1000}})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		for i := 0; i < 500; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
			if _, ok, err := sub.Next(); err != nil || !ok {
				t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
			}
		}

		if got := r.PacingSlipNanos(); got != 0 {
			t.Fatalf("PacingSlipNanos() = %d, want 0: capacity 1024 with a subscriber reading every record never engages the barrier", got)
		}
	})

	t.Run("the_gauge_grows_while_a_block_subscriber_lags", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0, Clock: &stepClock{step: 1000}})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if got := r.PacingSlipNanos(); got != 0 {
			t.Fatalf("PacingSlipNanos() before any write = %d, want 0", got)
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
		assertStillBlocked(t, write2Done, "Write(2) returned before the Block subscriber read index 0")

		if _, ok, err := sub.Next(); err != nil || !ok {
			t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}
		<-write2Done

		// The wait reads the clock exactly twice — once on entry, once
		// on release — so one wait cycle advances the gauge by exactly
		// one step, regardless of how many Gosched spins happened in
		// between.
		if got := r.PacingSlipNanos(); got != 1000 {
			t.Fatalf("PacingSlipNanos() = %d, want exactly 1000 (one wait cycle's step)", got)
		}
	})
}
