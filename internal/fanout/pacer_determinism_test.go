package fanout

import (
	"sync/atomic"
	"testing"

	"replay/internal/clock"
	"replay/internal/merge"
	"replay/internal/synth"
)

// countingResetTimer wraps a real clock.Timer and counts how many times
// Reset was called on it.
type countingResetTimer struct {
	clock.Timer
	calls *atomic.Int64
}

func (t *countingResetTimer) Reset(d int64) bool {
	t.calls.Add(1)
	return t.Timer.Reset(d)
}

// countingResetClock wraps a real clock.Clock and returns timers that
// count their own Reset calls — the direct evidence that a Pacer's wait
// never genuinely armed its timer, as opposed to inferring it from
// elapsed time.
type countingResetClock struct {
	clock.Clock
	resetCalls atomic.Int64
}

func (c *countingResetClock) NewTimer(d int64) clock.Timer {
	return &countingResetTimer{Timer: c.Clock.NewTimer(d), calls: &c.resetCalls}
}

// runPacedForHash replays ds through a real Merger, paced by a Pacer at
// num/den, into a single Block subscriber sized to receive the whole
// dataset with no lapping, and returns that subscriber's hash and count.
func runPacedForHash(t *testing.T, ds *synth.Dataset, num, den int64) (subResult, *countingResetClock) {
	t.Helper()

	cc := &countingResetClock{Clock: &clock.SimClock{}}
	s, err := NewSpeed(num, den)
	if err != nil {
		t.Fatalf("NewSpeed(%d, %d) error = %v, want nil", num, den, err)
	}
	p := NewPacer(cc, s)

	r, err := NewRing(Config{Capacity: 8192, MaxBlobBytes: 256, Clock: cc})
	if err != nil {
		t.Fatalf("NewRing() error = %v, want nil", err)
	}
	sub, err := r.Subscribe(ModeBlock, Beginning())
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want nil", err)
	}

	m, err := merge.NewMerger(openCursors(t, ds))
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- r.RunPaced(m, p) }()

	got := drainSubscriber(t, sub, nil)

	if err := <-runDone; err != nil {
		t.Fatalf("RunPaced() error = %v, want nil", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Merger.Close() error = %v, want nil", err)
	}
	return got, cc
}

// TestDeterminismPacing is the pacing content-invariance test
// plans/08-pacing.md and plans/12-testing.md both call for: the same
// dataset through the full merge -> pacer -> ring -> subscriber
// pipeline at 1x and 10,000x, under SimClock, must produce an identical
// canonical hash.
//
// Under SimClock, measureWindow measures unbounded (SimClock's Now
// never advances on its own — see measureWindow's own doc comment), so
// Pacer.Wait takes its fast path on every call at both speeds and the
// pacer's timer is never armed at either one. This test therefore
// genuinely proves: the pacer is wired into the emit loop in a way that
// cannot reorder, drop, duplicate, or coalesce records, and the numeric
// speed value cannot leak into content, at any speed. It does not, and
// cannot, prove that 10,000x under a real clock produces the same
// content as 1x under real waiting, because neither run here waits at
// all — that is a structural property of SimClock, not a limitation of
// this test's design, and real-clock accuracy has its own dedicated
// test (BenchmarkPacingAccuracy, once it lands) and BENCHMARKS.md
// section instead.
func TestDeterminismPacing(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)

	t.Run("identical_hash_and_count_at_1x_and_10000x", func(t *testing.T) {
		slow, slowClk := runPacedForHash(t, ds, 1, 1)
		fast, fastClk := runPacedForHash(t, ds, 10000, 1)

		if slow.received == 0 {
			t.Fatal("the 1x run received 0 records; this test proves nothing")
		}
		if slow.received != fast.received {
			t.Fatalf("1x received %d records, 10000x received %d, want equal", slow.received, fast.received)
		}
		if len(slow.gaps) != 0 || len(fast.gaps) != 0 {
			t.Fatalf("a Block subscriber reported a gap (1x: %d, 10000x: %d), want none", len(slow.gaps), len(fast.gaps))
		}
		if slow.hash != fast.hash {
			t.Fatalf("1x hash = %s, 10000x hash = %s, want equal", slow.hash, fast.hash)
		}

		if got := slowClk.resetCalls.Load(); got != 0 {
			t.Errorf("1x run: timer.Reset was called %d times, want 0: a SimClock pacer never waits", got)
		}
		if got := fastClk.resetCalls.Load(); got != 0 {
			t.Errorf("10000x run: timer.Reset was called %d times, want 0: a SimClock pacer never waits", got)
		}
	})

	t.Run("matches_the_unpaced_merged_stream", func(t *testing.T) {
		// The paced hash must also agree with a plain, no-pacer replay
		// of the same dataset — pacing must never change content on its
		// own, independent of whatever it agrees with itself across
		// speeds.
		want := computeWant(t, ds)
		got, _ := runPacedForHash(t, ds, 1, 1)

		if got.received != want.count {
			t.Fatalf("paced run received %d records, want %d", got.received, want.count)
		}
		if got.hash != want.hash {
			t.Fatalf("paced hash = %s, want %s (the unpaced merged stream's hash)", got.hash, want.hash)
		}
	})
}
