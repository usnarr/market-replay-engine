package fanout

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
)

// assertStillBlocked confirms done has not closed after a bounded number
// of scheduling opportunities. This is deliberately a best-effort check
// in one direction only: it can rarely miss a real bug if an erroneous
// unblock happens to race faster than this loop's own scheduling, but it
// can never fail a correct implementation. Proving a negative about
// concurrent timing this way is sound; proving a positive (a bounded
// poll asserting something DID happen within N iterations) is not — an
// earlier version of this test used exactly that unsound pattern for the
// "does the writer resume" assertion below, and it flaked, because there
// is no true upper bound on how long a goroutine may take to be
// scheduled. That assertion is now an unconditional channel wait
// instead, matching the pattern subscriber_test.go's
// "a_start_index_in_the_future_waits_for_the_writer" already uses.
func assertStillBlocked(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case <-done:
			t.Fatal(msg)
		default:
		}
		runtime.Gosched()
	}
}

func TestBlockBarrier(t *testing.T) {
	t.Run("the_writer_waits_for_the_slowest_block_subscriber", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		// The first capacity writes never wait: their slots hold
		// nothing yet.
		for i := 0; i < 4; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}

		// Index 4 reuses slot 0, which the subscriber has not read
		// yet, so this write must block until it does.
		write4Done := make(chan struct{})
		go func() {
			defer close(write4Done)
			if _, err := r.Write(seqRecord(4), nil); err != nil {
				t.Errorf("Write(4) error = %v, want nil", err)
			}
		}()
		assertStillBlocked(t, write4Done, "Write(4) returned before any Block subscriber read index 0")

		if _, ok, err := sub.Next(); err != nil || !ok {
			t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}
		<-write4Done // must complete now that the subscriber has moved past index 0
		if got := r.WriteIndex(); got != 5 {
			t.Fatalf("WriteIndex() = %d, want 5", got)
		}

		// Symmetric check one step further: index 5 reuses slot 1,
		// which the subscriber has also not read yet.
		write5Done := make(chan struct{})
		go func() {
			defer close(write5Done)
			if _, err := r.Write(seqRecord(5), nil); err != nil {
				t.Errorf("Write(5) error = %v, want nil", err)
			}
		}()
		assertStillBlocked(t, write5Done, "Write(5) returned before any Block subscriber read index 1")

		if _, ok, err := sub.Next(); err != nil || !ok {
			t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}
		<-write5Done
		if got := r.WriteIndex(); got != 6 {
			t.Fatalf("WriteIndex() = %d, want 6", got)
		}
	})

	t.Run("a_drop_subscriber_never_holds_the_writer", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		if _, err := r.Subscribe(ModeDrop, Beginning()); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		for i := 0; i < 1000; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}
		}
		if got := r.WriteIndex(); got != 1000 {
			t.Fatalf("WriteIndex() = %d, want 1000: a Drop subscriber must never engage the barrier", got)
		}
	})

	t.Run("an_empty_block_set_never_holds_the_writer", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}

		for i := 0; i < 1000; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}
		}
		if got := r.WriteIndex(); got != 1000 {
			t.Fatalf("WriteIndex() = %d, want 1000: no Block subscriber means minBlockCursor is unbounded", got)
		}
	})

	t.Run("two_block_subscribers_at_different_speeds_both_receive_every_record_with_no_gap", func(t *testing.T) {
		const total = 5000
		r, err := NewRing(Config{Capacity: 8, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		subFast, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		subSlow, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < total; i++ {
				if _, err := r.Write(seqRecord(i), nil); err != nil {
					t.Errorf("Write() error = %v, want nil", err)
					return
				}
			}
			r.SetEnd(total)
		}()

		drain := func(sub *Subscriber, perturb bool) []uint64 {
			rng := rand.New(rand.NewPCG(1, 2))
			seqs := make([]uint64, 0, total)
			for {
				if perturb && rng.Uint64()&3 == 0 {
					runtime.Gosched()
				}
				d, ok, err := sub.Next()
				if err != nil {
					t.Errorf("Next() error = %v, want nil", err)
					return nil
				}
				if !ok {
					return seqs
				}
				if d.HasGap {
					t.Errorf("Block subscriber reported a gap %+v; Block promises no loss", d.Gap)
				}
				seqs = append(seqs, d.Record.SequenceNumber)
			}
		}

		var gotFast, gotSlow []uint64
		wg.Add(2)
		go func() { defer wg.Done(); gotFast = drain(subFast, false) }()
		go func() { defer wg.Done(); gotSlow = drain(subSlow, true) }()
		wg.Wait()

		if len(gotFast) != total {
			t.Fatalf("fast subscriber received %d records, want %d", len(gotFast), total)
		}
		if len(gotSlow) != total {
			t.Fatalf("slow subscriber received %d records, want %d", len(gotSlow), total)
		}
		for i := uint64(0); i < total; i++ {
			if gotFast[i] != i {
				t.Fatalf("fast subscriber record %d carried SequenceNumber %d", i, gotFast[i])
			}
			if gotSlow[i] != i {
				t.Fatalf("slow subscriber record %d carried SequenceNumber %d", i, gotSlow[i])
			}
		}
	})
}
