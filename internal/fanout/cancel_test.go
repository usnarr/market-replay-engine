package fanout

import "testing"

// nextResult is what one out-of-band-stopped call to Next returned. The
// goroutine that fills it publishes it by closing the channel the test
// then receives from, so the test reads it only after that happens-before
// edge.
type nextResult struct {
	ok  bool
	err error
}

func TestSubscriberCancel(t *testing.T) {
	t.Run("a_parked_subscriber_unblocks_and_leaves_another_subscribers_stream_untouched", func(t *testing.T) {
		const (
			beforeCancel = 4
			total        = 12
		)
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		target, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		bystander, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		for i := 0; i < beforeCancel; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}
		var bystanderSeqs []uint64
		for i := 0; i < beforeCancel; i++ {
			if _, ok, err := target.Next(); !ok || err != nil {
				t.Fatalf("target.Next() = (ok=%v, err=%v), want a delivery", ok, err)
			}
			d, ok, err := bystander.Next()
			if !ok || err != nil {
				t.Fatalf("bystander.Next() = (ok=%v, err=%v), want a delivery", ok, err)
			}
			bystanderSeqs = append(bystanderSeqs, d.Record.SequenceNumber)
		}

		var parked nextResult
		parkedDone := make(chan struct{})
		go func() {
			defer close(parkedDone)
			_, parked.ok, parked.err = target.Next()
		}()
		// A bounded poll proves the negative -- that Next is genuinely
		// parked and not about to return on its own. The unconditional
		// receive below proves the positive, bounded only by the test's
		// own -timeout, never by a poll count.
		assertStillBlocked(t, parkedDone, "target.Next() returned before anything was written and before Cancel")
		target.Cancel()
		<-parkedDone

		if parked.ok || parked.err != ErrCanceled {
			t.Errorf("Next() on a canceled subscriber = (ok=%v, err=%v), want (false, ErrCanceled)", parked.ok, parked.err)
		}

		for i := beforeCancel; i < total; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}
		r.SetEnd(r.WriteIndex())

		if _, ok, err := target.Next(); ok || err != ErrCanceled {
			t.Errorf("Next() after more records and SetEnd = (ok=%v, err=%v), want (false, ErrCanceled): cancellation is final", ok, err)
		}
		for {
			d, ok, err := bystander.Next()
			if err != nil {
				t.Fatalf("bystander.Next() error = %v, want nil", err)
			}
			if !ok {
				break
			}
			if d.HasGap {
				t.Errorf("bystander was handed gap %+v; another subscriber's cancellation must not drop its records", d.Gap)
			}
			bystanderSeqs = append(bystanderSeqs, d.Record.SequenceNumber)
		}
		if len(bystanderSeqs) != total {
			t.Fatalf("bystander received %d records, want %d", len(bystanderSeqs), total)
		}
		for i, got := range bystanderSeqs {
			if got != uint64(i) {
				t.Errorf("bystander record %d has SequenceNumber %d, want %d", i, got, uint64(i))
			}
		}
	})

	t.Run("unsubscribe_releases_a_drop_subscriber_parked_in_next", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeDrop, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		var parked nextResult
		parkedDone := make(chan struct{})
		go func() {
			defer close(parkedDone)
			_, parked.ok, parked.err = sub.Next()
		}()
		assertStillBlocked(t, parkedDone, "Next() returned before anything was written and before Unsubscribe")
		if err := r.Unsubscribe(sub); err != nil {
			t.Fatalf("Unsubscribe() error = %v, want nil", err)
		}
		<-parkedDone

		if parked.ok || parked.err != ErrCanceled {
			t.Errorf("Next() after Unsubscribe() = (ok=%v, err=%v), want (false, ErrCanceled)", parked.ok, parked.err)
		}
	})

	t.Run("unsubscribe_releases_a_block_subscriber_parked_mid_run", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		r.StartEmitting()

		var parked nextResult
		parkedDone := make(chan struct{})
		go func() {
			defer close(parkedDone)
			_, parked.ok, parked.err = sub.Next()
		}()
		assertStillBlocked(t, parkedDone, "Next() returned before anything was written and before Unsubscribe")
		unsubDone := make(chan struct{})
		go func() {
			defer close(unsubDone)
			if err := r.Unsubscribe(sub); err != nil {
				t.Errorf("Unsubscribe() error = %v, want nil", err)
			}
		}()
		// Once emitting has started, a leave is applied by the emit loop
		// between records; this test is that loop.
		waitForPending(r)
		r.applyControl()
		<-unsubDone
		<-parkedDone

		if parked.ok || parked.err != ErrCanceled {
			t.Errorf("Next() after Unsubscribe() = (ok=%v, err=%v), want (false, ErrCanceled)", parked.ok, parked.err)
		}
	})
}
