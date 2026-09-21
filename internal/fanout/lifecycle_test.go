package fanout

import (
	"runtime"
	"testing"
)

// waitForPending blocks, without reading a clock, until at least one
// control request is visible in r's queue. Tests use it to eliminate
// the race between "a goroutine has called Subscribe/Unsubscribe" and
// "the driving loop has started applying control" — without it, a fast
// driving loop could finish and close the queue before a concurrently
// launched request ever arrives.
func waitForPending(r *Ring) {
	for {
		r.ctrl.mu.Lock()
		n := len(r.ctrl.pending)
		r.ctrl.mu.Unlock()
		if n > 0 {
			return
		}
		runtime.Gosched()
	}
}

// driveEmit is a minimal stand-in for a real emit loop's use of the
// control queue: apply pending control between every record, write the
// record, repeat. This exercises exactly the applyControl/Write
// integration a later commit's merge.Merger-driven Run would use,
// without this package's own unit tests needing a real Merger.
func driveEmit(t *testing.T, r *Ring, records []uint64) {
	t.Helper()
	for _, seq := range records {
		r.applyControl()
		if _, err := r.Write(seqRecord(seq), nil); err != nil {
			t.Fatalf("Write(%d) error = %v, want nil", seq, err)
		}
	}
	r.applyControl()
	r.SetEnd(r.WriteIndex())
}

func TestSubscriberLifecycle(t *testing.T) {
	t.Run("a_subscriber_set_fixed_before_the_first_record_needs_no_control_queue", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		if _, err := r.Subscribe(ModeDrop, Beginning()); err != nil {
			t.Fatalf("Subscribe() before StartEmitting error = %v, want nil", err)
		}
		if got := len(r.ctrl.pending); got != 0 {
			t.Fatalf("pending control requests = %d, want 0: Subscribe before StartEmitting must resolve immediately", got)
		}
	})

	t.Run("a_subscriber_joining_mid_run_at_an_emit_index_receives_exactly_that_suffix", func(t *testing.T) {
		const total = 200
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		fromStart, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		r.StartEmitting()

		const joinAt = 50
		joinDone := make(chan struct{})
		var joined *Subscriber
		var joinErr error
		go func() {
			defer close(joinDone)
			joined, joinErr = r.Subscribe(ModeBlock, EmitIndex(joinAt))
		}()
		waitForPending(r) // the join must be enqueued before the run can finish

		records := make([]uint64, total)
		for i := range records {
			records[i] = uint64(i)
		}
		driveEmit(t, r, records)
		<-joinDone

		if joinErr != nil {
			t.Fatalf("mid-run Subscribe() error = %v, want nil", joinErr)
		}

		var fromStartSuffix, joinedAll []uint64
		for {
			d, ok, err := fromStart.Next()
			if err != nil {
				t.Fatalf("fromStart.Next() error = %v, want nil", err)
			}
			if !ok {
				break
			}
			if d.Record.SequenceNumber >= joinAt {
				fromStartSuffix = append(fromStartSuffix, d.Record.SequenceNumber)
			}
		}
		for {
			d, ok, err := joined.Next()
			if err != nil {
				t.Fatalf("joined.Next() error = %v, want nil", err)
			}
			if !ok {
				break
			}
			if d.HasGap {
				t.Fatalf("joined subscriber reported a gap %+v; it started exactly at the live edge it asked for", d.Gap)
			}
			joinedAll = append(joinedAll, d.Record.SequenceNumber)
		}

		if len(joinedAll) != len(fromStartSuffix) {
			t.Fatalf("joined subscriber received %d records, want %d (the suffix from %d)", len(joinedAll), len(fromStartSuffix), joinAt)
		}
		for i := range joinedAll {
			if joinedAll[i] != fromStartSuffix[i] {
				t.Fatalf("record %d: joined got SequenceNumber %d, from-start suffix has %d", i, joinedAll[i], fromStartSuffix[i])
			}
		}
	})

	t.Run("a_subscriber_that_leaves_mid_run_stops_receiving_and_releases_a_parked_writer", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		staying, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		leaving, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		r.StartEmitting()

		// Fill capacity so the next write engages the barrier, then let
		// staying read ahead so only leaving is holding the writer back.
		for i := 0; i < 2; i++ {
			r.applyControl()
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}
		if _, ok, err := staying.Next(); err != nil || !ok {
			t.Fatalf("staying.Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}

		write2Done := make(chan struct{})
		go func() {
			defer close(write2Done)
			if _, err := r.Write(seqRecord(2), nil); err != nil {
				t.Errorf("Write(2) error = %v, want nil", err)
			}
		}()
		assertStillBlocked(t, write2Done, "Write(2) returned before leaving's cursor moved past index 0")

		if err := r.Unsubscribe(leaving); err != nil {
			t.Fatalf("Unsubscribe() error = %v, want nil", err)
		}
		<-write2Done // must complete once leaving no longer counts toward the barrier

		if got := r.WriteIndex(); got != 3 {
			t.Fatalf("WriteIndex() = %d, want 3", got)
		}
	})

	t.Run("join_and_leave_are_applied_between_records_never_inside_one", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		r.StartEmitting()

		// Queue a join before any record is written; it must be
		// resolved by applyControl, and see a cursor consistent with a
		// specific record boundary, never a value that could only exist
		// mid-write.
		joinDone := make(chan struct{})
		var joined *Subscriber
		go func() {
			defer close(joinDone)
			joined, _ = r.Subscribe(ModeDrop, Live())
		}()
		waitForPending(r) // the join must be enqueued before the run can finish

		for i := 0; i < 100; i++ {
			runtime.Gosched()
			r.applyControl()
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write(%d) error = %v, want nil", i, err)
			}
		}
		<-joinDone
		r.SetEnd(r.WriteIndex())

		cursor := joined.Cursor()
		if cursor > 100 {
			t.Fatalf("joined subscriber's resolved cursor %d exceeds the number of records ever written", cursor)
		}
		// Whatever record boundary the join landed on, everything from
		// there on must be received with no gap.
		for {
			d, ok, err := joined.Next()
			if err != nil {
				t.Fatalf("Next() error = %v, want nil", err)
			}
			if !ok {
				break
			}
			if d.HasGap {
				t.Fatalf("Live join reported a gap %+v; a join can only land exactly on a record boundary", d.Gap)
			}
		}
	})

	t.Run("a_join_after_the_stream_ends_is_rejected", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 8, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		r.StartEmitting()
		r.SetEnd(0)

		if _, err := r.Subscribe(ModeDrop, Beginning()); err != ErrClosed {
			t.Fatalf("Subscribe() after SetEnd error = %v, want ErrClosed", err)
		}
	})
}
