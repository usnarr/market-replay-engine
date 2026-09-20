package fanout

import "testing"

func writeN(t *testing.T, r *Ring, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}
	}
}

func TestSubscribe(t *testing.T) {
	t.Run("an_unset_backpressure_mode_is_rejected", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})

		if _, err := r.Subscribe(ModeUnset, Beginning()); err != ErrModeUnset {
			t.Fatalf("Subscribe() error = %v, want ErrModeUnset", err)
		}
	})

	t.Run("an_unset_start_position_is_rejected", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})

		if _, err := r.Subscribe(ModeBlock, StartAt{}); err != ErrStartUnset {
			t.Fatalf("Subscribe() error = %v, want ErrStartUnset", err)
		}
	})

	t.Run("a_block_subscriber_starting_before_the_oldest_record_is_rejected", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		writeN(t, r, 10) // capacity 2: only indexes 8, 9 remain

		if _, err := r.Subscribe(ModeBlock, EmitIndex(0)); err != ErrStartLapped {
			t.Fatalf("Subscribe() error = %v, want ErrStartLapped", err)
		}
		if _, err := r.Subscribe(ModeBlock, Beginning()); err != ErrStartLapped {
			t.Fatalf("Subscribe() error = %v, want ErrStartLapped", err)
		}
	})

	t.Run("a_drop_subscriber_starting_before_the_oldest_record_reports_the_gap_on_first_next", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		writeN(t, r, 10) // capacity 2: only indexes 8, 9 remain

		sub, err := r.Subscribe(ModeDrop, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		d, ok, err := sub.Next()
		if err != nil || !ok {
			t.Fatalf("Next() = (%+v, %v, %v), want a delivery", d, ok, err)
		}
		if !d.HasGap {
			t.Fatalf("Next() HasGap = false, want true")
		}
		if d.Gap.FirstMissedIndex != 0 || d.Gap.LastMissedIndex != 7 || d.Gap.Count != 8 {
			t.Errorf("Next() Gap = %+v, want {First:0 Last:7 Count:8}", d.Gap)
		}
		if d.Record.SequenceNumber != 8 {
			t.Errorf("Next() delivered SequenceNumber %d, want 8", d.Record.SequenceNumber)
		}
	})

	t.Run("a_start_index_in_the_future_waits_for_the_writer", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 8, MaxBlobBytes: 0})

		sub, err := r.Subscribe(ModeBlock, EmitIndex(3))
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		done := make(chan struct{})
		var got Delivery
		go func() {
			defer close(done)
			d, ok, err := sub.Next()
			if err != nil || !ok {
				t.Errorf("Next() = (%+v, %v, %v), want a delivery", d, ok, err)
				return
			}
			got = d
		}()

		writeN(t, r, 4) // writes indexes 0..3; sub is waiting on 3
		<-done

		if got.Record.SequenceNumber != 3 {
			t.Errorf("Next() delivered SequenceNumber %d, want 3", got.Record.SequenceNumber)
		}
		if got.HasGap {
			t.Errorf("Next() HasGap = true, want false: nothing was skipped")
		}
	})

	t.Run("live_starts_at_the_current_write_index", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 8, MaxBlobBytes: 0})
		writeN(t, r, 5)

		sub, err := r.Subscribe(ModeDrop, Live())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if got := sub.Cursor(); got != 5 {
			t.Fatalf("Cursor() = %d, want 5", got)
		}

		if _, err := r.Write(seqRecord(5), nil); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}
		d, ok, err := sub.Next()
		if err != nil || !ok {
			t.Fatalf("Next() = (%+v, %v, %v), want a delivery", d, ok, err)
		}
		if d.Record.SequenceNumber != 5 {
			t.Errorf("Next() delivered SequenceNumber %d, want 5", d.Record.SequenceNumber)
		}
	})

	t.Run("an_exchange_ts_start_skips_every_earlier_record", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 32, MaxBlobBytes: 0})
		writeN(t, r, 10) // ExchangeTs == SequenceNumber == index, from seqRecord

		sub, err := r.Subscribe(ModeDrop, ExchangeTs(7))
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		d, ok, err := sub.Next()
		if err != nil || !ok {
			t.Fatalf("Next() = (%+v, %v, %v), want a delivery", d, ok, err)
		}
		if d.HasGap {
			t.Errorf("Next() HasGap = true, want false: this subscriber never wanted the earlier records at all")
		}
		if d.Record.SequenceNumber != 7 {
			t.Errorf("Next() delivered SequenceNumber %d, want 7", d.Record.SequenceNumber)
		}
	})

	t.Run("an_exchange_ts_with_no_matching_record_yet_waits_from_the_live_edge", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 32, MaxBlobBytes: 0})
		writeN(t, r, 3) // ExchangeTs 0, 1, 2

		sub, err := r.Subscribe(ModeDrop, ExchangeTs(100))
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if got := sub.Cursor(); got != 3 {
			t.Fatalf("Cursor() = %d, want 3 (the live edge, since exchange_ts is non-decreasing)", got)
		}
	})

	t.Run("a_subscriber_snapshot_blob_is_preallocated_to_the_rings_max_blob_bytes", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 4, MaxBlobBytes: 64})

		sub, err := r.Subscribe(ModeDrop, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if cap(sub.blobBuf) != 64 {
			t.Errorf("blobBuf cap = %d, want 64", cap(sub.blobBuf))
		}
	})
}

func TestSubscriberNext(t *testing.T) {
	t.Run("a_block_subscriber_reads_every_record_in_order_with_no_gap", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		writeN(t, r, 500)

		sub, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		r.SetEnd(500)

		for i := uint64(0); i < 500; i++ {
			d, ok, err := sub.Next()
			if err != nil {
				t.Fatalf("Next() error = %v, want nil", err)
			}
			if !ok {
				t.Fatalf("Next() ok = false at index %d, want true", i)
			}
			if d.HasGap {
				t.Fatalf("Next() HasGap = true at index %d, want false", i)
			}
			if d.Record.SequenceNumber != i {
				t.Fatalf("Next() delivered SequenceNumber %d, want %d", d.Record.SequenceNumber, i)
			}
		}

		_, ok, err := sub.Next()
		if err != nil {
			t.Fatalf("Next() at end error = %v, want nil", err)
		}
		if ok {
			t.Fatalf("Next() at end ok = true, want false")
		}
	})

	t.Run("a_drop_subscriber_reports_a_gap_after_being_lapped_mid_stream", func(t *testing.T) {
		r, _ := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})

		sub, err := r.Subscribe(ModeDrop, Beginning())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		// Read the first two records before falling behind.
		writeN(t, r, 2)
		for i := 0; i < 2; i++ {
			if _, ok, err := sub.Next(); err != nil || !ok {
				t.Fatalf("Next() error = %v, ok = %v, want a delivery", err, ok)
			}
		}

		// Now write far enough ahead (capacity 4) to lap this
		// subscriber, which is parked at cursor 2.
		for i := 2; i < 20; i++ {
			if _, err := r.Write(seqRecord(uint64(i)), nil); err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}
		}

		d, ok, err := sub.Next()
		if err != nil || !ok {
			t.Fatalf("Next() = (ok=%v, err=%v), want a delivery", ok, err)
		}
		if !d.HasGap {
			t.Fatalf("Next() HasGap = false, want true")
		}
		if d.Gap.FirstMissedIndex != 2 {
			t.Errorf("Gap.FirstMissedIndex = %d, want 2", d.Gap.FirstMissedIndex)
		}
		if d.Gap.Count != d.Gap.LastMissedIndex-d.Gap.FirstMissedIndex+1 {
			t.Errorf("Gap.Count = %d, want LastMissedIndex-FirstMissedIndex+1", d.Gap.Count)
		}
		if d.Record.SequenceNumber != d.Gap.LastMissedIndex+1 {
			t.Errorf("delivered SequenceNumber %d, want Gap.LastMissedIndex+1 = %d", d.Record.SequenceNumber, d.Gap.LastMissedIndex+1)
		}
	})
}

