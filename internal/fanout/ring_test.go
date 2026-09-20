package fanout

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

func TestNewRing(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{"a_capacity_that_is_not_a_power_of_two_is_rejected", Config{Capacity: 3, MaxBlobBytes: 0}, ErrCapacity},
		{"a_capacity_of_zero_is_rejected", Config{Capacity: 0, MaxBlobBytes: 0}, ErrCapacity},
		{"a_capacity_of_one_is_rejected", Config{Capacity: 1, MaxBlobBytes: 0}, ErrCapacity},
		{"a_negative_capacity_is_rejected", Config{Capacity: -2, MaxBlobBytes: 0}, ErrCapacity},
		{"a_negative_max_blob_bytes_is_rejected", Config{Capacity: 2, MaxBlobBytes: -1}, ErrMaxBlobBytes},
		{"a_power_of_two_capacity_is_accepted", Config{Capacity: 2, MaxBlobBytes: 0}, nil},
		{"a_larger_power_of_two_capacity_is_accepted", Config{Capacity: 1024, MaxBlobBytes: 64}, nil},
		{"zero_max_blob_bytes_is_accepted", Config{Capacity: 2, MaxBlobBytes: 0}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewRing(tt.cfg)

			if tt.want != nil {
				if err != tt.want {
					t.Fatalf("NewRing() error = %v, want %v", err, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRing() error = %v, want nil", err)
			}
			if r.Capacity() != tt.cfg.Capacity {
				t.Errorf("Capacity() = %d, want %d", r.Capacity(), tt.cfg.Capacity)
			}
		})
	}
}

// deltaRecord returns a small, valid non-snapshot record for tests that
// do not care about field values beyond having some.
func deltaRecord(exchangeTs int64, seq uint64) store.Record {
	return store.Record{
		ExchangeTs:     exchangeTs,
		SequenceNumber: seq,
		InstrumentID:   7,
		VenueID:        3,
		RecordType:     store.RecordTypeDelta,
		SideFlags:      store.SideBid,
		Price:          100,
		Size:           5,
	}
}

// snapshotRecord returns a valid snapshot-pointer record together with
// the blob payload store.Reader.Blob would have returned for it (the
// four-byte checksum stripped, per docs/format.md).
func snapshotRecord(exchangeTs int64, seq uint64, payload []byte) store.Record {
	return store.Record{
		ExchangeTs:     exchangeTs,
		SequenceNumber: seq,
		InstrumentID:   7,
		VenueID:        3,
		RecordType:     store.RecordTypeSnapshotPointer,
		BlobLen:        uint32(len(payload)) + 4,
		LevelCount:     uint16((len(payload) - 4) / 16),
	}
}

func TestRingWriteAndRead(t *testing.T) {
	t.Run("a_written_slot_reads_back_every_record_field", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		want := deltaRecord(1000, 42)

		n, err := r.Write(want, nil)
		if err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}
		if n != 0 {
			t.Fatalf("Write() index = %d, want 0", n)
		}

		var got store.Record
		blob, held, complete := r.read(0, &got, nil)
		if !complete {
			t.Fatalf("read(0) complete = false, want true")
		}
		if held != 0 {
			t.Errorf("read(0) heldIdx = %d, want 0", held)
		}
		if blob != nil {
			t.Errorf("read(0) blob = %v, want nil", blob)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("read(0) record mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("the_emit_index_increases_by_one_per_record", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 8, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}

		for i := 0; i < 20; i++ {
			n, err := r.Write(deltaRecord(int64(i), uint64(i)), nil)
			if err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}
			if n != uint64(i) {
				t.Fatalf("Write() index = %d, want %d", n, i)
			}
			if got := r.WriteIndex(); got != uint64(i+1) {
				t.Errorf("WriteIndex() = %d, want %d", got, i+1)
			}
		}
	})

	t.Run("a_slot_the_writer_has_not_reached_reads_as_incomplete", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}

		var rec store.Record
		_, held, complete := r.read(0, &rec, nil)
		if complete {
			t.Fatalf("read(0) complete = true on an unwritten ring, want false")
		}
		if held != 0 {
			t.Errorf("read(0) heldIdx = %d, want 0 (the slot's zero value holds seq 0, index 0, mid-write)", held)
		}
	})

	t.Run("a_lapped_index_reads_as_incomplete_and_reports_the_newer_index", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := r.Write(deltaRecord(int64(i), uint64(i)), nil); err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}
		}

		var rec store.Record
		_, held, complete := r.read(0, &rec, nil)
		if complete {
			t.Fatalf("read(0) complete = true after 5 writes to a capacity-2 ring, want false")
		}
		// Index 0 shares a slot with indexes 2 and 4 (capacity 2). The
		// slot's last writer was index 4.
		if held != 4 {
			t.Errorf("read(0) heldIdx = %d, want 4", held)
		}
	})

	t.Run("a_snapshot_blob_round_trips_through_the_arena", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 64})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := []byte{
			0xAA, 0xBB, 0xCC, 0xDD, // checksum (opaque to the ring)
			0x01, 0x00, 0x00, 0x00, // one level
			1, 2, 3, 4, 5, 6, 7, 8, // price
			8, 7, 6, 5, 4, 3, 2, 1, // size
		}
		rec := snapshotRecord(500, 1, payload)

		if _, err := r.Write(rec, payload); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}

		var got store.Record
		blob, _, complete := r.read(0, &got, nil)
		if !complete {
			t.Fatalf("read(0) complete = false, want true")
		}
		if diff := cmp.Diff(payload, blob); diff != "" {
			t.Errorf("blob mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a_blob_shorter_than_one_word_round_trips", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 16})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := []byte{1, 2, 3}
		rec := store.Record{
			ExchangeTs: 1, RecordType: store.RecordTypeSnapshotPointer,
			BlobLen: uint32(len(payload)) + 4,
		}

		if _, err := r.Write(rec, payload); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}

		var got store.Record
		blob, _, complete := r.read(0, &got, nil)
		if !complete {
			t.Fatalf("read(0) complete = false, want true")
		}
		if diff := cmp.Diff(payload, blob); diff != "" {
			t.Errorf("blob mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a_reused_destination_buffer_is_not_reallocated", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 32})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := make([]byte, 20)
		for i := range payload {
			payload[i] = byte(i)
		}
		rec := store.Record{ExchangeTs: 1, RecordType: store.RecordTypeSnapshotPointer, BlobLen: uint32(len(payload)) + 4}
		if _, err := r.Write(rec, payload); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}

		dst := make([]byte, 0, 32)
		var got store.Record
		blob, _, complete := r.read(0, &got, dst)
		if !complete {
			t.Fatalf("read(0) complete = false, want true")
		}
		// dst has enough capacity, so read must reslice it rather than
		// allocate. dst[:1] reslices the same backing array up to its
		// existing capacity, purely to take &dst[:1][0]'s address — dst
		// itself is never read.
		if &blob[0] != &dst[:1][0] {
			t.Errorf("read(0) allocated a new blob buffer instead of reusing dst")
		}
	})

	t.Run("a_blob_larger_than_the_arena_is_rejected", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 8})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := make([]byte, 16)
		rec := store.Record{ExchangeTs: 1, RecordType: store.RecordTypeSnapshotPointer, BlobLen: uint32(len(payload)) + 4}

		if _, err := r.Write(rec, payload); err != ErrBlobTooLarge {
			t.Fatalf("Write() error = %v, want ErrBlobTooLarge", err)
		}
	})

	t.Run("the_slot_round_trips_every_record_field", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		want := store.Record{
			ExchangeTs:     0x0A0B0C0D0E0F1011,
			SequenceNumber: 0x2021222324252627,
			InstrumentID:   0x31323334,
			VenueID:        0x4142,
			RecordType:     store.RecordTypeTrade,
			SideFlags:      store.SideAsk,
			Price:          -12345,
			Size:           67890,
		}

		if _, err := r.Write(want, nil); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}

		var got store.Record
		_, _, complete := r.read(0, &got, nil)
		if !complete {
			t.Fatalf("read(0) complete = false, want true")
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("round trip mismatch (-want +got):\n%s", diff)
		}
	})
}
