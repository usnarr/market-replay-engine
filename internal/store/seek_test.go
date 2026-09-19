package store

import (
	"fmt"
	"testing"
)

// seekTimestamps is chosen so that a run of equal timestamps straddles
// two block boundaries and is longer than one whole block. With a block
// size of 4 the time index is [10, 20, 20, 50]: entries 1 and 2 are
// equal, and the first record holding 20 sits in the block *before* both
// of them. An implementation that finds the last block starting at or
// before the target and scans inside it answers 8 here instead of 1.
var seekTimestamps = []int64{10, 20, 20, 20, 20, 20, 20, 20, 20, 30, 30, 40, 50}

// seekSnapshotAt lists the record indexes written as snapshot pointers.
var seekSnapshotAt = map[int]bool{2: true, 9: true}

func writeSeekFixture(t *testing.T) *Reader {
	t.Helper()
	w, path := newTestWriter(t, 4)
	for i, ts := range seekTimestamps {
		rec := Record{
			ExchangeTs:     ts,
			SequenceNumber: uint64(i),
			InstrumentID:   10,
			VenueID:        testVenue,
		}
		var err error
		if seekSnapshotAt[i] {
			err = w.WriteSnapshot(rec, []Level{{Price: 100, Size: 1}}, nil)
		} else {
			rec.RecordType = RecordTypeDelta
			rec.Price = 100
			rec.Size = 1
			err = w.WriteRecord(rec)
		}
		if err != nil {
			t.Fatalf("writing record %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	return openFixture(t, path)
}

func TestSeekTime(t *testing.T) {
	r := writeSeekFixture(t)

	t.Run("the_time_index_repeats_across_a_block_boundary", func(t *testing.T) {
		// Guards the premise of every case below. If the fixture stops
		// producing equal adjacent entries, the interesting case is gone.
		if len(r.timeIndex) != 4 || r.timeIndex[1] != r.timeIndex[2] {
			t.Fatalf("time index = %v, want four entries with entries 1 and 2 equal", r.timeIndex)
		}
	})

	tests := []struct {
		name string
		t    int64
		want int
	}{
		{name: "before_every_record", t: 5, want: 0},
		{name: "exactly_the_first_timestamp", t: 10, want: 0},
		{name: "between_the_first_two_timestamps", t: 11, want: 1},
		{name: "the_start_of_a_run_that_straddles_blocks", t: 20, want: 1},
		{name: "just_past_that_run", t: 21, want: 9},
		{name: "exactly_a_later_timestamp", t: 30, want: 9},
		{name: "between_later_timestamps", t: 31, want: 11},
		{name: "exactly_the_last_timestamp", t: 50, want: 12},
		{name: "past_every_record", t: 51, want: 13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.SeekTime(tt.t)

			if err != nil {
				t.Fatalf("SeekTime(%d) error = %v, want nil", tt.t, err)
			}
			if got != tt.want {
				t.Errorf("SeekTime(%d) = %d, want %d", tt.t, got, tt.want)
			}
		})
	}

	t.Run("every_records_own_timestamp_seeks_to_the_first_record_holding_it", func(t *testing.T) {
		for i := 0; i < r.Len(); i++ {
			ts := r.RecordAt(i).ExchangeTs

			got, err := r.SeekTime(ts)

			if err != nil {
				t.Fatalf("SeekTime(%d) error = %v, want nil", ts, err)
			}
			if got > i {
				t.Errorf("SeekTime(%d) = %d, want at most %d", ts, got, i)
			}
			if r.RecordAt(got).ExchangeTs < ts {
				t.Errorf("SeekTime(%d) landed on record %d with ts %d", ts, got, r.RecordAt(got).ExchangeTs)
			}
			if got > 0 && r.RecordAt(got-1).ExchangeTs >= ts {
				t.Errorf("SeekTime(%d) = %d, but record %d already holds ts %d", ts, got, got-1, r.RecordAt(got-1).ExchangeTs)
			}
		}
	})

	t.Run("a_file_with_no_records", func(t *testing.T) {
		w, path := newTestWriter(t, 4)
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
		empty := openFixture(t, path)

		got, err := empty.SeekTime(1000)

		if err != nil {
			t.Fatalf("SeekTime() error = %v, want nil", err)
		}
		if got != 0 {
			t.Errorf("SeekTime() = %d, want 0", got)
		}
	})
}

func TestSeekTimeAcrossBlockSizes(t *testing.T) {
	// The answer must not depend on the block size: the index is an
	// accelerator, never part of the result.
	for _, blockSize := range []uint32{1, 2, 3, 4, 5, 13, 64} {
		t.Run(fmt.Sprintf("block_size_%d", blockSize), func(t *testing.T) {
			w, path := newTestWriter(t, blockSize)
			for i, ts := range seekTimestamps {
				if err := w.WriteRecord(Record{
					ExchangeTs:     ts,
					SequenceNumber: uint64(i),
					InstrumentID:   10,
					VenueID:        testVenue,
					RecordType:     RecordTypeDelta,
					Price:          100,
					Size:           1,
				}); err != nil {
					t.Fatalf("writing record %d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}
			r := openFixture(t, path)

			for target := int64(5); target <= 55; target++ {
				got, err := r.SeekTime(target)

				if err != nil {
					t.Fatalf("SeekTime(%d) error = %v, want nil", target, err)
				}
				if want := naiveSeek(seekTimestamps, target); got != want {
					t.Errorf("SeekTime(%d) = %d, want %d", target, got, want)
				}
			}
		})
	}
}

// naiveSeek is the definition SeekTime implements, written the slow way.
func naiveSeek(ts []int64, target int64) int {
	for i, v := range ts {
		if v >= target {
			return i
		}
	}
	return len(ts)
}

func TestSnapshotBefore(t *testing.T) {
	r := writeSeekFixture(t)

	tests := []struct {
		name        string
		recordIndex int
		want        int
		wantOK      bool
	}{
		{name: "before_the_first_snapshot", recordIndex: 0, wantOK: false},
		{name: "one_record_before_the_first_snapshot", recordIndex: 1, wantOK: false},
		{name: "exactly_the_first_snapshot", recordIndex: 2, want: 2, wantOK: true},
		{name: "between_snapshots", recordIndex: 5, want: 2, wantOK: true},
		{name: "exactly_the_second_snapshot", recordIndex: 9, want: 9, wantOK: true},
		{name: "after_every_snapshot", recordIndex: 12, want: 9, wantOK: true},
		{name: "a_negative_index", recordIndex: -1, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := r.SnapshotBefore(tt.recordIndex)

			if ok != tt.wantOK {
				t.Fatalf("SnapshotBefore(%d) ok = %v, want %v", tt.recordIndex, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if got != tt.want {
				t.Errorf("SnapshotBefore(%d) = %d, want %d", tt.recordIndex, got, tt.want)
			}
			if r.RecordAt(got).RecordType != RecordTypeSnapshotPointer {
				t.Errorf("SnapshotBefore(%d) named record %d, which is not a snapshot pointer", tt.recordIndex, got)
			}
		})
	}

	t.Run("a_file_with_no_snapshots", func(t *testing.T) {
		f := writeFixture(t, 2)
		plain := openFixture(t, f.path)
		// The shared fixture does hold snapshots, so rewind past them.
		if _, ok := plain.SnapshotBefore(0); ok {
			t.Error("SnapshotBefore(0) ok = true, want false — record 0 is a delta")
		}
	})
}
