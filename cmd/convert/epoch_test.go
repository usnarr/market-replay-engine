package main

import (
	"slices"
	"testing"

	"replay/internal/book"
	"replay/internal/store"
)

// epochRows is one venue's session over two instruments, with one
// timestamp per record so an epoch can land between any two of them. The
// seventh row removes a level, so the book an epoch snapshots is not
// simply every level the session mentioned.
func epochRows(base int64) []SourceRow {
	return []SourceRow{
		srcDelta(base+1, 7, 1, 10, store.SideBid, 100, 5),
		srcDelta(base+2, 7, 2, 11, store.SideAsk, 200, 6),
		srcDelta(base+3, 7, 3, 10, store.SideBid, 99, 7),
		srcDelta(base+4, 7, 4, 11, store.SideAsk, 201, 8),
		srcDelta(base+5, 7, 5, 10, store.SideAsk, 105, 9),
		srcDelta(base+6, 7, 6, 11, store.SideBid, 195, 10),
		srcDelta(base+7, 7, 7, 10, store.SideBid, 100, 0),
		srcDelta(base+8, 7, 8, 11, store.SideAsk, 200, 11),
		srcDelta(base+9, 7, 9, 10, store.SideBid, 98, 12),
		srcDelta(base+10, 7, 10, 11, store.SideBid, 194, 13),
	}
}

// convertRows writes rows to a Parquet file, converts it, and opens the
// single artifact it produces.
func convertRows(t *testing.T, rows []SourceRow, epochEvery int) *store.Reader {
	t.Helper()
	out := t.TempDir()
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", rows)

	paths, err := Convert(src, out, Options{PriceScale: testPriceScale, EpochEvery: epochEvery})
	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	if len(paths) != 1 {
		t.Fatalf("Convert() wrote %d files, want 1", len(paths))
	}
	r := openArtifact(t, paths[0])
	if err := r.VerifyAll(); err != nil {
		t.Fatalf("VerifyAll() error = %v, want nil", err)
	}
	return r
}

// snapshotPositions returns the record indexes of every snapshot pointer
// in the file, found by scanning rather than by reading the index the
// writer built, so the two can be compared against each other.
func snapshotPositions(r *store.Reader) []int {
	var out []int
	for i := 0; i < r.Len(); i++ {
		if r.RecordAt(i).RecordType == store.RecordTypeSnapshotPointer {
			out = append(out, i)
		}
	}
	return out
}

func TestConvertInsertsSnapshotEpochs(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	r := convertRows(t, epochRows(base), 4)

	if r.Len() != 14 {
		t.Fatalf("Len() = %d, want 14: ten source records and two epochs of two instruments", r.Len())
	}

	t.Run("an_epoch_lands_at_each_cadence_points_timestamp_boundary", func(t *testing.T) {
		got := snapshotPositions(r)

		want := []int{4, 5, 10, 11}
		if !slices.Equal(got, want) {
			t.Errorf("snapshot pointer positions = %v, want %v", got, want)
		}
	})

	t.Run("the_snapshot_index_agrees_with_the_records", func(t *testing.T) {
		tests := []struct {
			name      string
			at        int
			want      int
			wantFound bool
		}{
			{name: "before_the_first_epoch", at: 3},
			{name: "at_the_first_epochs_first_record", at: 4, want: 4, wantFound: true},
			{name: "at_the_first_epochs_last_record", at: 5, want: 5, wantFound: true},
			{name: "between_the_two_epochs", at: 9, want: 5, wantFound: true},
			{name: "after_the_last_epoch", at: 13, want: 11, wantFound: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, found := r.SnapshotBefore(tt.at)

				if found != tt.wantFound || (found && got != tt.want) {
					t.Errorf("SnapshotBefore(%d) = %d, %v, want %d, %v", tt.at, got, found, tt.want, tt.wantFound)
				}
			})
		}
	})

	t.Run("an_epoch_carries_the_previous_records_timestamp_and_runs_on_from_its_sequence", func(t *testing.T) {
		tests := []struct {
			at             int
			wantExchangeTs int64
			wantSeq        uint64
			wantInstrument uint32
		}{
			{at: 4, wantExchangeTs: base + 4, wantSeq: 5, wantInstrument: 10},
			{at: 5, wantExchangeTs: base + 4, wantSeq: 6, wantInstrument: 11},
			{at: 10, wantExchangeTs: base + 8, wantSeq: 9, wantInstrument: 10},
			{at: 11, wantExchangeTs: base + 8, wantSeq: 10, wantInstrument: 11},
		}

		for _, tt := range tests {
			rec := r.RecordAt(tt.at)

			if rec.ExchangeTs != tt.wantExchangeTs || rec.SequenceNumber != tt.wantSeq || rec.InstrumentID != tt.wantInstrument {
				t.Errorf("RecordAt(%d) = (exchange_ts=%d, sequence_number=%d, instrument_id=%d), want (%d, %d, %d)",
					tt.at, rec.ExchangeTs, rec.SequenceNumber, rec.InstrumentID, tt.wantExchangeTs, tt.wantSeq, tt.wantInstrument)
			}
		}
	})

	t.Run("an_epochs_levels_are_the_book_at_that_point", func(t *testing.T) {
		tests := []struct {
			at        int
			wantLevel []store.Level
			wantBids  int
		}{
			// Instrument 10 has two bids and no ask at the first epoch.
			{at: 4, wantLevel: []store.Level{{Price: 100, Size: 5}, {Price: 99, Size: 7}}, wantBids: 2},
			// Instrument 11 has two asks and no bid.
			{at: 5, wantLevel: []store.Level{{Price: 200, Size: 6}, {Price: 201, Size: 8}}, wantBids: 0},
			// By the second epoch instrument 10 has lost the 100 bid and
			// gained a 105 ask.
			{at: 10, wantLevel: []store.Level{{Price: 99, Size: 7}, {Price: 105, Size: 9}}, wantBids: 1},
		}

		for _, tt := range tests {
			levels, bids, err := r.AppendLevels(nil, r.RecordAt(tt.at))

			if err != nil {
				t.Fatalf("AppendLevels(%d) error = %v, want nil", tt.at, err)
			}
			if bids != tt.wantBids || !slices.Equal(levels, tt.wantLevel) {
				t.Errorf("AppendLevels(%d) = %v (%d bids), want %v (%d bids)", tt.at, levels, bids, tt.wantLevel, tt.wantBids)
			}
		}
	})
}

func TestEpochWaitsForTheNextTimestampBoundary(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	rows := []SourceRow{
		srcDelta(base+1, 7, 1, 10, store.SideBid, 100, 5),
		srcDelta(base+2, 7, 2, 10, store.SideBid, 99, 6),
		srcDelta(base+2, 7, 3, 10, store.SideBid, 98, 7),
		srcDelta(base+2, 7, 4, 10, store.SideBid, 97, 8),
		srcDelta(base+3, 7, 5, 10, store.SideBid, 96, 9),
	}
	r := convertRows(t, rows, 2)

	got := snapshotPositions(r)

	// The cadence comes due at row 1, but rows 2 and 3 repeat its
	// timestamp. The epoch waits for the boundary at row 4 rather than
	// splitting a run of equal timestamps.
	if want := []int{4}; !slices.Equal(got, want) {
		t.Fatalf("snapshot pointer positions = %v, want %v", got, want)
	}
	rec := r.RecordAt(4)
	if rec.ExchangeTs != base+2 || rec.SequenceNumber != 5 {
		t.Errorf("RecordAt(4) = (exchange_ts=%d, sequence_number=%d), want (%d, 5)", rec.ExchangeTs, rec.SequenceNumber, base+2)
	}
}

// replayFromScratch rebuilds instrumentID's book by walking every record
// below upTo, which is the state WarmUp must reach from an epoch alone.
func replayFromScratch(t *testing.T, r *store.Reader, instrumentID uint32, upTo int) *book.Book {
	t.Helper()
	b := book.NewBook(instrumentID)
	for i := 0; i < upTo; i++ {
		rec := r.RecordAt(i)
		if rec.InstrumentID != instrumentID {
			continue
		}
		switch rec.RecordType {
		case store.RecordTypeSnapshotPointer:
			levels, bidCount, err := r.AppendLevels(nil, rec)
			if err != nil {
				t.Fatalf("AppendLevels(%d) error = %v, want nil", i, err)
			}
			if err := b.ApplySnapshot(levels, bidCount); err != nil {
				t.Fatalf("ApplySnapshot(%d) error = %v, want nil", i, err)
			}
		case store.RecordTypeDelta:
			if err := b.Apply(rec); err != nil {
				t.Fatalf("Apply(%d) error = %v, want nil", i, err)
			}
		}
	}
	return b
}

func TestWarmUpFromAnInsertedEpochMatchesAFullReplay(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	r := convertRows(t, epochRows(base), 4)

	tests := []struct {
		name         string
		instrumentID uint32
		targetTs     int64
	}{
		{name: "before_the_first_epoch", instrumentID: 10, targetTs: base + 3},
		{name: "inside_the_first_epoch_run", instrumentID: 10, targetTs: base + 4},
		{name: "just_after_the_first_epoch", instrumentID: 10, targetTs: base + 7},
		{name: "just_after_the_second_epoch", instrumentID: 10, targetTs: base + 9},
		{name: "the_other_instrument_after_the_second_epoch", instrumentID: 11, targetTs: base + 10},
		{name: "past_the_end_of_the_file", instrumentID: 11, targetTs: base + 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, resume, err := book.WarmUp(r, tt.instrumentID, tt.targetTs)
			if err != nil {
				t.Fatalf("WarmUp(%d, %d) error = %v, want nil", tt.instrumentID, tt.targetTs, err)
			}
			want := replayFromScratch(t, r, tt.instrumentID, resume)

			if len(want.Bids()) == 0 && len(want.Asks()) == 0 {
				t.Fatal("the expected book is empty, so this case proves nothing")
			}
			if !slices.Equal(got.Bids(), want.Bids()) || !slices.Equal(got.Asks(), want.Asks()) {
				t.Errorf("WarmUp(%d, %d) = bids %v asks %v, want bids %v asks %v",
					tt.instrumentID, tt.targetTs, got.Bids(), got.Asks(), want.Bids(), want.Asks())
			}
		})
	}
}
