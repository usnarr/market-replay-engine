package main

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"replay/internal/store"
)

const testPriceScale = 100

func srcDelta(ts int64, venue uint32, seq uint64, instr uint32, side uint8, price, size int64) SourceRow {
	return SourceRow{
		ExchangeTs:     ts,
		SequenceNumber: seq,
		InstrumentID:   instr,
		VenueID:        venue,
		RecordType:     uint32(store.RecordTypeDelta),
		SideFlags:      uint32(side),
		Price:          price,
		Size:           size,
	}
}

func srcSnapshot(ts int64, venue uint32, seq uint64, instr uint32, levels []SourceLevel) SourceRow {
	return SourceRow{
		ExchangeTs:     ts,
		SequenceNumber: seq,
		InstrumentID:   instr,
		VenueID:        venue,
		RecordType:     uint32(store.RecordTypeSnapshotPointer),
		Levels:         levels,
	}
}

// openArtifact opens a converted file and closes it when the test ends.
// Closing matters on Windows, where the temp directory cannot be removed
// while the file is still mapped.
func openArtifact(t *testing.T, path string) *store.Reader {
	t.Helper()
	r, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open(%s) error = %v, want nil", filepath.Base(path), err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// dirEntries returns the names in dir, sorted.
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v, want nil", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

func TestConvertPartitionsByVenueAndDay(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	rows := []SourceRow{
		srcDelta(base+1, 7, 1, 10, store.SideBid, 100, 5),
		srcDelta(base+2, 9, 1, 20, store.SideAsk, 200, 6),
		srcDelta(base+3, 7, 2, 11, store.SideBid, 101, 7),
		srcDelta(base+4, 9, 2, 20, store.SideBid, 199, 8),
		srcDelta(base+nanosPerDay+1, 7, 3, 10, store.SideAsk, 102, 9),
		srcDelta(base+nanosPerDay+2, 7, 4, 10, store.SideBid, 103, 10),
	}
	out := t.TempDir()
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", rows)

	got, err := Convert(src, out, testPriceScale)

	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	want := []string{
		filepath.Join(out, "venue-7-2024-01-01.bin"),
		filepath.Join(out, "venue-7-2024-01-02.bin"),
		filepath.Join(out, "venue-9-2024-01-01.bin"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Convert() = %v, want %v", got, want)
	}

	t.Run("the_output_directory_holds_nothing_else", func(t *testing.T) {
		names := dirEntries(t, out)

		wantNames := []string{"venue-7-2024-01-01.bin", "venue-7-2024-01-02.bin", "venue-9-2024-01-01.bin"}
		if !slices.Equal(names, wantNames) {
			t.Errorf("the output directory holds %v, want %v", names, wantNames)
		}
	})

	t.Run("each_file_holds_its_own_venue_and_day", func(t *testing.T) {
		tests := []struct {
			path        string
			wantVenue   uint16
			wantRecords []store.Record
		}{
			{
				path:      want[0],
				wantVenue: 7,
				wantRecords: []store.Record{
					{ExchangeTs: base + 1, SequenceNumber: 1, InstrumentID: 10, VenueID: 7, SideFlags: store.SideBid, Price: 100, Size: 5},
					{ExchangeTs: base + 3, SequenceNumber: 2, InstrumentID: 11, VenueID: 7, SideFlags: store.SideBid, Price: 101, Size: 7},
				},
			},
			{
				path:      want[1],
				wantVenue: 7,
				wantRecords: []store.Record{
					{ExchangeTs: base + nanosPerDay + 1, SequenceNumber: 3, InstrumentID: 10, VenueID: 7, SideFlags: store.SideAsk, Price: 102, Size: 9},
					{ExchangeTs: base + nanosPerDay + 2, SequenceNumber: 4, InstrumentID: 10, VenueID: 7, SideFlags: store.SideBid, Price: 103, Size: 10},
				},
			},
			{
				path:      want[2],
				wantVenue: 9,
				wantRecords: []store.Record{
					{ExchangeTs: base + 2, SequenceNumber: 1, InstrumentID: 20, VenueID: 9, SideFlags: store.SideAsk, Price: 200, Size: 6},
					{ExchangeTs: base + 4, SequenceNumber: 2, InstrumentID: 20, VenueID: 9, SideFlags: store.SideBid, Price: 199, Size: 8},
				},
			},
		}

		for _, tt := range tests {
			t.Run(filepath.Base(tt.path), func(t *testing.T) {
				r := openArtifact(t, tt.path)
				if err := r.VerifyAll(); err != nil {
					t.Fatalf("VerifyAll() error = %v, want nil", err)
				}

				if r.VenueID() != tt.wantVenue {
					t.Errorf("VenueID() = %d, want %d", r.VenueID(), tt.wantVenue)
				}
				if r.PriceScale() != testPriceScale {
					t.Errorf("PriceScale() = %d, want %d", r.PriceScale(), testPriceScale)
				}
				if r.Len() != len(tt.wantRecords) {
					t.Fatalf("Len() = %d, want %d", r.Len(), len(tt.wantRecords))
				}
				for i, wantRec := range tt.wantRecords {
					if got := r.RecordAt(i); got != wantRec {
						t.Errorf("RecordAt(%d) = %+v, want %+v", i, got, wantRec)
					}
				}
			})
		}
	})
}

func TestConvertWritesSnapshotLevels(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	rows := []SourceRow{
		// Deliberately out of best-first order, and with the two sides
		// interleaved: the converter normalizes them.
		srcSnapshot(base+1, 7, 1, 10, []SourceLevel{
			{Side: uint32(store.SideAsk), Price: 105, Size: 2},
			{Side: uint32(store.SideBid), Price: 99, Size: 3},
			{Side: uint32(store.SideAsk), Price: 101, Size: 4},
			{Side: uint32(store.SideBid), Price: 100, Size: 5},
		}),
	}
	out := t.TempDir()
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", rows)

	paths, err := Convert(src, out, testPriceScale)

	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	r := openArtifact(t, paths[0])
	if err := r.VerifyAll(); err != nil {
		t.Fatalf("VerifyAll() error = %v, want nil", err)
	}
	rec := r.RecordAt(0)
	if rec.RecordType != store.RecordTypeSnapshotPointer {
		t.Fatalf("RecordAt(0).RecordType = %d, want %d", rec.RecordType, store.RecordTypeSnapshotPointer)
	}
	levels, bidCount, err := r.AppendLevels(nil, rec)
	if err != nil {
		t.Fatalf("AppendLevels() error = %v, want nil", err)
	}
	wantLevels := []store.Level{{Price: 100, Size: 5}, {Price: 99, Size: 3}, {Price: 101, Size: 4}, {Price: 105, Size: 2}}
	if bidCount != 2 || !slices.Equal(levels, wantLevels) {
		t.Errorf("AppendLevels() = %v (%d bids), want %v (2 bids)", levels, bidCount, wantLevels)
	}
}

func TestConvertRejectsAnUnconvertibleRow(t *testing.T) {
	base := int64(day2024) * nanosPerDay

	tests := []struct {
		name string
		row  SourceRow
		want error
	}{
		{
			name: "a_venue_id_wider_than_uint16",
			row:  srcDelta(base+1, 1<<16, 1, 10, store.SideBid, 100, 5),
			want: ErrVenueIDRange,
		},
		{
			name: "an_undefined_record_type",
			row:  SourceRow{ExchangeTs: base + 1, SequenceNumber: 1, InstrumentID: 10, VenueID: 7, RecordType: 3},
			want: ErrRecordTypeRange,
		},
		{
			name: "a_side_flags_reserved_bit",
			row:  srcDelta(base+1, 7, 1, 10, 2, 100, 5),
			want: ErrSideFlagsRange,
		},
		{
			name: "levels_on_a_delta_row",
			row: func() SourceRow {
				r := srcDelta(base+1, 7, 1, 10, store.SideBid, 100, 5)
				r.Levels = []SourceLevel{{Side: uint32(store.SideBid), Price: 100, Size: 5}}
				return r
			}(),
			want: ErrLevelsOnNonSnap,
		},
		{
			name: "a_price_on_a_snapshot_row",
			row: func() SourceRow {
				r := srcSnapshot(base+1, 7, 1, 10, []SourceLevel{{Side: uint32(store.SideBid), Price: 100, Size: 5}})
				r.Price = 100
				return r
			}(),
			want: ErrSnapshotFieldSet,
		},
		{
			name: "a_level_side_that_is_neither_bid_nor_ask",
			row:  srcSnapshot(base+1, 7, 1, 10, []SourceLevel{{Side: 2, Price: 100, Size: 5}}),
			want: ErrLevelSide,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := t.TempDir()
			src := writeSourceParquet(t, t.TempDir(), "source.parquet", []SourceRow{tt.row})

			_, err := Convert(src, out, testPriceScale)

			if !errors.Is(err, tt.want) {
				t.Fatalf("Convert() error = %v, want %v", err, tt.want)
			}
			var re *RowError
			if !errors.As(err, &re) {
				t.Fatalf("Convert() error = %v, want a *RowError", err)
			}
			if re.Index != 0 || re.Row.ExchangeTs != tt.row.ExchangeTs {
				t.Errorf("Convert() error names row %d (exchange_ts=%d), want row 0 (exchange_ts=%d)", re.Index, re.Row.ExchangeTs, tt.row.ExchangeTs)
			}
			if names := dirEntries(t, out); len(names) != 0 {
				t.Errorf("a rejected conversion left %v behind, want an empty directory", names)
			}
		})
	}
}

func TestConvertRejectsDecreasingExchangeTs(t *testing.T) {
	base := int64(day2024) * nanosPerDay

	tests := []struct {
		name       string
		rows       []SourceRow
		wantReject bool
		wantIndex  int64
	}{
		{
			name: "a_timestamp_that_stands_still",
			rows: []SourceRow{
				srcDelta(base+10, 7, 1, 10, store.SideBid, 100, 5),
				srcDelta(base+10, 7, 2, 10, store.SideBid, 101, 6),
			},
		},
		{
			name: "two_venues_whose_timestamps_interleave",
			rows: []SourceRow{
				srcDelta(base+10, 7, 1, 10, store.SideBid, 100, 5),
				srcDelta(base+1, 9, 1, 20, store.SideBid, 200, 5),
				srcDelta(base+11, 7, 2, 10, store.SideBid, 101, 6),
				srcDelta(base+2, 9, 2, 20, store.SideBid, 201, 6),
			},
		},
		{
			name: "a_timestamp_that_goes_backwards_within_one_day",
			rows: []SourceRow{
				srcDelta(base+10, 7, 1, 10, store.SideBid, 100, 5),
				srcDelta(base+9, 7, 2, 10, store.SideBid, 101, 6),
			},
			wantReject: true,
			wantIndex:  1,
		},
		{
			name: "a_timestamp_that_goes_back_into_the_previous_day",
			rows: []SourceRow{
				srcDelta(base+1, 7, 1, 10, store.SideBid, 100, 5),
				srcDelta(base+nanosPerDay+1, 7, 2, 10, store.SideBid, 101, 6),
				srcDelta(base+2, 7, 3, 10, store.SideBid, 102, 7),
			},
			wantReject: true,
			wantIndex:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := t.TempDir()
			src := writeSourceParquet(t, t.TempDir(), "source.parquet", tt.rows)

			_, err := Convert(src, out, testPriceScale)

			if !tt.wantReject {
				if err != nil {
					t.Fatalf("Convert() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrExchangeTsDecreased) {
				t.Fatalf("Convert() error = %v, want %v", err, ErrExchangeTsDecreased)
			}
			var re *RowError
			if !errors.As(err, &re) {
				t.Fatalf("Convert() error = %v, want a *RowError", err)
			}
			if re.Index != tt.wantIndex || re.Row.ExchangeTs != tt.rows[tt.wantIndex].ExchangeTs {
				t.Errorf("Convert() error names row %d (exchange_ts=%d), want row %d (exchange_ts=%d)",
					re.Index, re.Row.ExchangeTs, tt.wantIndex, tt.rows[tt.wantIndex].ExchangeTs)
			}
			if names := dirEntries(t, out); len(names) != 0 {
				t.Errorf("a rejected conversion left %v behind, want an empty directory", names)
			}
		})
	}
}
