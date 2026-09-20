package merge

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

const testInstrument = 7

// deltaRecords builds n delta records for one venue, with strictly
// increasing keys starting at firstTs and firstSeq.
func deltaRecords(venue uint16, firstTs int64, firstSeq uint64, n int) []store.Record {
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{
			ExchangeTs:     firstTs + int64(i),
			SequenceNumber: firstSeq + uint64(i),
			InstrumentID:   testInstrument,
			VenueID:        venue,
			RecordType:     store.RecordTypeDelta,
			Price:          100 + int64(i),
			Size:           1,
		}
	}
	return recs
}

// writeVenueFile writes one hot-tier file holding recs and returns its
// path. Each call gets its own temporary directory, so name only has to
// be readable in a failure message.
func writeVenueFile(t *testing.T, name string, venue uint16, recs []store.Record) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	w, err := store.NewWriter(path, venue, 100)
	if err != nil {
		t.Fatalf("NewWriter(%s) error = %v, want nil", name, err)
	}
	for i, rec := range recs {
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("WriteRecord(%s, %d) error = %v, want nil", name, i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(%s) error = %v, want nil", name, err)
	}
	return path
}

func openVenueFile(t *testing.T, path string) *store.Reader {
	t.Helper()

	r, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) error = %v, want nil", path, err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func newCursorOver(t *testing.T, venue uint16, paths ...string) *Cursor {
	t.Helper()

	readers := make([]*store.Reader, len(paths))
	for i, path := range paths {
		readers[i] = openVenueFile(t, path)
	}
	c, err := NewCursor(venue, readers)
	if err != nil {
		t.Fatalf("NewCursor() error = %v, want nil", err)
	}
	return c
}

// drainCursor reads a cursor to exhaustion, or to its first error.
func drainCursor(t *testing.T, c *Cursor) ([]store.Record, error) {
	t.Helper()

	var got []store.Record
	for {
		ev, ok, err := c.Next()
		if err != nil {
			return got, err
		}
		if !ok {
			return got, nil
		}
		got = append(got, ev.Record)
		if len(got) > 1<<20 {
			t.Fatal("cursor is not draining")
		}
	}
}

func TestCursor(t *testing.T) {
	t.Run("a_single_file_yields_every_record_in_order", func(t *testing.T) {
		want := deltaRecords(3, 100, 0, 20)
		c := newCursorOver(t, 3, writeVenueFile(t, "day1.bin", 3, want))

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("records_continue_across_a_file_boundary", func(t *testing.T) {
		day1 := deltaRecords(3, 100, 0, 8)
		day2 := deltaRecords(3, 200, 8, 8)
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, day1),
			writeVenueFile(t, "day2.bin", 3, day2))

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if diff := cmp.Diff(append(day1, day2...), got); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a_repeated_timestamp_across_a_file_boundary_is_accepted", func(t *testing.T) {
		// One venue repeats a timestamp, and a capture window can split a
		// run of them. Only the full key has to increase.
		day1 := []store.Record{deltaRecords(3, 100, 0, 1)[0]}
		day2 := []store.Record{deltaRecords(3, 100, 1, 1)[0]}
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, day1),
			writeVenueFile(t, "day2.bin", 3, day2))

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if len(got) != 2 {
			t.Errorf("read %d records, want 2", len(got))
		}
	})

	t.Run("an_empty_partition_yields_nothing", func(t *testing.T) {
		c := newCursorOver(t, 3)

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if len(got) != 0 {
			t.Errorf("read %d records, want 0", len(got))
		}
	})

	t.Run("an_empty_file_between_two_full_ones_is_skipped", func(t *testing.T) {
		day1 := deltaRecords(3, 100, 0, 4)
		day3 := deltaRecords(3, 300, 4, 4)
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, day1),
			writeVenueFile(t, "day2.bin", 3, nil),
			writeVenueFile(t, "day3.bin", 3, day3))

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if diff := cmp.Diff(append(day1, day3...), got); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a_partition_of_only_empty_files_yields_nothing", func(t *testing.T) {
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, nil),
			writeVenueFile(t, "day2.bin", 3, nil))

		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if len(got) != 0 {
			t.Errorf("read %d records, want 0", len(got))
		}
	})

	t.Run("records_span_many_checksummed_blocks", func(t *testing.T) {
		// The default block size is 1024 records, so this crosses two
		// block boundaries and lands the last block partly filled.
		want := deltaRecords(3, 0, 0, 2500)
		path := writeVenueFile(t, "big.bin", 3, want)
		r := openVenueFile(t, path)
		if r.BlockCount() < 3 {
			t.Fatalf("fixture has %d blocks, want at least 3", r.BlockCount())
		}
		c, err := NewCursor(3, []*store.Reader{r})
		if err != nil {
			t.Fatalf("NewCursor() error = %v, want nil", err)
		}

		got, drainErr := drainCursor(t, c)

		if drainErr != nil {
			t.Fatalf("Next() error = %v, want nil", drainErr)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("venue_id_reports_the_partitions_venue", func(t *testing.T) {
		c := newCursorOver(t, 42, writeVenueFile(t, "day1.bin", 42, deltaRecords(42, 100, 0, 2)))

		if got := c.VenueID(); got != 42 {
			t.Errorf("VenueID() = %d, want 42", got)
		}
	})
}

func TestCursorSeekTime(t *testing.T) {
	// Three files so a seek target can land before the first file, on a
	// boundary between two files, and inside the last one, exercising
	// the whole-file skip as well as the sparse-index lookup inside a
	// file.
	day1 := deltaRecords(3, 100, 0, 4) // ts 100..103
	day2 := deltaRecords(3, 200, 4, 4) // ts 200..203
	day3 := deltaRecords(3, 300, 8, 4) // ts 300..303
	all := append(append(append([]store.Record{}, day1...), day2...), day3...)

	newSeekCursor := func(t *testing.T) *Cursor {
		return newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, day1),
			writeVenueFile(t, "day2.bin", 3, day2),
			writeVenueFile(t, "day3.bin", 3, day3))
	}

	tests := []struct {
		name string
		t    int64
		want []store.Record
	}{
		{name: "before_every_record", t: 0, want: all},
		{name: "exactly_the_first_timestamp", t: 100, want: all},
		{name: "between_two_records_in_the_first_file", t: 101, want: all[1:]},
		{name: "exactly_a_later_files_first_timestamp", t: 200, want: all[4:]},
		{name: "between_two_files", t: 150, want: all[4:]},
		{name: "inside_the_last_file", t: 301, want: all[9:]},
		{name: "exactly_the_last_timestamp", t: 303, want: all[11:]},
		{name: "past_every_record", t: 304, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newSeekCursor(t)

			if err := c.SeekTime(tt.t); err != nil {
				t.Fatalf("SeekTime(%d) error = %v, want nil", tt.t, err)
			}
			got, err := drainCursor(t, c)

			if err != nil {
				t.Fatalf("Next() error = %v, want nil", err)
			}
			if diff := cmp.Diff(tt.want, got, cmpEmptyRecordSlices); diff != "" {
				t.Errorf("SeekTime(%d) suffix mismatch (-want +got):\n%s", tt.t, diff)
			}
		})
	}

	t.Run("seeking_an_empty_partition_yields_nothing", func(t *testing.T) {
		c := newCursorOver(t, 3)

		if err := c.SeekTime(1000); err != nil {
			t.Fatalf("SeekTime() error = %v, want nil", err)
		}
		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if len(got) != 0 {
			t.Errorf("read %d records, want 0", len(got))
		}
	})

	t.Run("seeking_past_a_whole_empty_file", func(t *testing.T) {
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, day1),
			writeVenueFile(t, "day2.bin", 3, nil),
			writeVenueFile(t, "day3.bin", 3, day3))

		if err := c.SeekTime(250); err != nil {
			t.Fatalf("SeekTime() error = %v, want nil", err)
		}
		got, err := drainCursor(t, c)

		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if diff := cmp.Diff(day3, got); diff != "" {
			t.Errorf("suffix mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("calling_it_after_next_panics", func(t *testing.T) {
		c := newSeekCursor(t)
		if _, _, err := c.Next(); err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}

		defer func() {
			if recover() == nil {
				t.Error("SeekTime() after Next did not panic")
			}
		}()
		_ = c.SeekTime(200)
	})
}

// cmpEmptyRecordSlices treats a nil and an empty record slice as equal.
var cmpEmptyRecordSlices = cmp.FilterValues(
	func(a, b []store.Record) bool { return len(a) == 0 && len(b) == 0 },
	cmp.Comparer(func(a, b []store.Record) bool { return true }),
)

func TestCursorRejectsBrokenOrderingAcrossAFileBoundary(t *testing.T) {
	// Each file is in order on its own, so the writer accepts both. The
	// boundary is the one place a per-file check cannot see the problem,
	// which is exactly where overlapping capture windows put it.
	last := deltaRecords(3, 100, 5, 1)[0]

	tests := []struct {
		name  string
		first store.Record
		want  error
	}{
		{
			name:  "the_same_key_repeats",
			first: last,
			want:  ErrDuplicateKey,
		},
		{
			name:  "the_timestamp_goes_backwards",
			first: store.Record{ExchangeTs: 90, SequenceNumber: 99, InstrumentID: testInstrument, VenueID: 3, RecordType: store.RecordTypeDelta, Price: 1, Size: 1},
			want:  ErrOutOfOrder,
		},
		{
			name:  "the_sequence_number_goes_backwards_at_the_same_timestamp",
			first: store.Record{ExchangeTs: 100, SequenceNumber: 3, InstrumentID: testInstrument, VenueID: 3, RecordType: store.RecordTypeDelta, Price: 1, Size: 1},
			want:  ErrOutOfOrder,
		},
		{
			name:  "the_instrument_id_goes_backwards_at_the_same_sequence_number",
			first: store.Record{ExchangeTs: 100, SequenceNumber: 5, InstrumentID: testInstrument - 1, VenueID: 3, RecordType: store.RecordTypeDelta, Price: 1, Size: 1},
			want:  ErrOutOfOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			day2 := append([]store.Record{tt.first}, deltaRecords(3, 500, 500, 2)...)
			c := newCursorOver(t, 3,
				writeVenueFile(t, "day1.bin", 3, []store.Record{last}),
				writeVenueFile(t, "day2.bin", 3, day2))

			got, err := drainCursor(t, c)

			if !errors.Is(err, tt.want) {
				t.Fatalf("Next() error = %v, want %v", err, tt.want)
			}
			if len(got) != 1 {
				t.Errorf("read %d records before the error, want 1", len(got))
			}
		})
	}
}

func TestCursorRejectsFilesFromDifferentVenues(t *testing.T) {
	readers := []*store.Reader{
		openVenueFile(t, writeVenueFile(t, "venue3.bin", 3, deltaRecords(3, 100, 0, 2))),
		openVenueFile(t, writeVenueFile(t, "venue4.bin", 4, deltaRecords(4, 200, 0, 2))),
	}

	_, err := NewCursor(3, readers)

	if !errors.Is(err, ErrVenueMismatch) {
		t.Fatalf("NewCursor() error = %v, want %v", err, ErrVenueMismatch)
	}
}

func TestCursorVerifiesABlockBeforeYieldingItsRecords(t *testing.T) {
	// A cursor is the only thing that walks a file in record order, so it
	// is where a block gets checksummed. Corrupt one record's payload and
	// the whole block it sits in must be refused, not just that record.
	const markerPrice = 0x5EED_FACE_1234_5678

	// corruptVenueFile writes n records for the venue, damages the sixth,
	// and returns the file's path.
	corruptVenueFile := func(t *testing.T, name string, firstTs int64, firstSeq uint64, n int) string {
		t.Helper()

		recs := deltaRecords(3, firstTs, firstSeq, n)
		recs[5].Price = markerPrice
		path := writeVenueFile(t, name, 3, recs)

		var marker [8]byte
		binary.LittleEndian.PutUint64(marker[:], markerPrice)
		corruptFileAt(t, path, marker[:])
		return path
	}

	t.Run("in_the_first_file", func(t *testing.T) {
		c := newCursorOver(t, 3, corruptVenueFile(t, "corrupt.bin", 0, 0, 50))

		got, err := drainCursor(t, c)

		if !errors.Is(err, store.ErrBlockChecksum) {
			t.Fatalf("Next() error = %v, want %v", err, store.ErrBlockChecksum)
		}
		if len(got) != 0 {
			t.Errorf("read %d records from a corrupt block, want 0", len(got))
		}
	})

	t.Run("in_a_later_file", func(t *testing.T) {
		// Moving to the next file resets which block has been verified.
		// Carrying the previous file's block number over would leave this
		// file's first block unchecked.
		clean := deltaRecords(3, 0, 0, 50)
		c := newCursorOver(t, 3,
			writeVenueFile(t, "day1.bin", 3, clean),
			corruptVenueFile(t, "day2.bin", 1000, 1000, 50))

		got, err := drainCursor(t, c)

		if !errors.Is(err, store.ErrBlockChecksum) {
			t.Fatalf("Next() error = %v, want %v", err, store.ErrBlockChecksum)
		}
		if len(got) != len(clean) {
			t.Errorf("read %d records before the error, want %d", len(got), len(clean))
		}
	})
}

// corruptFileAt flips every bit of the first occurrence of needle in the
// file at path. It finds the bytes by searching rather than by
// arithmetic, so the test stays independent of header_size — which is
// the writing host's page size, not a constant.
func corruptFileAt(t *testing.T, path string, needle []byte) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v, want nil", err)
	}
	i := bytes.Index(data, needle)
	if i < 0 {
		t.Fatal("marker bytes are not in the file; the fixture no longer corrupts a record")
	}
	if bytes.Contains(data[i+1:], needle) {
		t.Fatal("marker bytes appear more than once; the fixture may corrupt the wrong region")
	}
	for j := range needle {
		data[i+j] ^= 0xff
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v, want nil", err)
	}
}

func TestCursorCloseReleasesEveryFile(t *testing.T) {
	paths := []string{
		writeVenueFile(t, "day1.bin", 3, deltaRecords(3, 100, 0, 4)),
		writeVenueFile(t, "day2.bin", 3, deltaRecords(3, 200, 4, 4)),
	}
	readers := make([]*store.Reader, len(paths))
	for i, path := range paths {
		r, err := store.Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v, want nil", err)
		}
		readers[i] = r
	}
	c, err := NewCursor(3, readers)
	if err != nil {
		t.Fatalf("NewCursor() error = %v, want nil", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	// Truncation is the check that works here. Windows refuses it while a
	// view of the file is still mapped, and deleting the file would
	// succeed either way. See internal/store's own mmap tests.
	for i, path := range paths {
		if err := os.Truncate(path, 0); err != nil {
			t.Errorf("file %d (%s) is still mapped after Close: %v", i, path, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

func TestCursorNextAfterExhaustionStaysExhausted(t *testing.T) {
	c := newCursorOver(t, 3, writeVenueFile(t, "day1.bin", 3, deltaRecords(3, 100, 0, 2)))
	if _, err := drainCursor(t, c); err != nil {
		t.Fatalf("Next() error = %v, want nil", err)
	}

	for i := 0; i < 3; i++ {
		rec, ok, err := c.Next()

		if err != nil || ok {
			t.Fatalf("Next() call %s after exhaustion = (%+v, %v, %v), want (zero, false, nil)",
				strconv.Itoa(i+1), rec, ok, err)
		}
	}
}
