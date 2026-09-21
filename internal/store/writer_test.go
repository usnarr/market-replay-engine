package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const (
	testVenue      uint16 = 7
	testPriceScale int64  = 100_000_000
)

func delta(ts int64, seq uint64, instr uint32) Record {
	return Record{
		ExchangeTs:     ts,
		SequenceNumber: seq,
		InstrumentID:   instr,
		VenueID:        testVenue,
		RecordType:     RecordTypeDelta,
		SideFlags:      SideBid,
		Price:          100,
		Size:           1,
	}
}

// newTestWriter returns a writer over its own temp directory. The Abort
// cleanup is registered straight away: on Windows the temp directory
// cannot be removed while a handle is open, and t.TempDir's own removal
// cleanup was registered first, so it runs last.
func newTestWriter(t *testing.T, blockSize uint32) (*Writer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "venue.rpl")
	opts := defaultWriterOptions()
	opts.blockSizeRecords = blockSize
	w, err := newWriter(path, testVenue, testPriceScale, opts)
	if err != nil {
		t.Fatalf("newWriter() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = w.Abort() })
	return w, path
}

// readFinalized reads a closed file and returns its bytes and validated
// header. It goes through the byte-level decoders rather than the
// Reader, so it tests the file the writer produced rather than a round
// trip through the writer's own view of it.
func readFinalized(t *testing.T, path string) ([]byte, header) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", filepath.Base(path), err)
	}
	h, err := decodeHeader(data)
	if err != nil {
		t.Fatalf("decodeHeader() error = %v, want nil", err)
	}
	if err := validateHeader(h, uint64(len(data))); err != nil {
		t.Fatalf("validateHeader() error = %v, want nil", err)
	}
	return data, h
}

func recordAtIndex(t *testing.T, data []byte, h header, i uint64) Record {
	t.Helper()
	rec, err := decodeRecord(data[uint64(h.HeaderSize)+i*RecordSize:])
	if err != nil {
		t.Fatalf("decodeRecord(%d) error = %v, want nil", i, err)
	}
	return rec
}

func TestWriterProducesAValidFile(t *testing.T) {
	w, path := newTestWriter(t, 2)
	want := []Record{delta(1000, 1, 10), delta(1000, 2, 10), delta(1001, 3, 11), delta(1002, 4, 11), delta(1003, 5, 12)}
	for _, rec := range want {
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("WriteRecord() error = %v, want nil", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	data, h := readFinalized(t, path)

	t.Run("header_describes_the_records_written", func(t *testing.T) {
		if h.RecordCount != uint64(len(want)) {
			t.Errorf("RecordCount = %d, want %d", h.RecordCount, len(want))
		}
		if h.VenueID != testVenue {
			t.Errorf("VenueID = %d, want %d", h.VenueID, testVenue)
		}
		if h.PriceScale != testPriceScale {
			t.Errorf("PriceScale = %d, want %d", h.PriceScale, testPriceScale)
		}
		if h.MinExchangeTs != 1000 || h.MaxExchangeTs != 1003 {
			t.Errorf("exchange_ts range = [%d, %d], want [1000, 1003]", h.MinExchangeTs, h.MaxExchangeTs)
		}
		if !h.Finalized {
			t.Error("Finalized = false, want true")
		}
	})

	t.Run("record_zero_starts_at_this_hosts_page_size", func(t *testing.T) {
		page := uint32(os.Getpagesize())

		if h.HeaderSize != page {
			t.Errorf("HeaderSize = %d, want this host's page size %d", h.HeaderSize, page)
		}
	})

	t.Run("every_record_decodes_to_what_was_written", func(t *testing.T) {
		for i, wantRec := range want {
			got := recordAtIndex(t, data, h, uint64(i))

			if diff := cmp.Diff(wantRec, got); diff != "" {
				t.Errorf("record %d mismatch (-want +got):\n%s", i, diff)
			}
		}
	})

	t.Run("the_time_index_holds_one_entry_per_block", func(t *testing.T) {
		idx := make([]int64, h.TimeIndexCount)
		if err := decodeTimeIndex(idx, data[h.TimeIndexOffset:]); err != nil {
			t.Fatalf("decodeTimeIndex() error = %v, want nil", err)
		}

		// Blocks of 2 over 5 records start at records 0, 2 and 4.
		if diff := cmp.Diff([]int64{1000, 1001, 1003}, idx); diff != "" {
			t.Errorf("time index mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("every_block_checksum_matches", func(t *testing.T) {
		entries := make([]footerEntry, blockCountFor(h.RecordCount, h.BlockSizeRecords))
		if err := decodeFooter(entries, data[h.FooterOffset:], h.BlockSizeRecords); err != nil {
			t.Fatalf("decodeFooter() error = %v, want nil", err)
		}
		records := data[h.HeaderSize : uint64(h.HeaderSize)+h.RecordCount*RecordSize]

		if len(entries) != 3 {
			t.Fatalf("len(entries) = %d, want 3", len(entries))
		}
		for i := range entries {
			if err := verifyBlock(records, entries, i, h.BlockSizeRecords, h.RecordCount); err != nil {
				t.Errorf("verifyBlock(%d) error = %v, want nil", i, err)
			}
		}
	})

	t.Run("a_file_with_no_snapshots_leaves_no_temp_file", func(t *testing.T) {
		if _, err := os.Stat(path + ".blob.tmp"); !os.IsNotExist(err) {
			t.Errorf("os.Stat(blob temp) error = %v, want a not-exist error", err)
		}
	})
}

func TestWriterOrderingValidation(t *testing.T) {
	tests := []struct {
		name string
		next Record
		want error
	}{
		{name: "the_same_key_twice", next: delta(1000, 5, 10), want: ErrDuplicateKey},
		{name: "exchange_ts_goes_backwards", next: delta(999, 6, 10), want: ErrOutOfOrder},
		{name: "equal_exchange_ts_with_a_lower_sequence", next: delta(1000, 4, 10), want: ErrOutOfOrder},
		{name: "equal_key_prefix_with_a_lower_instrument", next: delta(1000, 5, 9), want: ErrOutOfOrder},
		{name: "equal_exchange_ts_with_a_higher_sequence", next: delta(1000, 6, 1), want: nil},
		{name: "equal_ts_and_sequence_with_a_higher_instrument", next: delta(1000, 5, 11), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, _ := newTestWriter(t, 2)
			if err := w.WriteRecord(delta(1000, 5, 10)); err != nil {
				t.Fatalf("WriteRecord() error = %v, want nil", err)
			}

			err := w.WriteRecord(tt.next)

			if !errors.Is(err, tt.want) {
				t.Errorf("WriteRecord() error = %v, want %v", err, tt.want)
			}
		})
	}

	t.Run("a_record_from_another_venue", func(t *testing.T) {
		w, _ := newTestWriter(t, 2)
		rec := delta(1000, 1, 10)
		rec.VenueID = testVenue + 1

		err := w.WriteRecord(rec)

		if !errors.Is(err, ErrVenueMismatch) {
			t.Errorf("WriteRecord() error = %v, want %v", err, ErrVenueMismatch)
		}
	})

	t.Run("a_snapshot_pointer_through_WriteRecord", func(t *testing.T) {
		w, _ := newTestWriter(t, 2)
		rec := delta(1000, 1, 10)
		rec.RecordType = RecordTypeSnapshotPointer

		err := w.WriteRecord(rec)

		if !errors.Is(err, ErrUseWriteSnapshot) {
			t.Errorf("WriteRecord() error = %v, want %v", err, ErrUseWriteSnapshot)
		}
	})
}

func TestWriterSnapshots(t *testing.T) {
	w, path := newTestWriter(t, 4)
	bids := []Level{{Price: 100, Size: 5}, {Price: 99, Size: 7}}
	asks := []Level{{Price: 101, Size: 3}}

	if err := w.WriteRecord(delta(1000, 1, 10)); err != nil {
		t.Fatalf("WriteRecord() error = %v, want nil", err)
	}
	if err := w.WriteSnapshot(Record{ExchangeTs: 1001, SequenceNumber: 2, InstrumentID: 10, VenueID: testVenue}, bids, asks); err != nil {
		t.Fatalf("WriteSnapshot() error = %v, want nil", err)
	}
	if err := w.WriteSnapshot(Record{ExchangeTs: 1002, SequenceNumber: 3, InstrumentID: 11, VenueID: testVenue}, nil, nil); err != nil {
		t.Fatalf("WriteSnapshot() error = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	data, h := readFinalized(t, path)

	t.Run("the_pointer_record_carries_the_blob_geometry", func(t *testing.T) {
		rec := recordAtIndex(t, data, h, 1)

		if rec.RecordType != RecordTypeSnapshotPointer {
			t.Fatalf("RecordType = %d, want %d", rec.RecordType, RecordTypeSnapshotPointer)
		}
		if rec.LevelCount != 3 {
			t.Errorf("LevelCount = %d, want 3", rec.LevelCount)
		}
		if want := uint32(blobHeaderSize + 3*levelSize); rec.BlobLen != want {
			t.Errorf("BlobLen = %d, want %d", rec.BlobLen, want)
		}
		if rec.BlobOffset != 0 {
			t.Errorf("BlobOffset = %d, want 0 — the first blob starts at the region start", rec.BlobOffset)
		}
	})

	t.Run("blob_offset_is_relative_to_the_region", func(t *testing.T) {
		second := recordAtIndex(t, data, h, 2)
		first := recordAtIndex(t, data, h, 1)

		if second.BlobOffset != uint64(first.BlobLen) {
			t.Errorf("second BlobOffset = %d, want %d", second.BlobOffset, first.BlobLen)
		}
		if h.BlobRegionLen != uint64(first.BlobLen)+uint64(second.BlobLen) {
			t.Errorf("BlobRegionLen = %d, want %d", h.BlobRegionLen, uint64(first.BlobLen)+uint64(second.BlobLen))
		}
	})

	t.Run("the_blob_holds_the_levels_bids_first", func(t *testing.T) {
		rec := recordAtIndex(t, data, h, 1)
		blob := data[h.BlobRegionOffset+rec.BlobOffset:][:rec.BlobLen]

		if got := crc32.Checksum(blob[4:], castagnoli); got != binary.LittleEndian.Uint32(blob) {
			t.Errorf("blob crc = %#08x, want %#08x", binary.LittleEndian.Uint32(blob), got)
		}
		if got := binary.LittleEndian.Uint16(blob[4:]); got != 2 {
			t.Errorf("bid_count = %d, want 2", got)
		}
		if got := binary.LittleEndian.Uint16(blob[6:]); got != 1 {
			t.Errorf("ask_count = %d, want 1", got)
		}
		for i, want := range append(append([]Level{}, bids...), asks...) {
			off := blobHeaderSize + i*levelSize
			got := Level{
				Price: int64(binary.LittleEndian.Uint64(blob[off:])),
				Size:  int64(binary.LittleEndian.Uint64(blob[off+8:])),
			}

			if got != want {
				t.Errorf("level %d = %+v, want %+v", i, got, want)
			}
		}
	})

	t.Run("the_snapshot_index_names_both_pointer_records", func(t *testing.T) {
		idx := make([]uint64, h.SnapshotIndexCount)
		if err := decodeSnapshotIndex(idx, data[h.SnapshotIndexOffset:], h.RecordCount); err != nil {
			t.Fatalf("decodeSnapshotIndex() error = %v, want nil", err)
		}

		if diff := cmp.Diff([]uint64{1, 2}, idx); diff != "" {
			t.Errorf("snapshot index mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("the_blob_temp_file_is_removed", func(t *testing.T) {
		if _, err := os.Stat(path + ".blob.tmp"); !os.IsNotExist(err) {
			t.Errorf("os.Stat(blob temp) error = %v, want a not-exist error", err)
		}
	})
}

func TestWriteSnapshotRejectsPresetFields(t *testing.T) {
	tests := []struct {
		name string
		mut  func(r *Record)
	}{
		{name: "price", mut: func(r *Record) { r.Price = 1 }},
		{name: "size", mut: func(r *Record) { r.Size = 1 }},
		{name: "side_flags", mut: func(r *Record) { r.SideFlags = SideAsk }},
		{name: "blob_offset", mut: func(r *Record) { r.BlobOffset = 1 }},
		{name: "blob_len", mut: func(r *Record) { r.BlobLen = 1 }},
		{name: "level_count", mut: func(r *Record) { r.LevelCount = 1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_is_rejected_rather_than_overwritten", func(t *testing.T) {
			w, _ := newTestWriter(t, 4)
			rec := Record{ExchangeTs: 1000, SequenceNumber: 1, InstrumentID: 10, VenueID: testVenue}
			tt.mut(&rec)

			err := w.WriteSnapshot(rec, nil, nil)

			if !errors.Is(err, ErrUnusedFieldSet) {
				t.Errorf("WriteSnapshot() error = %v, want %v", err, ErrUnusedFieldSet)
			}
		})
	}
}

func TestWriterEmptyFile(t *testing.T) {
	w, path := newTestWriter(t, 4)

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	data, h := readFinalized(t, path)

	if h.RecordCount != 0 {
		t.Errorf("RecordCount = %d, want 0", h.RecordCount)
	}
	if h.TimeIndexCount != 0 || h.SnapshotIndexCount != 0 || h.BlobRegionLen != 0 {
		t.Errorf("index and blob counts = (%d, %d, %d), want all zero", h.TimeIndexCount, h.SnapshotIndexCount, h.BlobRegionLen)
	}
	if uint64(len(data)) != uint64(h.HeaderSize) {
		t.Errorf("file length = %d, want %d — an empty file is a header and nothing else", len(data), h.HeaderSize)
	}
}

func TestWriterLifecycle(t *testing.T) {
	t.Run("an_unclosed_file_has_no_magic_yet", func(t *testing.T) {
		// The header region is the zeroes reserved at construction until
		// Close writes over them. A crashed writer leaves a file nothing
		// will open, never one whose header points at an index that was
		// never written. Write past the record-stream buffer so the
		// reserved zeroes are on disk rather than still in memory.
		w, path := newTestWriter(t, 2)
		const records = writerBufSize / RecordSize
		for i := 0; i < records; i++ {
			if err := w.WriteRecord(delta(int64(1000+i), uint64(i), 10)); err != nil {
				t.Fatalf("WriteRecord() error = %v, want nil", err)
			}
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading the unclosed file: %v", err)
		}
		_, err = decodeHeader(data)

		if !errors.Is(err, ErrBadMagic) {
			t.Errorf("decodeHeader() error = %v, want %v", err, ErrBadMagic)
		}
	})

	t.Run("a_finalized_byte_that_never_landed_is_rejected", func(t *testing.T) {
		w, path := newTestWriter(t, 2)
		if err := w.WriteRecord(delta(1000, 1, 10)); err != nil {
			t.Fatalf("WriteRecord() error = %v, want nil", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
		data, _ := readFinalized(t, path)
		data[hdrOffFinalized] = 0

		h, err := decodeHeader(data)
		if err != nil {
			t.Fatalf("decodeHeader() error = %v, want nil — the checksum must not cover the finalized byte", err)
		}
		err = validateHeader(h, uint64(len(data)))

		if !errors.Is(err, ErrNotFinalized) {
			t.Errorf("validateHeader() error = %v, want %v", err, ErrNotFinalized)
		}
	})

	t.Run("writing_after_close", func(t *testing.T) {
		w, _ := newTestWriter(t, 2)
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}

		err := w.WriteRecord(delta(1000, 1, 10))

		if !errors.Is(err, ErrWriterClosed) {
			t.Errorf("WriteRecord() error = %v, want %v", err, ErrWriterClosed)
		}
	})

	t.Run("close_is_idempotent", func(t *testing.T) {
		w, _ := newTestWriter(t, 2)
		if err := w.Close(); err != nil {
			t.Fatalf("first Close() error = %v, want nil", err)
		}

		err := w.Close()

		if err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
	})

	t.Run("abort_removes_both_files", func(t *testing.T) {
		w, path := newTestWriter(t, 2)
		if err := w.WriteSnapshot(Record{ExchangeTs: 1000, SequenceNumber: 1, InstrumentID: 10, VenueID: testVenue}, []Level{{Price: 1, Size: 1}}, nil); err != nil {
			t.Fatalf("WriteSnapshot() error = %v, want nil", err)
		}

		if err := w.Abort(); err != nil {
			t.Fatalf("Abort() error = %v, want nil", err)
		}

		for _, p := range []string{path, path + ".blob.tmp"} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("os.Stat(%s) error = %v, want a not-exist error", filepath.Base(p), err)
			}
		}
	})

	t.Run("writing_after_abort", func(t *testing.T) {
		w, _ := newTestWriter(t, 2)
		if err := w.Abort(); err != nil {
			t.Fatalf("Abort() error = %v, want nil", err)
		}

		err := w.WriteRecord(delta(1000, 1, 10))

		if !errors.Is(err, ErrWriterAborted) {
			t.Errorf("WriteRecord() error = %v, want %v", err, ErrWriterAborted)
		}
	})

	t.Run("abort_after_close_leaves_the_file_alone", func(t *testing.T) {
		w, path := newTestWriter(t, 2)
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}

		if err := w.Abort(); err != nil {
			t.Fatalf("Abort() error = %v, want nil", err)
		}

		if _, err := os.Stat(path); err != nil {
			t.Errorf("os.Stat() error = %v, want nil — Abort must not remove a closed file", err)
		}
	})
}

func TestNewWriterRejects(t *testing.T) {
	t.Run("a_path_that_already_exists", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "venue.rpl")
		if err := os.WriteFile(path, []byte("existing"), 0o644); err != nil {
			t.Fatalf("seeding the path: %v", err)
		}

		_, err := NewWriter(path, testVenue, testPriceScale)

		if !os.IsExist(err) {
			t.Errorf("NewWriter() error = %v, want an already-exists error", err)
		}
	})

	t.Run("a_non_positive_price_scale", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "venue.rpl")

		_, err := NewWriter(path, testVenue, 0)

		if !errors.Is(err, ErrPriceScale) {
			t.Errorf("NewWriter() error = %v, want %v", err, ErrPriceScale)
		}
	})

	t.Run("a_zero_block_size", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "venue.rpl")
		opts := defaultWriterOptions()
		opts.blockSizeRecords = 0

		_, err := newWriter(path, testVenue, testPriceScale, opts)

		if !errors.Is(err, ErrBlockSize) {
			t.Errorf("newWriter() error = %v, want %v", err, ErrBlockSize)
		}
	})
}

func TestNewWriterWithOptions(t *testing.T) {
	hostHeaderSize, err := headerSizeFor(os.Getpagesize())
	if err != nil {
		t.Fatalf("headerSizeFor(os.Getpagesize()) error = %v, want nil", err)
	}

	tests := []struct {
		name           string
		opts           WriterOptions
		wantHeaderSize uint32
		wantBlockSize  uint32
		wantErr        error
	}{
		{
			name:           "both_fields_zero_matches_the_host_defaults",
			wantHeaderSize: hostHeaderSize,
			wantBlockSize:  defaultBlockSizeRecords,
		},
		{
			name:           "a_pinned_page_size_sets_header_size_on_every_host",
			opts:           WriterOptions{PageSize: 16384},
			wantHeaderSize: 16384,
			wantBlockSize:  defaultBlockSizeRecords,
		},
		{
			name:           "a_pinned_block_size_leaves_header_size_on_the_host_default",
			opts:           WriterOptions{BlockSizeRecords: 8},
			wantHeaderSize: hostHeaderSize,
			wantBlockSize:  8,
		},
		{
			name:           "both_fields_pinned",
			opts:           WriterOptions{PageSize: 4096, BlockSizeRecords: 2},
			wantHeaderSize: 4096,
			wantBlockSize:  2,
		},
		{
			name:    "a_negative_page_size",
			opts:    WriterOptions{PageSize: -1},
			wantErr: ErrPageSize,
		},
		{
			name:    "a_block_size_over_the_maximum",
			opts:    WriterOptions{BlockSizeRecords: maxBlockSizeRecords + 1},
			wantErr: ErrBlockSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "venue.rpl")

			w, err := NewWriterWithOptions(path, testVenue, testPriceScale, tt.opts)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("NewWriterWithOptions(%+v) error = %v, want %v", tt.opts, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewWriterWithOptions(%+v) error = %v, want nil", tt.opts, err)
			}
			if err := w.WriteRecord(delta(1000, 1, 10)); err != nil {
				t.Fatalf("WriteRecord() error = %v, want nil", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}
			_, h := readFinalized(t, path)
			if h.HeaderSize != tt.wantHeaderSize {
				t.Errorf("NewWriterWithOptions(%+v) header_size = %d, want %d", tt.opts, h.HeaderSize, tt.wantHeaderSize)
			}
			if h.BlockSizeRecords != tt.wantBlockSize {
				t.Errorf("NewWriterWithOptions(%+v) block_size_records = %d, want %d", tt.opts, h.BlockSizeRecords, tt.wantBlockSize)
			}
		})
	}
}
