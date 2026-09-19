package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// fixHeaderCRC recomputes the header checksum after a test mutates a
// header field, so the mutation reaches the semantic checks instead of
// stopping at the checksum.
func fixHeaderCRC(b []byte) {
	binary.LittleEndian.PutUint32(b[hdrOffCRC:], crc32.Checksum(b[:headerCRCLen], castagnoli))
}

// fixture is a small file with deltas, two snapshots, and a repeated
// timestamp, written with a block size of 2 so block boundaries are easy
// to reach.
type fixture struct {
	path    string
	records []Record
	bids    []Level
	asks    []Level
}

func writeFixture(t *testing.T, blockSize uint32) fixture {
	t.Helper()
	w, path := newTestWriter(t, blockSize)
	f := fixture{
		path: path,
		bids: []Level{{Price: 100, Size: 5}, {Price: 99, Size: 7}},
		asks: []Level{{Price: 101, Size: 3}},
	}

	plan := []struct {
		rec      Record
		snapshot bool
	}{
		{rec: delta(1000, 1, 10)},
		{rec: delta(1000, 2, 10)},
		{rec: Record{ExchangeTs: 1001, SequenceNumber: 3, InstrumentID: 10, VenueID: testVenue}, snapshot: true},
		{rec: delta(1002, 4, 10)},
		{rec: delta(1002, 5, 11)},
		{rec: Record{ExchangeTs: 1003, SequenceNumber: 6, InstrumentID: 11, VenueID: testVenue}, snapshot: true},
		{rec: delta(1004, 7, 11)},
	}
	for _, p := range plan {
		var err error
		if p.snapshot {
			err = w.WriteSnapshot(p.rec, f.bids, f.asks)
		} else {
			err = w.WriteRecord(p.rec)
		}
		if err != nil {
			t.Fatalf("writing the fixture: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	h, err := decodeHeader(data)
	if err != nil {
		t.Fatalf("decodeHeader() error = %v, want nil", err)
	}
	for i := uint64(0); i < h.RecordCount; i++ {
		f.records = append(f.records, recordAtIndex(t, data, h, i))
	}
	return f
}

func openFixture(t *testing.T, path string) *Reader {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestReaderOpen(t *testing.T) {
	f := writeFixture(t, 2)
	r := openFixture(t, f.path)

	t.Run("header_fields_are_readable", func(t *testing.T) {
		if r.Len() != len(f.records) {
			t.Errorf("Len() = %d, want %d", r.Len(), len(f.records))
		}
		if r.VenueID() != testVenue {
			t.Errorf("VenueID() = %d, want %d", r.VenueID(), testVenue)
		}
		if r.PriceScale() != testPriceScale {
			t.Errorf("PriceScale() = %d, want %d", r.PriceScale(), testPriceScale)
		}
		if r.BlockSizeRecords() != 2 {
			t.Errorf("BlockSizeRecords() = %d, want 2", r.BlockSizeRecords())
		}
		if want := 4; r.BlockCount() != want {
			t.Errorf("BlockCount() = %d, want %d", r.BlockCount(), want)
		}
	})

	t.Run("every_record_reads_back", func(t *testing.T) {
		for i, want := range f.records {
			got := r.RecordAt(i)

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("RecordAt(%d) mismatch (-want +got):\n%s", i, diff)
			}
		}
	})

	t.Run("every_block_and_blob_verifies", func(t *testing.T) {
		if err := r.VerifyAll(); err != nil {
			t.Errorf("VerifyAll() error = %v, want nil", err)
		}
	})

	t.Run("close_is_idempotent", func(t *testing.T) {
		r2 := openFixture(t, f.path)

		if err := r2.Close(); err != nil {
			t.Fatalf("first Close() error = %v, want nil", err)
		}
		if err := r2.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
	})
}

func TestReaderRecordAtPanics(t *testing.T) {
	f := writeFixture(t, 2)
	r := openFixture(t, f.path)

	for _, i := range []int{-1, r.Len()} {
		t.Run(fmt.Sprintf("index_%d_is_out_of_range", i), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("RecordAt(%d) did not panic", i)
				}
			}()

			r.RecordAt(i)
		})
	}
}

func TestReaderBlob(t *testing.T) {
	f := writeFixture(t, 2)
	r := openFixture(t, f.path)
	snapshot := r.RecordAt(2)

	t.Run("levels_come_back_bids_first", func(t *testing.T) {
		got, bids, err := r.AppendLevels(nil, snapshot)

		if err != nil {
			t.Fatalf("AppendLevels() error = %v, want nil", err)
		}
		if bids != len(f.bids) {
			t.Errorf("bid count = %d, want %d", bids, len(f.bids))
		}
		if diff := cmp.Diff(append(append([]Level{}, f.bids...), f.asks...), got); diff != "" {
			t.Errorf("levels mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("append_reuses_the_callers_buffer", func(t *testing.T) {
		dst := make([]Level, 0, 16)

		got, _, err := r.AppendLevels(dst, snapshot)

		if err != nil {
			t.Fatalf("AppendLevels() error = %v, want nil", err)
		}
		if len(got) != len(f.bids)+len(f.asks) {
			t.Errorf("len = %d, want %d", len(got), len(f.bids)+len(f.asks))
		}
		if cap(got) != cap(dst) {
			t.Errorf("cap = %d, want the caller's %d — AppendLevels grew the buffer", cap(got), cap(dst))
		}
	})

	t.Run("a_non_snapshot_record_has_no_blob", func(t *testing.T) {
		_, err := r.Blob(r.RecordAt(0))

		if !errors.Is(err, ErrNotSnapshot) {
			t.Errorf("Blob() error = %v, want %v", err, ErrNotSnapshot)
		}
	})

	t.Run("a_blob_offset_past_the_region", func(t *testing.T) {
		bad := snapshot
		bad.BlobOffset = 1 << 40

		_, err := r.Blob(bad)

		if !errors.Is(err, ErrBlobOutOfRange) {
			t.Errorf("Blob() error = %v, want %v", err, ErrBlobOutOfRange)
		}
	})

	t.Run("a_blob_length_that_would_wrap_uint64", func(t *testing.T) {
		bad := snapshot
		bad.BlobOffset = ^uint64(0) - 8

		_, err := r.Blob(bad)

		if !errors.Is(err, ErrBlobOutOfRange) {
			t.Errorf("Blob() error = %v, want %v", err, ErrBlobOutOfRange)
		}
	})

	t.Run("a_corrupt_blob", func(t *testing.T) {
		data, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatalf("reading the fixture: %v", err)
		}
		h, _ := decodeHeader(data)
		data[h.BlobRegionOffset+8] ^= 0x01
		corrupt, err := openBytes(data)
		if err != nil {
			t.Fatalf("openBytes() error = %v, want nil — a blob is not covered by a block checksum", err)
		}

		_, err = corrupt.Blob(corrupt.RecordAt(2))

		if !errors.Is(err, ErrBlobChecksum) {
			t.Errorf("Blob() error = %v, want %v", err, ErrBlobChecksum)
		}
	})
}

func TestReaderVerifyBlock(t *testing.T) {
	f := writeFixture(t, 2)

	t.Run("corruption_fails_only_its_own_block", func(t *testing.T) {
		data, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatalf("reading the fixture: %v", err)
		}
		h, _ := decodeHeader(data)
		const corruptBlock = 1
		// Flip a price byte, which no header field describes, so the
		// block checksum is the only thing that can catch it.
		data[uint64(h.HeaderSize)+corruptBlock*2*RecordSize+offPrice] ^= 0x01
		r, err := openBytes(data)
		if err != nil {
			t.Fatalf("openBytes() error = %v, want nil", err)
		}

		for i := 0; i < r.BlockCount(); i++ {
			err := r.VerifyBlock(i)

			if i == corruptBlock {
				if !errors.Is(err, ErrBlockChecksum) {
					t.Errorf("VerifyBlock(%d) error = %v, want %v", i, err, ErrBlockChecksum)
				}
				continue
			}
			if err != nil {
				t.Errorf("VerifyBlock(%d) error = %v, want nil", i, err)
			}
		}
	})

	t.Run("a_block_index_out_of_range", func(t *testing.T) {
		r := openFixture(t, f.path)

		for _, i := range []int{-1, r.BlockCount()} {
			err := r.VerifyBlock(i)

			if !errors.Is(err, ErrBlockRange) {
				t.Errorf("VerifyBlock(%d) error = %v, want %v", i, err, ErrBlockRange)
			}
		}
	})
}

func TestReaderEmptyFile(t *testing.T) {
	w, path := newTestWriter(t, 4)
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	r := openFixture(t, path)

	if r.Len() != 0 {
		t.Errorf("Len() = %d, want 0", r.Len())
	}
	if r.BlockCount() != 0 {
		t.Errorf("BlockCount() = %d, want 0", r.BlockCount())
	}
	if err := r.VerifyAll(); err != nil {
		t.Errorf("VerifyAll() error = %v, want nil", err)
	}
	if err := r.VerifyBlock(0); !errors.Is(err, ErrBlockRange) {
		t.Errorf("VerifyBlock(0) error = %v, want %v", err, ErrBlockRange)
	}
}

func TestOpenRejects(t *testing.T) {
	f := writeFixture(t, 2)
	good, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}

	tests := []struct {
		name string
		mut  func(b []byte) []byte
		want error
	}{
		{
			name: "an_empty_file",
			mut:  func([]byte) []byte { return nil },
			want: ErrShortHeader,
		},
		{
			name: "a_file_that_is_not_ours",
			mut:  func(b []byte) []byte { b[0] ^= 0xFF; return b },
			want: ErrBadMagic,
		},
		{
			name: "a_file_the_writer_never_finalized",
			mut:  func(b []byte) []byte { b[hdrOffFinalized] = 0; return b },
			want: ErrNotFinalized,
		},
		{
			name: "a_file_truncated_below_its_footer",
			mut:  func(b []byte) []byte { return b[:len(b)-1] },
			want: ErrOffsetChain,
		},
		{
			name: "a_header_whose_timestamp_range_is_wrong",
			mut: func(b []byte) []byte {
				b[hdrOffMinExchangeTs] ^= 0x01
				fixHeaderCRC(b)
				return b
			},
			want: ErrHeaderRange,
		},
		{
			name: "a_snapshot_index_entry_past_the_last_record",
			mut: func(b []byte) []byte {
				h, _ := decodeHeader(b)
				b[h.SnapshotIndexOffset] = byte(h.RecordCount)
				return b
			},
			want: ErrIndexRange,
		},
		{
			name: "a_time_index_that_goes_backwards",
			mut: func(b []byte) []byte {
				h, _ := decodeHeader(b)
				b[h.TimeIndexOffset+indexEntrySize] = 0
				return b
			},
			want: ErrIndexOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := make([]byte, len(good))
			copy(b, good)

			_, err := openBytes(tt.mut(b))

			if !errors.Is(err, tt.want) {
				t.Errorf("openBytes() error = %v, want %v", err, tt.want)
			}
		})
	}

	t.Run("a_path_that_does_not_exist", func(t *testing.T) {
		_, err := Open(filepath.Join(t.TempDir(), "absent.rpl"))

		if !os.IsNotExist(err) {
			t.Errorf("Open() error = %v, want a not-exist error", err)
		}
	})
}
