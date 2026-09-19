package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// storeErrors is every error this package returns. The fuzz target
// asserts that a rejection is one of these: a leaked io.ErrUnexpectedEOF
// or a raw encoding error would mean a decoder read past what it
// checked. Adding a sentinel without adding it here fails the fuzz
// target, which is the intended reminder.
var storeErrors = []error{
	ErrShortRecord, ErrRecordType, ErrSideFlags, ErrReserved, ErrUnusedFieldSet, ErrBlobLen,
	ErrShortHeader, ErrBadMagic, ErrHeaderCRC, ErrTrailerCRC, ErrPageSize,
	ErrFormatVersion, ErrNotFinalized, ErrHeaderSize, ErrBlockSize, ErrPriceScale,
	ErrRecordCount, ErrOffsetChain,
	ErrShortIndex, ErrIndexOrder, ErrIndexRange, ErrIndexCount,
	ErrShortFooter, ErrFooterGeometry, ErrBlockRange, ErrBlockChecksum,
	ErrWriterClosed, ErrWriterAborted, ErrVenueMismatch, ErrOutOfOrder, ErrDuplicateKey,
	ErrUseWriteSnapshot, ErrTooManyLevels,
	ErrHeaderRange, ErrNotSnapshot, ErrBlobOutOfRange, ErrBlobChecksum, ErrBlobLevelCount,
}

func requireStoreError(t *testing.T, what string, err error) {
	t.Helper()
	for _, e := range storeErrors {
		if errors.Is(err, e) {
			return
		}
	}
	t.Fatalf("%s returned %v, which is not one of this package's errors", what, err)
}

// FuzzDecode drives every decoder in this package. It must never panic,
// must never read out of bounds, and must reject malformed input with a
// typed error. See docs/format.md.
func FuzzDecode(f *testing.F) {
	for _, seed := range fuzzSeeds(f) {
		f.Add(uint8(0), seed)
		f.Add(uint8(1), seed)
		f.Add(uint8(2), seed)
	}

	f.Fuzz(func(t *testing.T, sel uint8, data []byte) {
		switch sel % 3 {
		case 0:
			fuzzRecord(t, data)
		case 1:
			fuzzHeader(t, data)
		default:
			fuzzOpen(t, data)
		}
	})
}

// fuzzRecord asserts the strongest property the record codec has: if a
// record decodes, re-encoding it reproduces the bytes exactly. That
// holds only while every byte of a record is either a field or checked
// to be zero, so this fails the moment a decoder starts ignoring
// something.
func fuzzRecord(t *testing.T, data []byte) {
	rec, err := decodeRecord(data)
	if err != nil {
		requireStoreError(t, "decodeRecord", err)
		return
	}

	var buf [RecordSize]byte
	encodeRecord(buf[:], rec)

	if !bytes.Equal(buf[:], data[:RecordSize]) {
		t.Fatalf("decode then encode changed the bytes\n got: %x\nwant: %x", buf[:], data[:RecordSize])
	}
}

func fuzzHeader(t *testing.T, data []byte) {
	h, err := decodeHeader(data)
	if err != nil {
		requireStoreError(t, "decodeHeader", err)
		return
	}

	var buf [headerFieldsSize]byte
	encodeHeader(buf[:], h)
	if h.Finalized {
		setFinalized(buf[:])
	}

	if !bytes.Equal(buf[:], data[:headerFieldsSize]) {
		t.Fatalf("decode then encode changed the header bytes\n got: %x\nwant: %x", buf[:], data[:headerFieldsSize])
	}
	if err := validateHeader(h, uint64(len(data))); err != nil {
		requireStoreError(t, "validateHeader", err)
	}
}

func fuzzOpen(t *testing.T, data []byte) {
	r, err := openBytes(data)
	if err != nil {
		requireStoreError(t, "openBytes", err)
		return
	}
	defer func() { _ = r.Close() }()

	// Integrity first. Verification is lazy by design, so a file whose
	// record array contradicts its own indexes still opens: RecordAt is
	// unchecked and the block checksum is what stands between a caller
	// and a corrupt record. The ordering properties below are therefore
	// asserted only once the blocks verify. The structural properties —
	// no panic, a result inside the record array, a snapshot at or
	// before the record asked for — hold either way, because they come
	// from bounds this package checks at Open.
	intact := true
	for b := 0; b < r.BlockCount(); b++ {
		if err := r.VerifyBlock(b); err != nil {
			requireStoreError(t, "VerifyBlock", err)
			intact = false
		}
	}

	for i := 0; i < r.Len(); i++ {
		rec := r.RecordAt(i)

		j, err := r.SeekTime(rec.ExchangeTs)
		switch {
		case err != nil:
			requireStoreError(t, "SeekTime", err)
		case j < 0 || j > r.Len():
			t.Fatalf("SeekTime(%d) = %d, outside [0, %d]", rec.ExchangeTs, j, r.Len())
		case !intact:
		case j > i:
			t.Fatalf("SeekTime(%d) = %d, want at most %d", rec.ExchangeTs, j, i)
		case j < r.Len() && r.RecordAt(j).ExchangeTs < rec.ExchangeTs:
			t.Fatalf("SeekTime(%d) landed on a record holding %d", rec.ExchangeTs, r.RecordAt(j).ExchangeTs)
		}

		if s, ok := r.SnapshotBefore(i); ok {
			if s < 0 || s > i {
				t.Fatalf("SnapshotBefore(%d) = %d, outside [0, %d]", i, s, i)
			}
			if intact && r.RecordAt(s).RecordType != RecordTypeSnapshotPointer {
				t.Fatalf("SnapshotBefore(%d) named record %d, which is not a snapshot pointer", i, s)
			}
		}

		if rec.RecordType != RecordTypeSnapshotPointer {
			continue
		}
		payload, err := r.Blob(rec)
		if err != nil {
			requireStoreError(t, "Blob", err)
			continue
		}
		if len(payload) != int(rec.BlobLen)-4 {
			t.Fatalf("Blob() returned %d bytes, want %d", len(payload), int(rec.BlobLen)-4)
		}
	}
}

// fuzzSeeds builds the corpus: valid files, the record goldens, and
// mutations that each aim at one decoder's bounds.
func fuzzSeeds(f *testing.F) [][]byte {
	f.Helper()
	seeds := [][]byte{
		nil,
		{0x00},
		make([]byte, headerFieldsSize),
	}
	for _, g := range goldenRecords {
		seeds = append(seeds, g.buf)
	}

	empty := fuzzBuildFile(f, false)
	full := fuzzBuildFile(f, true)
	seeds = append(seeds, empty, full)

	h, err := decodeHeader(full)
	if err != nil {
		f.Fatalf("decodeHeader() on the seed file: %v", err)
	}

	mutations := []struct {
		name string
		mut  func(b []byte) []byte
	}{
		{"truncated_to_the_header", func(b []byte) []byte { return b[:headerFieldsSize] }},
		{"truncated_by_one_byte", func(b []byte) []byte { return b[:len(b)-1] }},
		{"finalized_cleared", func(b []byte) []byte { b[hdrOffFinalized] = 0; return b }},
		{"finalized_set_to_an_undefined_value", func(b []byte) []byte { b[hdrOffFinalized] = 0xFF; return b }},
		{"magic_corrupted", func(b []byte) []byte { b[0] ^= 0xFF; return b }},
		{"format_version_bumped", func(b []byte) []byte { b[hdrOffFormatVersion]++; fixHeaderCRC(b); return b }},
		{"record_count_at_the_ceiling", func(b []byte) []byte { fill(b[hdrOffRecordCount:hdrOffRecordCount+8], 0xFF); fixHeaderCRC(b); return b }},
		{"block_size_zero", func(b []byte) []byte {
			fill(b[hdrOffBlockSizeRecords:hdrOffBlockSizeRecords+4], 0)
			fixHeaderCRC(b)
			return b
		}},
		{"time_index_offset_past_the_file", func(b []byte) []byte {
			fill(b[hdrOffTimeIndexOffset:hdrOffTimeIndexOffset+8], 0xFF)
			fixHeaderCRC(b)
			return b
		}},
		{"blob_region_past_the_time_index", func(b []byte) []byte {
			fill(b[hdrOffBlobRegionLen:hdrOffBlobRegionLen+8], 0xFF)
			fixHeaderCRC(b)
			return b
		}},
		{"header_reserved_byte_set", func(b []byte) []byte { b[hdrOffReserved2] = 0xFF; fixHeaderCRC(b); return b }},
		{"a_record_byte_flipped", func(b []byte) []byte { b[uint64(h.HeaderSize)+offPrice] ^= 0x01; return b }},
		{"a_record_reserved_byte_set", func(b []byte) []byte { b[uint64(h.HeaderSize)+offReserved] = 0xFF; return b }},
		{"an_undefined_record_type", func(b []byte) []byte { b[uint64(h.HeaderSize)+offRecordType] = 200; return b }},
		{"a_blob_offset_at_the_ceiling", func(b []byte) []byte {
			fill(b[uint64(h.HeaderSize)+offBlobOffset:][:8], 0xFF)
			return b
		}},
		{"a_level_count_that_disagrees", func(b []byte) []byte { b[uint64(h.HeaderSize)+offLevelCount] ^= 0xFF; return b }},
		{"a_footer_entry_moved", func(b []byte) []byte { b[h.FooterOffset] ^= 0xFF; return b }},
		{"a_snapshot_index_entry_moved", func(b []byte) []byte { b[h.SnapshotIndexOffset] ^= 0xFF; return b }},
		{"a_time_index_entry_moved", func(b []byte) []byte { b[h.TimeIndexOffset] ^= 0xFF; return b }},
	}
	for _, m := range mutations {
		b := make([]byte, len(full))
		copy(b, full)
		seeds = append(seeds, m.mut(b))
	}
	return seeds
}

func fill(b []byte, v byte) {
	for i := range b {
		b[i] = v
	}
}

// fuzzBuildFile writes a real file and returns its bytes.
func fuzzBuildFile(f *testing.F, withRecords bool) []byte {
	f.Helper()
	name := "empty.rpl"
	if withRecords {
		name = "full.rpl"
	}
	path := filepath.Join(f.TempDir(), name)
	opts := defaultWriterOptions()
	opts.blockSizeRecords = 2
	w, err := newWriter(path, testVenue, testPriceScale, opts)
	if err != nil {
		f.Fatalf("newWriter() error = %v", err)
	}

	if withRecords {
		steps := []func() error{
			func() error { return w.WriteRecord(delta(1000, 1, 10)) },
			func() error { return w.WriteRecord(delta(1000, 2, 11)) },
			func() error {
				return w.WriteSnapshot(
					Record{ExchangeTs: 1001, SequenceNumber: 3, InstrumentID: 10, VenueID: testVenue},
					[]Level{{Price: 100, Size: 5}, {Price: 99, Size: 4}},
					[]Level{{Price: 101, Size: 2}},
				)
			},
			func() error { return w.WriteRecord(delta(1002, 4, 10)) },
			func() error { return w.WriteRecord(delta(1002, 5, 11)) },
		}
		for i, step := range steps {
			if err := step(); err != nil {
				f.Fatalf("writing seed record %d: %v", i, err)
			}
		}
	}
	if err := w.Close(); err != nil {
		f.Fatalf("Close() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		f.Fatalf("reading the seed file: %v", err)
	}
	return data
}
