package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// goldenHeader describes a coherent 1024-record file. Later tests reuse
// it, so its offsets form a valid chain even though this file's tests
// only care about field placement.
func goldenHeader() header {
	return header{
		FormatVersion:       formatVersion,
		VenueID:             0x1234,
		PriceScale:          100_000_000,
		RecordCount:         1024,
		MinExchangeTs:       0x0102030405060708,
		MaxExchangeTs:       0x1112131415161718,
		HeaderSize:          4096,
		BlockSizeRecords:    512,
		BlobRegionOffset:    4096 + 1024*RecordSize,
		BlobRegionLen:       168,
		TimeIndexOffset:     4096 + 1024*RecordSize + 168,
		TimeIndexCount:      2,
		SnapshotIndexOffset: 4096 + 1024*RecordSize + 168 + 16,
		SnapshotIndexCount:  3,
		FooterOffset:        4096 + 1024*RecordSize + 168 + 16 + 24,
		TrailerCRC:          0xDEADBEEF,
	}
}

func le16(v uint16) []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, v); return b }
func le32(v uint32) []byte { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, v); return b }
func le64(v uint64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, v); return b }

func TestHeaderFieldOffsets(t *testing.T) {
	// The offsets below are written out from docs/format.md, not read
	// from the constants encodeHeader uses, so a field that moves fails
	// here. The little-endian helpers are not what is under test.
	h := goldenHeader()
	var buf [headerFieldsSize]byte
	for i := range buf {
		buf[i] = 0xAB
	}

	encodeHeader(buf[:], h)

	checks := []struct {
		name string
		off  int
		want []byte
	}{
		{"magic", 0, []byte{0x89, 'R', 'P', 'L', 0x0D, 0x0A, 0x1A, 0x0A}},
		{"format_version", 8, le32(1)},
		{"venue_id", 12, le16(0x1234)},
		{"reserved1", 14, []byte{0, 0}},
		{"price_scale", 16, le64(100_000_000)},
		{"record_count", 24, le64(1024)},
		{"min_exchange_ts", 32, le64(0x0102030405060708)},
		{"max_exchange_ts", 40, le64(0x1112131415161718)},
		{"header_size", 48, le32(4096)},
		{"block_size_records", 52, le32(512)},
		{"blob_region_offset", 56, le64(69632)},
		{"blob_region_len", 64, le64(168)},
		{"time_index_offset", 72, le64(69800)},
		{"time_index_count", 80, le64(2)},
		{"snapshot_index_offset", 88, le64(69816)},
		{"snapshot_index_count", 96, le64(3)},
		{"footer_offset", 104, le64(69840)},
		{"trailer_crc32c", 112, le32(0xDEADBEEF)},
		{"reserved2", 116, make([]byte, 4)},
		{"finalized", 120, []byte{0}},
		{"reserved3", 121, make([]byte, 3)},
	}

	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			got := buf[c.off : c.off+len(c.want)]

			if diff := cmp.Diff(c.want, got); diff != "" {
				t.Errorf("bytes at offset %d (-want +got):\n%s", c.off, diff)
			}
		})
	}

	t.Run("checksum_is_the_last_four_bytes", func(t *testing.T) {
		want := crc32.Checksum(buf[:headerCRCLen], castagnoli)

		got := binary.LittleEndian.Uint32(buf[124:])

		if got != want {
			t.Errorf("header crc = %#x, want %#x", got, want)
		}
	})
}

func TestHeaderRoundTrip(t *testing.T) {
	t.Run("every_field_survives_encode_then_decode", func(t *testing.T) {
		want := goldenHeader()
		var buf [headerFieldsSize]byte

		encodeHeader(buf[:], want)
		got, err := decodeHeader(buf[:])

		if err != nil {
			t.Fatalf("decodeHeader() error = %v, want nil", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("round trip mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("encode_clears_the_finalized_byte", func(t *testing.T) {
		h := goldenHeader()
		h.Finalized = true
		var buf [headerFieldsSize]byte

		encodeHeader(buf[:], h)

		if buf[hdrOffFinalized] != 0 {
			t.Errorf("finalized byte = %d after encode, want 0 — Close sets it, not encodeHeader", buf[hdrOffFinalized])
		}
	})

	t.Run("setting_the_finalized_byte_keeps_the_checksum_valid", func(t *testing.T) {
		// This is the whole point of putting finalized outside the
		// checksummed range: the writer flips one byte last, after every
		// other byte is durable, without recomputing anything.
		var buf [headerFieldsSize]byte
		encodeHeader(buf[:], goldenHeader())

		buf[hdrOffFinalized] = 1
		got, err := decodeHeader(buf[:])

		if err != nil {
			t.Fatalf("decodeHeader() error = %v, want nil", err)
		}
		if !got.Finalized {
			t.Error("Finalized = false, want true")
		}
	})
}

func TestHeaderSizeFor(t *testing.T) {
	tests := []struct {
		name     string
		pageSize int
		want     uint32
		wantErr  error
	}{
		{name: "four_kilobyte_page", pageSize: 4096, want: 4096},
		{name: "sixteen_kilobyte_page_on_apple_silicon", pageSize: 16384, want: 16384},
		{name: "page_smaller_than_the_header_fields_rounds_up", pageSize: 64, want: 128},
		{name: "page_exactly_the_header_fields_size", pageSize: 128, want: 128},
		{name: "zero_page_size", pageSize: 0, wantErr: ErrPageSize},
		{name: "negative_page_size", pageSize: -1, wantErr: ErrPageSize},
		{name: "page_larger_than_any_plausible_header", pageSize: maxHeaderSize + 1, wantErr: ErrPageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := headerSizeFor(tt.pageSize)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("headerSizeFor(%d) error = %v, want %v", tt.pageSize, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("headerSizeFor(%d) = %d, want %d", tt.pageSize, got, tt.want)
			}
		})
	}

	t.Run("this_hosts_page_size_gives_a_page_aligned_record_zero", func(t *testing.T) {
		page := os.Getpagesize()

		got, err := headerSizeFor(page)

		if err != nil {
			t.Fatalf("headerSizeFor(%d) error = %v, want nil", page, err)
		}
		if int(got)%page != 0 {
			t.Errorf("headerSizeFor(%d) = %d, which is not a multiple of the page size", page, got)
		}
		if int(got) < headerFieldsSize {
			t.Errorf("headerSizeFor(%d) = %d, which does not cover the %d header bytes", page, got, headerFieldsSize)
		}
	})
}

func TestDecodeHeaderRejects(t *testing.T) {
	valid := func() []byte {
		b := make([]byte, headerFieldsSize)
		encodeHeader(b, goldenHeader())
		return b
	}

	tests := []struct {
		name string
		buf  func() []byte
		want error
	}{
		{
			name: "buffer_shorter_than_the_header_fields",
			buf:  func() []byte { return valid()[:headerFieldsSize-1] },
			want: ErrShortHeader,
		},
		{
			name: "magic_first_byte_changed",
			buf:  func() []byte { b := valid(); b[0] ^= 0xFF; return b },
			want: ErrBadMagic,
		},
		{
			name: "magic_line_ending_converted",
			buf: func() []byte {
				// What git on Windows does to a file it thinks is text.
				b := valid()
				b[4], b[5], b[6], b[7] = 0x0A, 0x1A, 0x0A, 0x00
				return b
			},
			want: ErrBadMagic,
		},
		{
			name: "record_count_changed_without_the_checksum",
			buf:  func() []byte { b := valid(); b[hdrOffRecordCount] ^= 0x01; return b },
			want: ErrHeaderCRC,
		},
		{
			name: "checksum_itself_changed",
			buf:  func() []byte { b := valid(); b[hdrOffCRC] ^= 0x01; return b },
			want: ErrHeaderCRC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeHeader(tt.buf())

			if !errors.Is(err, tt.want) {
				t.Errorf("decodeHeader() error = %v, want %v", err, tt.want)
			}
		})
	}
}
