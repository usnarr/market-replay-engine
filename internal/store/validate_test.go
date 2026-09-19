package store

import (
	"errors"
	"testing"
)

// finalizedHeader is goldenHeader with the finalized byte set, together
// with the file length its region chain implies exactly.
func finalizedHeader() (header, uint64) {
	h := goldenHeader()
	h.Finalized = true
	return h, h.FooterOffset + blockCountFor(h.RecordCount, h.BlockSizeRecords)*footerEntrySize
}

// emptyHeader is a finalized file that holds no records at all. An
// empty venue partition is normal, not an error.
func emptyHeader() (header, uint64) {
	h := header{
		FormatVersion:       formatVersion,
		VenueID:             7,
		PriceScale:          100_000_000,
		HeaderSize:          4096,
		BlockSizeRecords:    512,
		BlobRegionOffset:    4096,
		TimeIndexOffset:     4096,
		SnapshotIndexOffset: 4096,
		FooterOffset:        4096,
		Finalized:           true,
	}
	return h, 4096
}

func TestValidateHeader(t *testing.T) {
	t.Run("a_coherent_file_validates", func(t *testing.T) {
		h, fileLen := finalizedHeader()

		err := validateHeader(h, fileLen)

		if err != nil {
			t.Errorf("validateHeader() error = %v, want nil", err)
		}
	})

	t.Run("a_finalized_file_with_no_records_validates", func(t *testing.T) {
		h, fileLen := emptyHeader()

		err := validateHeader(h, fileLen)

		if err != nil {
			t.Errorf("validateHeader() error = %v, want nil", err)
		}
	})

	t.Run("a_file_longer_than_its_regions_validates", func(t *testing.T) {
		// Trailing bytes are not this function's concern; a short file is.
		h, fileLen := finalizedHeader()

		err := validateHeader(h, fileLen+4096)

		if err != nil {
			t.Errorf("validateHeader() error = %v, want nil", err)
		}
	})

	tests := []struct {
		name string
		mut  func(h *header, fileLen *uint64)
		want error
	}{
		{
			name: "format_version_from_a_future_release",
			mut:  func(h *header, _ *uint64) { h.FormatVersion = formatVersion + 1 },
			want: ErrFormatVersion,
		},
		{
			name: "writer_died_before_setting_the_finalized_byte",
			mut:  func(h *header, _ *uint64) { h.Finalized = false },
			want: ErrNotFinalized,
		},
		{
			name: "header_size_below_the_header_fields",
			mut:  func(h *header, _ *uint64) { h.HeaderSize = headerFieldsSize - 1 },
			want: ErrHeaderSize,
		},
		{
			name: "header_size_past_any_plausible_header",
			mut:  func(h *header, _ *uint64) { h.HeaderSize = maxHeaderSize + 1 },
			want: ErrHeaderSize,
		},
		{
			name: "block_size_zero_would_divide_by_zero",
			mut:  func(h *header, _ *uint64) { h.BlockSizeRecords = 0 },
			want: ErrBlockSize,
		},
		{
			name: "block_size_past_its_bound",
			mut:  func(h *header, _ *uint64) { h.BlockSizeRecords = maxBlockSizeRecords + 1 },
			want: ErrBlockSize,
		},
		{
			name: "price_scale_zero",
			mut:  func(h *header, _ *uint64) { h.PriceScale = 0 },
			want: ErrPriceScale,
		},
		{
			name: "price_scale_negative",
			mut:  func(h *header, _ *uint64) { h.PriceScale = -1 },
			want: ErrPriceScale,
		},
		{
			name: "record_count_past_its_bound",
			mut:  func(h *header, _ *uint64) { h.RecordCount = maxRecordCount + 1 },
			want: ErrRecordCount,
		},
		{
			name: "record_count_at_the_uint64_ceiling",
			mut:  func(h *header, _ *uint64) { h.RecordCount = ^uint64(0) },
			want: ErrRecordCount,
		},
		{
			name: "blob_region_starts_inside_the_record_array",
			mut:  func(h *header, _ *uint64) { h.BlobRegionOffset -= RecordSize },
			want: ErrOffsetChain,
		},
		{
			name: "blob_region_length_wraps_uint64",
			mut:  func(h *header, _ *uint64) { h.BlobRegionLen = ^uint64(0) },
			want: ErrOffsetChain,
		},
		{
			name: "time_index_starts_inside_the_blob_region",
			mut:  func(h *header, _ *uint64) { h.TimeIndexOffset -= 8 },
			want: ErrOffsetChain,
		},
		{
			name: "time_index_count_wraps_uint64",
			mut:  func(h *header, _ *uint64) { h.TimeIndexCount = ^uint64(0) },
			want: ErrOffsetChain,
		},
		{
			name: "snapshot_index_starts_inside_the_time_index",
			mut:  func(h *header, _ *uint64) { h.SnapshotIndexOffset -= 8 },
			want: ErrOffsetChain,
		},
		{
			name: "snapshot_index_count_wraps_uint64",
			mut:  func(h *header, _ *uint64) { h.SnapshotIndexCount = ^uint64(0) },
			want: ErrOffsetChain,
		},
		{
			name: "footer_starts_inside_the_snapshot_index",
			mut:  func(h *header, _ *uint64) { h.FooterOffset -= 8 },
			want: ErrOffsetChain,
		},
		{
			name: "footer_runs_one_byte_past_the_file",
			mut:  func(_ *header, fileLen *uint64) { *fileLen-- },
			want: ErrOffsetChain,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fileLen := finalizedHeader()
			tt.mut(&h, &fileLen)

			err := validateHeader(h, fileLen)

			if !errors.Is(err, tt.want) {
				t.Errorf("validateHeader() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestSetFinalized(t *testing.T) {
	t.Run("a_header_is_rejected_until_the_byte_is_set", func(t *testing.T) {
		h, fileLen := finalizedHeader()
		buf := make([]byte, headerFieldsSize)
		encodeHeader(buf, h)

		before, err := decodeHeader(buf)
		if err != nil {
			t.Fatalf("decodeHeader() error = %v, want nil", err)
		}
		if err := validateHeader(before, fileLen); !errors.Is(err, ErrNotFinalized) {
			t.Fatalf("validateHeader() before setFinalized = %v, want %v", err, ErrNotFinalized)
		}

		setFinalized(buf)

		after, err := decodeHeader(buf)
		if err != nil {
			t.Fatalf("decodeHeader() after setFinalized error = %v, want nil", err)
		}
		if err := validateHeader(after, fileLen); err != nil {
			t.Errorf("validateHeader() after setFinalized = %v, want nil", err)
		}
	})

	t.Run("only_the_finalized_byte_changes", func(t *testing.T) {
		h, _ := finalizedHeader()
		buf := make([]byte, headerFieldsSize)
		encodeHeader(buf, h)
		before := make([]byte, headerFieldsSize)
		copy(before, buf)

		setFinalized(buf)

		for i := range buf {
			if i == hdrOffFinalized {
				continue
			}
			if buf[i] != before[i] {
				t.Errorf("byte %d changed from %#x to %#x, want only byte %d to change", i, before[i], buf[i], hdrOffFinalized)
			}
		}
	})
}

func TestBlockCountFor(t *testing.T) {
	tests := []struct {
		name        string
		recordCount uint64
		blockSize   uint32
		want        uint64
	}{
		{name: "no_records_means_no_blocks", recordCount: 0, blockSize: 512, want: 0},
		{name: "one_record_fills_one_short_block", recordCount: 1, blockSize: 512, want: 1},
		{name: "an_exact_multiple", recordCount: 1024, blockSize: 512, want: 2},
		{name: "one_past_an_exact_multiple", recordCount: 1025, blockSize: 512, want: 3},
		{name: "one_short_of_an_exact_multiple", recordCount: 1023, blockSize: 512, want: 2},
		{name: "block_size_one", recordCount: 7, blockSize: 1, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blockCountFor(tt.recordCount, tt.blockSize)

			if got != tt.want {
				t.Errorf("blockCountFor(%d, %d) = %d, want %d", tt.recordCount, tt.blockSize, got, tt.want)
			}
		})
	}
}
