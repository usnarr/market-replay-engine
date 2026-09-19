package store

import (
	"errors"
	"hash/crc32"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// makeRecordBytes builds an encoded record array of n delta records with
// strictly increasing keys.
func makeRecordBytes(n int) []byte {
	buf := make([]byte, n*RecordSize)
	for i := 0; i < n; i++ {
		encodeRecord(buf[i*RecordSize:], Record{
			ExchangeTs:     int64(1000 + i),
			SequenceNumber: uint64(i),
			InstrumentID:   1,
			VenueID:        7,
			RecordType:     RecordTypeDelta,
			Price:          int64(100 + i),
			Size:           1,
		})
	}
	return buf
}

// buildFooter computes the footer entries for a record array.
func buildFooter(records []byte, blockSize uint32, recordCount uint64) []footerEntry {
	entries := make([]footerEntry, blockCountFor(recordCount, blockSize))
	for i := range entries {
		b, ok := blockRecordBytes(records, i, blockSize, recordCount)
		if !ok {
			panic("block out of range while building a footer")
		}
		entries[i] = footerEntry{
			BlockStartIndex: uint64(i) * uint64(blockSize),
			CRC32C:          blockChecksum(b),
		}
	}
	return entries
}

func TestCastagnoliCheckValue(t *testing.T) {
	// The published CRC-32C check value. It pins the polynomial: the same
	// input under crc32.IEEE gives 0xCBF43926, so a table swapped by
	// accident fails here rather than silently changing every checksum in
	// every file this package has ever written.
	const want = 0xE3069283

	got := crc32.Checksum([]byte("123456789"), castagnoli)

	if got != want {
		t.Errorf("crc32c(\"123456789\") = %#08x, want %#08x", got, want)
	}
}

func TestFooterRoundTrip(t *testing.T) {
	records := makeRecordBytes(5)
	want := buildFooter(records, 2, 5)
	buf := make([]byte, len(want)*footerEntrySize)
	got := make([]footerEntry, len(want))

	encodeFooter(buf, want)
	err := decodeFooter(got, buf, 2)

	if err != nil {
		t.Fatalf("decodeFooter() error = %v, want nil", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

func TestDecodeFooterRejects(t *testing.T) {
	records := makeRecordBytes(5)
	entries := buildFooter(records, 2, 5)
	buf := make([]byte, len(entries)*footerEntrySize)
	encodeFooter(buf, entries)

	t.Run("region_shorter_than_the_entry_count", func(t *testing.T) {
		err := decodeFooter(make([]footerEntry, len(entries)), buf[:len(buf)-1], 2)

		if !errors.Is(err, ErrShortFooter) {
			t.Errorf("decodeFooter() error = %v, want %v", err, ErrShortFooter)
		}
	})

	t.Run("entry_does_not_start_where_its_block_does", func(t *testing.T) {
		bad := make([]byte, len(buf))
		copy(bad, buf)
		bad[footerEntrySize] ^= 0x01 // entry 1's block_start_index

		err := decodeFooter(make([]footerEntry, len(entries)), bad, 2)

		if !errors.Is(err, ErrFooterGeometry) {
			t.Errorf("decodeFooter() error = %v, want %v", err, ErrFooterGeometry)
		}
	})

	t.Run("entries_decoded_against_the_wrong_block_size", func(t *testing.T) {
		err := decodeFooter(make([]footerEntry, len(entries)), buf, 4)

		if !errors.Is(err, ErrFooterGeometry) {
			t.Errorf("decodeFooter() error = %v, want %v", err, ErrFooterGeometry)
		}
	})
}

func TestBlockRecordBytes(t *testing.T) {
	records := makeRecordBytes(5)

	tests := []struct {
		name       string
		blockIndex int
		wantStart  int
		wantCount  int
		wantOK     bool
	}{
		{name: "first_full_block", blockIndex: 0, wantStart: 0, wantCount: 2, wantOK: true},
		{name: "second_full_block", blockIndex: 1, wantStart: 2, wantCount: 2, wantOK: true},
		{name: "final_short_block", blockIndex: 2, wantStart: 4, wantCount: 1, wantOK: true},
		{name: "one_block_past_the_end", blockIndex: 3, wantOK: false},
		{name: "negative_block_index", blockIndex: -1, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := blockRecordBytes(records, tt.blockIndex, 2, 5)

			if ok != tt.wantOK {
				t.Fatalf("blockRecordBytes(%d) ok = %v, want %v", tt.blockIndex, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if len(got) != tt.wantCount*RecordSize {
				t.Errorf("block %d covers %d bytes, want %d", tt.blockIndex, len(got), tt.wantCount*RecordSize)
			}
			if diff := cmp.Diff(records[tt.wantStart*RecordSize:(tt.wantStart+tt.wantCount)*RecordSize], got); diff != "" {
				t.Errorf("block %d bytes (-want +got):\n%s", tt.blockIndex, diff)
			}
		})
	}

	t.Run("the_final_short_block_is_not_padded_out", func(t *testing.T) {
		// Growing the record array must not change the last block's
		// checksum. If the block were padded to a full blockSize, the
		// extra record's bytes would be inside the covered range.
		short := makeRecordBytes(5)
		long := makeRecordBytes(6)
		b, _ := blockRecordBytes(short, 2, 2, 5)
		want := blockChecksum(b)

		b2, _ := blockRecordBytes(long, 2, 2, 5)
		got := blockChecksum(b2)

		if got != want {
			t.Errorf("last block checksum = %#08x over a 6-record array, want %#08x — the block is padded", got, want)
		}
	})

	t.Run("a_file_with_no_records_has_no_blocks", func(t *testing.T) {
		_, ok := blockRecordBytes(nil, 0, 2, 0)

		if ok {
			t.Error("blockRecordBytes() ok = true for an empty file, want false")
		}
	})
}

func TestVerifyBlock(t *testing.T) {
	const blockSize = 2
	const recordCount = 5

	t.Run("every_block_of_an_intact_file_verifies", func(t *testing.T) {
		records := makeRecordBytes(recordCount)
		entries := buildFooter(records, blockSize, recordCount)

		for i := range entries {
			if err := verifyBlock(records, entries, i, blockSize, recordCount); err != nil {
				t.Errorf("verifyBlock(%d) error = %v, want nil", i, err)
			}
		}
	})

	t.Run("corruption_fails_only_the_block_that_holds_it", func(t *testing.T) {
		// This is what "verify lazily, per block" buys: one damaged block
		// does not cost the reader the rest of the file.
		records := makeRecordBytes(recordCount)
		entries := buildFooter(records, blockSize, recordCount)
		const corrupt = 1
		records[corrupt*blockSize*RecordSize] ^= 0x01

		for i := range entries {
			err := verifyBlock(records, entries, i, blockSize, recordCount)

			if i == corrupt {
				if !errors.Is(err, ErrBlockChecksum) {
					t.Errorf("verifyBlock(%d) error = %v, want %v", i, err, ErrBlockChecksum)
				}
				continue
			}
			if err != nil {
				t.Errorf("verifyBlock(%d) error = %v, want nil", i, err)
			}
		}
	})

	t.Run("corruption_in_the_final_short_block_is_caught", func(t *testing.T) {
		records := makeRecordBytes(recordCount)
		entries := buildFooter(records, blockSize, recordCount)
		records[4*RecordSize] ^= 0x01

		err := verifyBlock(records, entries, 2, blockSize, recordCount)

		if !errors.Is(err, ErrBlockChecksum) {
			t.Errorf("verifyBlock(2) error = %v, want %v", err, ErrBlockChecksum)
		}
	})

	t.Run("block_index_out_of_range", func(t *testing.T) {
		records := makeRecordBytes(recordCount)
		entries := buildFooter(records, blockSize, recordCount)

		for _, i := range []int{-1, len(entries)} {
			err := verifyBlock(records, entries, i, blockSize, recordCount)

			if !errors.Is(err, ErrBlockRange) {
				t.Errorf("verifyBlock(%d) error = %v, want %v", i, err, ErrBlockRange)
			}
		}
	})

	t.Run("an_exact_multiple_of_the_block_size", func(t *testing.T) {
		records := makeRecordBytes(4)
		entries := buildFooter(records, blockSize, 4)

		if len(entries) != 2 {
			t.Fatalf("len(entries) = %d, want 2", len(entries))
		}
		for i := range entries {
			if err := verifyBlock(records, entries, i, blockSize, 4); err != nil {
				t.Errorf("verifyBlock(%d) error = %v, want nil", i, err)
			}
		}
	})
}
