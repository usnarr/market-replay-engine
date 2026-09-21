package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"replay/internal/store"
)

// hdrOffHeaderSize is where the header carries the offset of record 0.
// See docs/format.md's header table.
const hdrOffHeaderSize = 48

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v, want nil", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// reproducibleRows spans two venues, two days, two instruments and an
// epoch, so the bytes under test cover the record array, the blob
// region, both indexes and the footer.
func reproducibleRows(base int64) []SourceRow {
	rows := []SourceRow{
		srcSnapshot(base+1, 9, 1, 20, []SourceLevel{
			{Side: uint32(store.SideBid), Price: 500, Size: 1},
			{Side: uint32(store.SideAsk), Price: 501, Size: 2},
		}),
	}
	rows = append(rows, epochRows(base)...)
	rows = append(rows,
		srcDelta(base+nanosPerDay+1, 7, 11, 10, store.SideBid, 97, 14),
		srcDelta(base+nanosPerDay+2, 7, 12, 11, store.SideAsk, 202, 15),
		srcDelta(base+nanosPerDay+3, 9, 2, 20, store.SideBid, 499, 3),
	)
	slices.SortStableFunc(rows, func(a, b SourceRow) int {
		switch {
		case a.ExchangeTs < b.ExchangeTs:
			return -1
		case a.ExchangeTs > b.ExchangeTs:
			return 1
		}
		return 0
	})
	return rows
}

func TestConvertIsByteReproducible(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", reproducibleRows(base))
	opts := Options{PriceScale: testPriceScale, EpochEvery: 4}

	first, err := Convert(src, t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	second, err := Convert(src, t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}

	if len(first) != 4 {
		t.Fatalf("Convert() wrote %d files, want 4", len(first))
	}
	if len(first) != len(second) {
		t.Fatalf("two conversions wrote %d and %d files, want the same count", len(first), len(second))
	}
	for i := range first {
		if filepath.Base(first[i]) != filepath.Base(second[i]) {
			t.Fatalf("two conversions wrote %q and %q at position %d, want the same name",
				filepath.Base(first[i]), filepath.Base(second[i]), i)
		}
		gotFirst, gotSecond := fileDigest(t, first[i]), fileDigest(t, second[i])
		if gotFirst != gotSecond {
			t.Errorf("%s: two conversions hash to %s and %s, want one digest", filepath.Base(first[i]), gotFirst, gotSecond)
		}
	}
}

func TestConvertPinsTheArtifactLayout(t *testing.T) {
	base := int64(day2024) * nanosPerDay
	src := writeSourceParquet(t, t.TempDir(), "source.parquet", reproducibleRows(base))

	paths, err := Convert(src, t.TempDir(), Options{PriceScale: testPriceScale, EpochEvery: 4})

	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v, want nil", path, err)
		}
		// A host whose page size is not artifactPageSize -- Apple silicon
		// uses 16384 -- must still produce this value, or the same input
		// would convert to different bytes on different machines.
		if got := binary.LittleEndian.Uint32(b[hdrOffHeaderSize:]); got != artifactPageSize {
			t.Errorf("%s: header_size = %d, want %d", filepath.Base(path), got, artifactPageSize)
		}
		r := openArtifact(t, path)
		if got := r.BlockSizeRecords(); got != artifactBlockSizeRecords {
			t.Errorf("%s: block_size_records = %d, want %d", filepath.Base(path), got, artifactBlockSizeRecords)
		}
	}
}
