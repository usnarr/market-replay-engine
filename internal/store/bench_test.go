package store

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"replay/internal/allocgate"
)

// Sinks stop the compiler from proving a benchmarked result is unused,
// and stop a Record return value escaping into a formatting call, which
// would allocate and hide the number the allocation gate cares about.
var (
	sinkRecord Record
	sinkInt    int
	sinkBytes  []byte
	sinkUint32 uint32
)

// benchRecord is a delta: the common case on the hot path.
var benchRecord = Record{
	ExchangeTs:     1_700_000_000_000_000_000,
	SequenceNumber: 987_654_321,
	InstrumentID:   4242,
	VenueID:        7,
	RecordType:     RecordTypeDelta,
	SideFlags:      SideAsk,
	Price:          12_345_678_900,
	Size:           250_000,
}

func BenchmarkEncodeRecord(b *testing.B) {
	var buf [RecordSize]byte
	allocgate.AssertZero(b, func() { EncodeRecord(buf[:], benchRecord) })

	b.SetBytes(RecordSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		EncodeRecord(buf[:], benchRecord)
	}
}

// BenchmarkDecodeRecordFields is the reader's hot path: no validation,
// no error return.
func BenchmarkDecodeRecordFields(b *testing.B) {
	var buf [RecordSize]byte
	EncodeRecord(buf[:], benchRecord)
	allocgate.AssertZero(b, func() { sinkRecord = DecodeRecordFields(buf[:]) })

	b.SetBytes(RecordSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sinkRecord = DecodeRecordFields(buf[:])
	}
}

// BenchmarkDecodeRecord is the checked decode, used on input that has
// not been through a block checksum.
func BenchmarkDecodeRecord(b *testing.B) {
	var buf [RecordSize]byte
	EncodeRecord(buf[:], benchRecord)
	b.SetBytes(RecordSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sinkRecord, _ = decodeRecord(buf[:])
	}
}

// benchReader writes a file of n records and opens it. Snapshots land
// every 1000 records, roughly the epoch cadence a converter produces.
func benchReader(b *testing.B, n int, blockSize uint32) *Reader {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.rpl")
	opts := defaultWriterOptions()
	opts.blockSizeRecords = blockSize
	w, err := newWriter(path, testVenue, testPriceScale, opts)
	if err != nil {
		b.Fatalf("newWriter() error = %v", err)
	}

	bids := make([]Level, 25)
	asks := make([]Level, 25)
	for i := range bids {
		bids[i] = Level{Price: int64(10_000 - i), Size: int64(i + 1)}
		asks[i] = Level{Price: int64(10_001 + i), Size: int64(i + 1)}
	}
	for i := 0; i < n; i++ {
		rec := Record{
			ExchangeTs:     int64(1_700_000_000_000_000_000 + i/2*1000),
			SequenceNumber: uint64(i),
			InstrumentID:   uint32(i%64 + 1),
			VenueID:        testVenue,
		}
		if i%1000 == 0 {
			err = w.WriteSnapshot(rec, bids, asks)
		} else {
			rec.RecordType = RecordTypeDelta
			rec.SideFlags = uint8(i % 2)
			rec.Price = int64(10_000 + i%100)
			rec.Size = int64(i%50 + 1)
			err = w.WriteRecord(rec)
		}
		if err != nil {
			b.Fatalf("writing record %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}

	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() { _ = r.Close() })
	return r
}

func BenchmarkRecordAt(b *testing.B) {
	const n = 1 << 17
	r := benchReader(b, n, defaultBlockSizeRecords)

	b.Run("sequential", func(b *testing.B) {
		b.SetBytes(RecordSize)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sinkRecord = r.RecordAt(i % n)
		}
	})

	b.Run("random", func(b *testing.B) {
		// The number that predicts replay throughput once the file is
		// larger than the page cache.
		rng := rand.New(rand.NewPCG(1, 2))
		order := make([]int, n)
		for i := range order {
			order[i] = int(rng.UintN(n))
		}
		b.SetBytes(RecordSize)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sinkRecord = r.RecordAt(order[i%n])
		}
	})
}

func BenchmarkSeekTime(b *testing.B) {
	const n = 1 << 17
	// Seek cost is linear in the block size: the index narrows the answer
	// to one block and the rest is a scan. This is the measurement the
	// block size is chosen against.
	for _, blockSize := range []uint32{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("block_size_%d", blockSize), func(b *testing.B) {
			r := benchReader(b, n, blockSize)
			targets := make([]int64, 1024)
			rng := rand.New(rand.NewPCG(3, 4))
			lo, hi := r.RecordAt(0).ExchangeTs, r.RecordAt(n-1).ExchangeTs
			for i := range targets {
				targets[i] = lo + int64(rng.Uint64N(uint64(hi-lo+1)))
			}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				sinkInt, _ = r.SeekTime(targets[i%len(targets)])
			}
		})
	}
}

// BenchmarkVerifyBlock measures what the block size costs. A block is
// the unit a reader pays for on first touch, so the number that matters
// is throughput per byte checksummed, against the metadata a block
// costs: one 12-byte footer entry plus one 8-byte time index entry.
func BenchmarkVerifyBlock(b *testing.B) {
	const n = 1 << 17
	for _, blockSize := range []uint32{64, 256, 1024, 4096, 16384} {
		b.Run(fmt.Sprintf("block_size_%d", blockSize), func(b *testing.B) {
			r := benchReader(b, n, blockSize)
			blocks := r.BlockCount()
			b.SetBytes(int64(blockSize) * RecordSize)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := r.VerifyBlock(i % blocks); err != nil {
					b.Fatalf("VerifyBlock() error = %v", err)
				}
			}
			b.StopTimer()
			// Footer entry plus time index entry, as a share of the file.
			b.ReportMetric(float64(20*blocks)/float64(n*RecordSize)*100, "%meta")
		})
	}
}

func BenchmarkBlob(b *testing.B) {
	r := benchReader(b, 1<<13, defaultBlockSizeRecords)
	rec := r.RecordAt(0)
	if rec.RecordType != RecordTypeSnapshotPointer {
		b.Fatalf("record 0 is type %d, want a snapshot pointer", rec.RecordType)
	}

	b.Run("payload", func(b *testing.B) {
		b.SetBytes(int64(rec.BlobLen))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sinkBytes, _ = r.Blob(rec)
		}
	})

	b.Run("append_levels", func(b *testing.B) {
		dst := make([]Level, 0, 64)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			dst, sinkInt, _ = r.AppendLevels(dst[:0], rec)
		}
		sinkInt = len(dst)
	})
}

func BenchmarkCanonicalV1(b *testing.B) {
	payload := make([]byte, blobHeaderSize-4+50*levelSize)
	snapshot := Record{
		ExchangeTs: 1_700_000_000_000_000_000,
		VenueID:    7,
		RecordType: RecordTypeSnapshotPointer,
		BlobLen:    blobHeaderSize + 50*levelSize,
		LevelCount: 50,
	}

	b.Run("delta", func(b *testing.B) {
		h := NewCanonicalHasher(CanonicalCRC32C)
		allocgate.AssertZero(b, func() { h.Write(benchRecord, nil) })

		b.SetBytes(40)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			h.Write(benchRecord, nil)
		}
		b.StopTimer()
		sinkUint32 = uint32(len(h.Sum(nil)))
	})

	b.Run("snapshot", func(b *testing.B) {
		h := NewCanonicalHasher(CanonicalCRC32C)
		b.SetBytes(int64(canonicalPrefixMax + len(payload)))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			h.Write(snapshot, payload)
		}
		b.StopTimer()
		sinkUint32 = uint32(len(h.Sum(nil)))
	})

	b.Run("delta_sha256", func(b *testing.B) {
		h := NewCanonicalHasher(CanonicalSHA256)
		b.SetBytes(40)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			h.Write(benchRecord, nil)
		}
		b.StopTimer()
		sinkUint32 = uint32(len(h.Sum(nil)))
	})
}
