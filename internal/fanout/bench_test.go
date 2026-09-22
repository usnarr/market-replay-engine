package fanout

import (
	"testing"

	"replay/internal/allocgate"
	"replay/internal/store"
)

// Sinks stop the compiler from proving a benchmarked result is unused.
var (
	sinkUint64 uint64
	sinkRecord store.Record
	sinkBool   bool
)

// benchNilHasher is RunDigest's nil-hasher case. It is a package-level
// variable, not a local nil literal, so the compiler cannot prove the
// branch away and the benchmark measures the check the loop really does.
var benchNilHasher *store.CanonicalHasher

// benchDelta is the common case on the hot path: no blob.
var benchDelta = store.Record{
	ExchangeTs:     1_700_000_000_000_000_000,
	SequenceNumber: 987_654_321,
	InstrumentID:   4242,
	VenueID:        7,
	RecordType:     store.RecordTypeDelta,
	SideFlags:      store.SideAsk,
	Price:          12_345_678_900,
	Size:           250_000,
}

func BenchmarkRingWrite(b *testing.B) {
	b.Run("no_blob", func(b *testing.B) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			b.Fatalf("NewRing() error = %v, want nil", err)
		}
		write := func() {
			n, err := r.Write(benchDelta, nil)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
		}
		allocgate.AssertZero(b, write)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			write()
		}
	})

	b.Run("with_a_snapshot_blob", func(b *testing.B) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 64})
		if err != nil {
			b.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := make([]byte, 52) // header + one level, see docs/format.md
		rec := store.Record{
			ExchangeTs: 1, RecordType: store.RecordTypeSnapshotPointer,
			BlobLen: uint32(len(payload)) + 4,
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			n, err := r.Write(rec, payload)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
		}
	})
}

// BenchmarkRunDigest measures RunDigest's per-record step: the one
// h.Write and the one r.Write the loop runs for each record. It is that
// step, not a whole-dataset drain, because allocgate.AssertZero calls
// its closure hundreds of times and a drain would rebuild the merge on
// each of them, measuring setup instead of the hot path.
func BenchmarkRunDigest(b *testing.B) {
	b.Run("delta_only", func(b *testing.B) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			b.Fatalf("NewRing() error = %v, want nil", err)
		}
		h := store.NewCanonicalHasher(store.CanonicalCRC32C)
		step := func() {
			h.Write(benchDelta, nil)
			n, err := r.Write(benchDelta, nil)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
		}
		allocgate.AssertZero(b, step)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			step()
		}
	})

	b.Run("with_a_snapshot_blob", func(b *testing.B) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 64})
		if err != nil {
			b.Fatalf("NewRing() error = %v, want nil", err)
		}
		payload := make([]byte, 52) // header + one level, see docs/format.md
		rec := store.Record{
			ExchangeTs: 1, RecordType: store.RecordTypeSnapshotPointer,
			BlobLen: uint32(len(payload)) + 4,
		}
		h := store.NewCanonicalHasher(store.CanonicalCRC32C)
		step := func() {
			h.Write(rec, payload)
			n, err := r.Write(rec, payload)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
		}
		allocgate.AssertZero(b, step)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			step()
		}
	})

	// nil_hasher is what Run and RunPaced now take: the digest branch,
	// not taken. It is here to be compared against BenchmarkRingWrite's
	// own no_blob row, which is the same write with no branch at all.
	b.Run("nil_hasher", func(b *testing.B) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			b.Fatalf("NewRing() error = %v, want nil", err)
		}
		step := func() {
			if benchNilHasher != nil {
				benchNilHasher.Write(benchDelta, nil)
			}
			n, err := r.Write(benchDelta, nil)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
		}
		allocgate.AssertZero(b, step)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			step()
		}
	})
}

func BenchmarkRingRead(b *testing.B) {
	r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
	if err != nil {
		b.Fatalf("NewRing() error = %v, want nil", err)
	}
	if _, err := r.Write(benchDelta, nil); err != nil {
		b.Fatalf("Write() error = %v, want nil", err)
	}
	read := func() {
		_, _, ok := r.read(0, &sinkRecord, nil)
		sinkBool = ok
	}
	allocgate.AssertZero(b, read)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read()
	}
}
