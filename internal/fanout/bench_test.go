package fanout

import (
	"testing"

	"replay/internal/store"
)

// Sinks stop the compiler from proving a benchmarked result is unused.
var (
	sinkUint64 uint64
	sinkRecord store.Record
	sinkBool   bool
)

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
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			n, err := r.Write(benchDelta, nil)
			if err != nil {
				b.Fatalf("Write() error = %v, want nil", err)
			}
			sinkUint64 = n
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

func BenchmarkRingRead(b *testing.B) {
	r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
	if err != nil {
		b.Fatalf("NewRing() error = %v, want nil", err)
	}
	if _, err := r.Write(benchDelta, nil); err != nil {
		b.Fatalf("Write() error = %v, want nil", err)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, ok := r.read(0, &sinkRecord, nil)
		sinkBool = ok
	}
}
