package store

import (
	"encoding/hex"
	"testing"
)

// canonicalRun is the fixed record stream the golden digests below
// describe. Changing it changes those digests, which is the point: a
// baseline that moves without a deliberate format change is a
// regression.
func canonicalRun() []struct {
	rec     Record
	payload []byte
} {
	return []struct {
		rec     Record
		payload []byte
	}{
		{rec: Record{ExchangeTs: 1000, SequenceNumber: 1, InstrumentID: 10, VenueID: 7, RecordType: RecordTypeDelta, SideFlags: SideBid, Price: 500, Size: 3}},
		{rec: Record{ExchangeTs: 1000, SequenceNumber: 2, InstrumentID: 11, VenueID: 7, RecordType: RecordTypeTrade, SideFlags: SideAsk, Price: 501, Size: 1}},
		{
			rec:     Record{ExchangeTs: 1001, SequenceNumber: 3, InstrumentID: 10, VenueID: 7, RecordType: RecordTypeSnapshotPointer, BlobOffset: 4096, BlobLen: blobHeaderSize + levelSize, LevelCount: 1},
			payload: []byte{1, 0, 0, 0, 0x64, 0, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0, 0, 0, 0, 0},
		},
	}
}

func hashRun(algo CanonicalAlgo) string {
	h := NewCanonicalHasher(algo)
	for _, e := range canonicalRun() {
		h.Write(e.rec, e.payload)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestCanonicalGoldenDigest(t *testing.T) {
	// Committed baselines for canonical_v1 over canonicalRun. Never
	// regenerate them from the code under test to make a failure go away:
	// a digest that moves means either a deliberate format change, which
	// needs canonical_v2, or a bug.
	tests := []struct {
		name string
		algo CanonicalAlgo
		want string
	}{
		{name: "crc32c", algo: CanonicalCRC32C, want: "68709540"},
		{name: "sha256", algo: CanonicalSHA256, want: "573396cae4871ee4eb4aa366a12c5c93edf0a036aeea5fe67267818dc13b0c72"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hashRun(tt.algo)

			if got != tt.want {
				t.Errorf("canonical_v1 %s digest = %s, want %s", tt.name, got, tt.want)
			}
		})
	}
}

func TestCanonicalFieldSensitivity(t *testing.T) {
	base := hashRun(CanonicalCRC32C)

	mutations := []struct {
		name string
		mut  func(e *Record)
	}{
		{name: "exchange_ts", mut: func(e *Record) { e.ExchangeTs++ }},
		{name: "sequence_number", mut: func(e *Record) { e.SequenceNumber++ }},
		{name: "instrument_id", mut: func(e *Record) { e.InstrumentID++ }},
		{name: "venue_id", mut: func(e *Record) { e.VenueID++ }},
		{name: "record_type", mut: func(e *Record) { e.RecordType = RecordTypeTrade }},
		{name: "side_flags", mut: func(e *Record) { e.SideFlags ^= SideAsk }},
		{name: "price", mut: func(e *Record) { e.Price++ }},
		{name: "size", mut: func(e *Record) { e.Size++ }},
	}

	for _, m := range mutations {
		t.Run(m.name+"_changes_the_digest", func(t *testing.T) {
			run := canonicalRun()
			m.mut(&run[0].rec)
			h := NewCanonicalHasher(CanonicalCRC32C)
			for _, e := range run {
				h.Write(e.rec, e.payload)
			}

			got := hex.EncodeToString(h.Sum(nil))

			if got == base {
				t.Errorf("changing %s left the digest at %s", m.name, got)
			}
		})
	}

	t.Run("blob_offset_and_blob_len_are_excluded", func(t *testing.T) {
		// They describe where a snapshot sits in one file, not what it
		// holds. Two conversions of the same session may pack blobs
		// differently and must still match.
		run := canonicalRun()
		run[2].rec.BlobOffset = 999999
		h := NewCanonicalHasher(CanonicalCRC32C)
		for _, e := range run {
			h.Write(e.rec, e.payload)
		}

		got := hex.EncodeToString(h.Sum(nil))

		if got != base {
			t.Errorf("digest = %s after moving blob_offset, want %s", got, base)
		}
	})

	t.Run("the_blob_payload_is_included", func(t *testing.T) {
		run := canonicalRun()
		payload := make([]byte, len(run[2].payload))
		copy(payload, run[2].payload)
		payload[4] ^= 0x01
		run[2].payload = payload
		h := NewCanonicalHasher(CanonicalCRC32C)
		for _, e := range run {
			h.Write(e.rec, e.payload)
		}

		got := hex.EncodeToString(h.Sum(nil))

		if got == base {
			t.Errorf("changing a blob level left the digest at %s", got)
		}
	})

	t.Run("a_payload_on_a_non_snapshot_record_is_ignored", func(t *testing.T) {
		run := canonicalRun()
		run[0].payload = []byte{9, 9, 9, 9}
		h := NewCanonicalHasher(CanonicalCRC32C)
		for _, e := range run {
			h.Write(e.rec, e.payload)
		}

		got := hex.EncodeToString(h.Sum(nil))

		if got != base {
			t.Errorf("digest = %s with a payload on a delta, want %s", got, base)
		}
	})
}

func TestCanonicalFraming(t *testing.T) {
	// Without the length prefix and the record count, these two runs are
	// byte-for-byte identical: run B's snapshot payload is exactly run
	// A's second record's projection, so the concatenated projections
	// match and only the record boundaries differ.
	delta := Record{ExchangeTs: 2000, SequenceNumber: 9, InstrumentID: 3, VenueID: 7, RecordType: RecordTypeDelta, SideFlags: SideBid, Price: 42, Size: 7}
	var deltaCanon [canonicalPrefixMax]byte
	n := canonicalPrefix(deltaCanon[:], delta)

	emptySnapshot := Record{ExchangeTs: 1999, SequenceNumber: 8, InstrumentID: 3, VenueID: 7, RecordType: RecordTypeSnapshotPointer, BlobLen: blobHeaderSize, LevelCount: 0}
	fatSnapshot := emptySnapshot

	hA := NewCanonicalHasher(CanonicalCRC32C)
	hA.Write(emptySnapshot, nil)
	hA.Write(delta, nil)
	a := hex.EncodeToString(hA.Sum(nil))

	hB := NewCanonicalHasher(CanonicalCRC32C)
	hB.Write(fatSnapshot, deltaCanon[:n])
	b := hex.EncodeToString(hB.Sum(nil))

	if a == b {
		t.Errorf("two runs with different record boundaries both hash to %s — the framing is not load-bearing", a)
	}
}

func TestCanonicalOrderSensitivity(t *testing.T) {
	run := canonicalRun()

	forward := NewCanonicalHasher(CanonicalCRC32C)
	for _, e := range run {
		forward.Write(e.rec, e.payload)
	}
	swapped := NewCanonicalHasher(CanonicalCRC32C)
	swapped.Write(run[1].rec, run[1].payload)
	swapped.Write(run[0].rec, run[0].payload)
	swapped.Write(run[2].rec, run[2].payload)

	if hex.EncodeToString(forward.Sum(nil)) == hex.EncodeToString(swapped.Sum(nil)) {
		t.Error("swapping two records left the digest unchanged")
	}
}

func TestCanonicalSumIsIdempotent(t *testing.T) {
	h := NewCanonicalHasher(CanonicalCRC32C)
	for _, e := range canonicalRun() {
		h.Write(e.rec, e.payload)
	}

	first := hex.EncodeToString(h.Sum(nil))
	second := hex.EncodeToString(h.Sum(nil))

	if first != second {
		t.Errorf("Sum() = %s then %s, want the same digest", first, second)
	}
}

func TestCanonicalWriteAfterSumPanics(t *testing.T) {
	h := NewCanonicalHasher(CanonicalCRC32C)
	h.Write(canonicalRun()[0].rec, nil)
	h.Sum(nil)

	defer func() {
		if recover() == nil {
			t.Error("Write after Sum did not panic")
		}
	}()

	h.Write(canonicalRun()[0].rec, nil)
}
