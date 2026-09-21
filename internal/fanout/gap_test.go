package fanout

import (
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"replay/internal/store"
)

// seqRecord returns a record whose SequenceNumber is exactly its own
// emit index, and nothing else meaningful. The forced-lap test writes a
// known sequence of these so a delivered record can be checked against
// the index it was delivered as, independent of the gap accounting under
// test.
func seqRecord(i uint64) store.Record {
	return store.Record{
		ExchangeTs:     int64(i),
		SequenceNumber: i,
		VenueID:        1,
		RecordType:     store.RecordTypeDelta,
	}
}

func TestRingNextKeepsUp(t *testing.T) {
	t.Run("a_reader_that_keeps_up_sees_every_record_in_order_with_no_gap", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 1024, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}

		const total = 5000
		cursor := uint64(0)
		for i := uint64(0); i < total; i++ {
			if _, err := r.Write(seqRecord(i), nil); err != nil {
				t.Fatalf("Write() error = %v, want nil", err)
			}

			var rec store.Record
			_, gap, hasGap, next, ready := r.next(cursor, &rec, nil)
			if !ready {
				t.Fatalf("next(%d) ready = false immediately after Write, want true", cursor)
			}
			if hasGap {
				t.Fatalf("next(%d) reported a gap %+v on a reader that never fell behind", cursor, gap)
			}
			if rec.SequenceNumber != i {
				t.Fatalf("next(%d) delivered SequenceNumber %d, want %d", cursor, rec.SequenceNumber, i)
			}
			if next != cursor+1 {
				t.Fatalf("next(%d) newCursor = %d, want %d", cursor, next, cursor+1)
			}
			cursor = next
		}
	})

	t.Run("next_reports_not_ready_when_the_writer_has_not_reached_the_cursor", func(t *testing.T) {
		r, err := NewRing(Config{Capacity: 4, MaxBlobBytes: 0})
		if err != nil {
			t.Fatalf("NewRing() error = %v, want nil", err)
		}
		if _, err := r.Write(seqRecord(0), nil); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}

		var rec store.Record
		_, _, hasGap, next, ready := r.next(1, &rec, nil)
		if ready {
			t.Fatalf("next(1) ready = true, want false: only index 0 has been written")
		}
		if hasGap {
			t.Errorf("next(1) hasGap = true, want false")
		}
		if next != 1 {
			t.Errorf("next(1) newCursor = %d, want 1 (unchanged)", next)
		}
	})
}

// lapResult is one record TestForcedLap's reader accepted, tagged with
// the emit index it was accepted as.
type lapResult struct {
	index uint64
	seq   uint64
}

func TestForcedLap(t *testing.T) {
	// Small capacities force heavy lapping; larger ones exercise the
	// same protocol under lighter contention.
	for _, capacity := range []int{2, 4, 8} {
		t.Run("capacity_"+strconv.Itoa(capacity), func(t *testing.T) {
			for attempt := 0; attempt < 20; attempt++ {
				const total = 20_000
				r, err := NewRing(Config{Capacity: capacity, MaxBlobBytes: 0})
				if err != nil {
					t.Fatalf("NewRing() error = %v, want nil", err)
				}

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := uint64(0); i < total; i++ {
						if _, err := r.Write(seqRecord(i), nil); err != nil {
							t.Errorf("Write() error = %v, want nil", err)
							return
						}
					}
				}()

				rng := rand.New(rand.NewPCG(uint64(attempt), uint64(capacity)))
				var results []lapResult
				var gaps []Gap
				cursor := uint64(0)
				for cursor < total {
					if rng.Uint64()&1 == 0 {
						runtime.Gosched()
					}
					var rec store.Record
					_, gap, hasGap, next, ready := r.next(cursor, &rec, nil)
					if !ready {
						continue
					}
					if hasGap {
						gaps = append(gaps, gap)
					}
					// The record just delivered is at index next-1: on
					// a gap that is the catch-up target, not the
					// original cursor.
					results = append(results, lapResult{index: next - 1, seq: rec.SequenceNumber})
					cursor = next
				}
				wg.Wait()

				// (a) every delivered record's SequenceNumber equals its
				// emit index: no torn value was ever accepted.
				for _, res := range results {
					if res.seq != res.index {
						t.Fatalf("delivered index %d carried SequenceNumber %d: a torn or wrong record was accepted", res.index, res.seq)
					}
				}

				// (b) delivered indexes plus the union of gap ranges
				// cover [0, total) exactly, with no overlap and no hole.
				covered := make([]bool, total)
				for _, res := range results {
					if covered[res.index] {
						t.Fatalf("index %d was delivered more than once", res.index)
					}
					covered[res.index] = true
				}
				var totalMissed uint64
				for _, g := range gaps {
					// (c) Count always matches the reported range.
					if want := g.LastMissedIndex - g.FirstMissedIndex + 1; g.Count != want {
						t.Fatalf("gap %+v has Count %d, want %d", g, g.Count, want)
					}
					for idx := g.FirstMissedIndex; idx <= g.LastMissedIndex; idx++ {
						if covered[idx] {
							t.Fatalf("index %d is both delivered and reported missed", idx)
						}
						covered[idx] = true
					}
					totalMissed += g.Count
				}
				for i := uint64(0); i < total; i++ {
					if !covered[i] {
						t.Fatalf("index %d is neither delivered nor reported missed", i)
					}
				}
				if uint64(len(results))+totalMissed != total {
					t.Fatalf("delivered %d + missed %d != total %d", len(results), totalMissed, total)
				}

				// (d) the run actually lapped. Otherwise this test
				// passes while exercising far less than it claims to,
				// exactly the failure mode internal/merge's determinism
				// suite guards against for its own dataset.
				if totalMissed == 0 {
					t.Fatalf("capacity %d never lapped the reader across %d records; this run proves nothing about the lapping protocol", capacity, total)
				}
			}
		})
	}
}

// pokeSlot hand-writes rec's wire form directly into r's slot i, at the
// given seq, bypassing Write. It is how the torn-slot tests build a slot
// state Write itself would never produce: seq published but the record's
// own fields internally inconsistent, exactly what a genuine tear would
// look like to a reader.
func pokeSlot(r *Ring, i int, seq uint64, rec store.Record) {
	var buf [store.RecordSize]byte
	store.EncodeRecord(buf[:], rec)
	sl := &r.slots[i]
	for w := 0; w < slotWords; w++ {
		sl.words[w].Store(binary.LittleEndian.Uint64(buf[w*8:]))
	}
	sl.seq.Store(seq)
}

func TestTornSlotNeverPanics(t *testing.T) {
	tests := []struct {
		name    string
		blobLen uint32
	}{
		{"a_blob_len_that_decodes_to_a_huge_length_never_panics_or_reads_out_of_bounds", 0xFFFFFFFF},
		{"a_blob_len_shorter_than_the_header_never_panics", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewRing(Config{Capacity: 2, MaxBlobBytes: 16})
			if err != nil {
				t.Fatalf("NewRing() error = %v, want nil", err)
			}

			// Publish (odd seq) a slot whose own fields are internally
			// inconsistent: a snapshot pointer with a blob_len no real
			// Write call would ever produce. This is what the load-copy-
			// reload protocol must survive without panicking, even
			// though nothing in this state came from a real tear.
			pokeSlot(r, 0, 1, store.Record{
				RecordType: store.RecordTypeSnapshotPointer,
				BlobLen:    tt.blobLen,
			})

			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("read() panicked: %v", p)
				}
			}()

			var rec store.Record
			_, _, complete := r.read(0, &rec, nil)
			if complete {
				t.Fatalf("read(0) complete = true on an internally inconsistent record, want false")
			}
		})
	}
}
