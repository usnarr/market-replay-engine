package merge

import (
	"encoding/binary"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

// replayPartitionsConcurrent is replayPartitions with a worker pool
// decoding instead of the calling goroutine.
func replayPartitionsConcurrent(t *testing.T, partitions []venueFiles, workers int) ([]Key, error) {
	t.Helper()

	m, err := NewConcurrentMerger(cursorsFor(t, partitions), workers)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = m.Close() })
	return drainMerger(t, m)
}

func TestConcurrentMergerMatchesInlineDecoding(t *testing.T) {
	// TestDeterminism compares the two on a large generated dataset.
	// These are the awkward small shapes: partitions that produce no
	// batches at all, a partition longer than one batch, and a partial
	// last batch.
	tests := []struct {
		name       string
		partitions []venueFiles
	}{
		{
			name: "a_venue_with_no_files_at_all",
			partitions: []venueFiles{
				{venue: 1, days: nil},
				{venue: 2, days: [][]store.Record{records(2, 0, 1, 2, 3)}},
			},
		},
		{
			name: "a_venue_whose_only_file_is_empty",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{{}}},
				{venue: 2, days: [][]store.Record{records(2, 0, 1, 2, 3)}},
			},
		},
		{
			name:       "every_venue_empty",
			partitions: []venueFiles{{venue: 1, days: nil}, {venue: 2, days: [][]store.Record{{}}}},
		},
		{
			name: "a_venue_exactly_one_batch_long",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{deltaRecords(1, 0, 0, batchRecords)}},
				{venue: 2, days: [][]store.Record{records(2, 0, 5, 6)}},
			},
		},
		{
			name: "a_venue_one_record_past_a_batch_boundary",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{deltaRecords(1, 0, 0, batchRecords+1)}},
				{venue: 2, days: [][]store.Record{records(2, 0, 5, 6)}},
			},
		},
		{
			name: "several_batches_split_across_files",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{
					deltaRecords(1, 0, 0, 1500),
					deltaRecords(1, 5000, 1500, 1500),
				}},
				{venue: 2, days: [][]store.Record{deltaRecords(2, 100, 0, 2500)}},
				{venue: 3, days: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, err := replayPartitions(t, tt.partitions)
			if err != nil {
				t.Fatalf("inline replay error = %v, want nil", err)
			}

			for _, workers := range workerCounts {
				t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
					got, err := replayPartitionsConcurrent(t, tt.partitions, workers)

					if err != nil {
						t.Fatalf("concurrent replay error = %v, want nil", err)
					}
					if diff := cmp.Diff(want, got, cmpEmptyKeySlices); diff != "" {
						t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestBatchFill(t *testing.T) {
	// A worker decodes one range of one file and checks everything it
	// can see on its own. The merge's own ordering check would catch
	// most of this later, so test the worker's contract here where it is
	// the only thing being exercised.
	recs := deltaRecords(3, 100, 0, 2500)
	r := openVenueFile(t, writeVenueFile(t, "venue.bin", 3, recs))

	t.Run("decodes_exactly_the_range_it_is_given", func(t *testing.T) {
		b := newBatch(3)

		b.fill(r, 1000, 500)

		if b.err != nil {
			t.Fatalf("fill() err = %v, want nil", b.err)
		}
		got := make([]store.Record, len(b.events))
		for i, ev := range b.events {
			got[i] = ev.Record
		}
		if diff := cmp.Diff(recs[1000:1500], got); diff != "" {
			t.Errorf("records mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a_refilled_batch_keeps_nothing_from_the_last_range", func(t *testing.T) {
		b := newBatch(3)
		b.fill(r, 0, 500)

		b.fill(r, 2000, 10)

		if b.err != nil {
			t.Fatalf("fill() err = %v, want nil", b.err)
		}
		if len(b.events) != 10 {
			t.Fatalf("fill() left %d events, want 10", len(b.events))
		}
		if got := b.events[0].Record; got != recs[2000] {
			t.Errorf("first record = %+v, want %+v", got, recs[2000])
		}
	})

	t.Run("a_range_from_another_venue_is_rejected", func(t *testing.T) {
		b := newBatch(99)

		b.fill(r, 0, 4)

		if !errors.Is(b.err, ErrVenueMismatch) {
			t.Fatalf("fill() err = %v, want %v", b.err, ErrVenueMismatch)
		}
	})

	t.Run("a_corrupt_block_is_rejected", func(t *testing.T) {
		const markerPrice = 0x1234_5678_9ABC_DEF0

		bad := deltaRecords(3, 0, 0, 50)
		bad[5].Price = markerPrice
		path := writeVenueFile(t, "corrupt.bin", 3, bad)
		var marker [8]byte
		binary.LittleEndian.PutUint64(marker[:], markerPrice)
		corruptFileAt(t, path, marker[:])
		b := newBatch(3)

		b.fill(openVenueFile(t, path), 0, 50)

		if !errors.Is(b.err, store.ErrBlockChecksum) {
			t.Fatalf("fill() err = %v, want %v", b.err, store.ErrBlockChecksum)
		}
	})

	t.Run("a_snapshot_pointer_carries_its_blob_and_a_delta_does_not", func(t *testing.T) {
		snap := openVenueFile(t, writeSnapshotFile(t, 3, 500, 7))
		b := newBatch(3)

		b.fill(snap, 0, snap.Len())

		if b.err != nil {
			t.Fatalf("fill() err = %v, want nil", b.err)
		}
		for i, ev := range b.events {
			isSnapshot := ev.Record.RecordType == store.RecordTypeSnapshotPointer
			if isSnapshot == (ev.Blob == nil) {
				t.Errorf("event %d: record type %d with blob length %d",
					i, ev.Record.RecordType, len(ev.Blob))
			}
		}
	})
}

// rangeCounts is the batch sizes a file of n records splits into when
// one batch covers size records.
func rangeCounts(n, size int) []int {
	var counts []int
	for n > 0 {
		c := min(n, size)
		counts = append(counts, c)
		n -= c
	}
	return counts
}

func TestVenueFeedFollowsTheBatchShape(t *testing.T) {
	// Batch size and feed depth are two of the axes the determinism suite
	// varies. A shape that is accepted but not applied would make those
	// subtests silently replay the default shape every time.
	recs := deltaRecords(3, 100, 0, 2500)
	path := writeVenueFile(t, "venue.bin", 3, recs)

	tests := []struct {
		name  string
		shape batchShape
	}{
		{name: "the_default_shape", shape: defaultBatchShape()},
		{name: "one_record_per_batch", shape: batchShape{records: 1, batches: 2}},
		{name: "one_batch_larger_than_the_whole_file", shape: batchShape{records: len(recs) + 1, batches: 2}},
		{name: "a_size_that_does_not_divide_the_file", shape: batchShape{records: 333, batches: 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var abort atomic.Bool
			f := newVenueFeed(newCursorOver(t, 3, path), &abort, tt.shape)

			var got []int
			for {
				_, _, count, ok := f.nextRange()
				if !ok {
					break
				}
				got = append(got, count)
			}

			if diff := cmp.Diff(rangeCounts(len(recs), tt.shape.records), got); diff != "" {
				t.Errorf("nextRange() counts mismatch (-want +got):\n%s", diff)
			}
			if n := cap(f.free); n != tt.shape.batches {
				t.Errorf("cap(free) = %d, want %d", n, tt.shape.batches)
			}
			if n := len(f.free); n != tt.shape.batches {
				t.Errorf("len(free) = %d, want %d", n, tt.shape.batches)
			}
			if n := cap((<-f.free).events); n != tt.shape.records {
				t.Errorf("cap(batch.events) = %d, want %d", n, tt.shape.records)
			}
		})
	}
}

func TestNewMergerShapeRejectsAnUnusableShape(t *testing.T) {
	// A venue issues work only while it holds fewer than batches-1
	// batches, so a depth below two lets it issue none at all and the
	// replay would report an empty stream instead of failing.
	tests := []struct {
		name  string
		shape batchShape
	}{
		{name: "no_records_in_a_batch", shape: batchShape{records: 0, batches: 3}},
		{name: "a_negative_batch_size", shape: batchShape{records: -1, batches: 3}},
		{name: "one_batch_per_venue", shape: batchShape{records: 8, batches: 1}},
		{name: "no_batches_at_all", shape: batchShape{records: 8, batches: 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursors := cursorsFor(t, []venueFiles{{venue: 1, days: [][]store.Record{records(1, 0, 1)}}})

			_, err := newMergerShape(cursors, 4, tt.shape)

			if !errors.Is(err, ErrBatchShape) {
				t.Fatalf("newMergerShape(%+v) error = %v, want %v", tt.shape, err, ErrBatchShape)
			}
		})
	}
}

func TestCheckOrder(t *testing.T) {
	// Every layer that validates the ordering key calls this: the
	// cursor per record, a worker inside its batch, a venue at the seam
	// between two batches, and the loser tree across the whole stream.
	tests := []struct {
		name string
		prev Key
		next Key
		want error
	}{
		{name: "a_later_timestamp", prev: mk(10, 1, 0, 7), next: mk(11, 1, 0, 7)},
		{name: "the_same_timestamp_and_a_later_sequence", prev: mk(10, 1, 0, 7), next: mk(10, 1, 1, 7)},
		{name: "the_same_key_twice", prev: mk(10, 1, 0, 7), next: mk(10, 1, 0, 7), want: ErrDuplicateKey},
		{name: "an_earlier_timestamp", prev: mk(10, 1, 0, 7), next: mk(9, 1, 9, 9), want: ErrOutOfOrder},
		{name: "an_earlier_sequence", prev: mk(10, 1, 5, 7), next: mk(10, 1, 4, 7), want: ErrOutOfOrder},
		{name: "an_earlier_instrument", prev: mk(10, 1, 5, 7), next: mk(10, 1, 5, 6), want: ErrOutOfOrder},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkOrder(tt.prev, tt.next); !errors.Is(err, tt.want) {
				t.Errorf("checkOrder(%v, %v) = %v, want %v", tt.prev, tt.next, err, tt.want)
			}
		})
	}
}

func TestVenueFeedCheckSeam(t *testing.T) {
	// Only the venue's own goroutine ever sees two consecutive batches,
	// so the join between them is checked there. A file boundary always
	// lands on one.
	batchOf := func(recs []store.Record) *batch {
		b := newBatch(3)
		for _, rec := range recs {
			b.events = append(b.events, Event{Record: rec})
		}
		return b
	}

	tests := []struct {
		name   string
		first  []store.Record
		second []store.Record
		want   error
	}{
		{
			name:   "keys_keep_increasing",
			first:  deltaRecords(3, 100, 0, 4),
			second: deltaRecords(3, 200, 4, 4),
		},
		{
			name:   "the_last_key_repeats",
			first:  deltaRecords(3, 100, 0, 4),
			second: deltaRecords(3, 103, 3, 2),
			want:   ErrDuplicateKey,
		},
		{
			name:   "the_key_goes_backwards",
			first:  deltaRecords(3, 200, 10, 4),
			second: deltaRecords(3, 100, 0, 4),
			want:   ErrOutOfOrder,
		},
		{
			name:   "an_empty_batch_between_two_full_ones_changes_nothing",
			first:  deltaRecords(3, 100, 0, 4),
			second: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &venueFeed{venueID: 3}
			if err := f.checkSeam(batchOf(tt.first)); err != nil {
				t.Fatalf("checkSeam() on the first batch = %v, want nil", err)
			}

			err := f.checkSeam(batchOf(tt.second))

			if !errors.Is(err, tt.want) {
				t.Fatalf("checkSeam() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestConcurrentMergerReportsBadInput(t *testing.T) {
	last := records(3, 5, 100)[0]

	tests := []struct {
		name       string
		partitions []venueFiles
		want       error
	}{
		{
			name: "a_key_that_repeats_across_a_file_seam",
			partitions: []venueFiles{
				{venue: 3, days: [][]store.Record{{last}, {last}}},
				{venue: 4, days: [][]store.Record{records(4, 0, 1, 2, 3)}},
			},
			want: ErrDuplicateKey,
		},
		{
			name: "a_key_that_goes_backwards_across_a_file_seam",
			partitions: []venueFiles{
				{venue: 3, days: [][]store.Record{deltaRecords(3, 500, 500, 4), deltaRecords(3, 100, 0, 4)}},
				{venue: 4, days: [][]store.Record{records(4, 0, 1, 2, 3)}},
			},
			want: ErrOutOfOrder,
		},
		{
			name: "a_key_that_goes_backwards_inside_one_batch",
			partitions: []venueFiles{
				// Two files whose records interleave in time, so neither
				// file is out of order on its own but the seam is, and
				// the pair is short enough to land in one batch each.
				{venue: 3, days: [][]store.Record{deltaRecords(3, 200, 10, 2), deltaRecords(3, 100, 0, 2)}},
			},
			want: ErrOutOfOrder,
		},
	}

	for _, tt := range tests {
		for _, workers := range workerCounts {
			t.Run(tt.name+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
				_, err := replayPartitionsConcurrent(t, tt.partitions, workers)

				if !errors.Is(err, tt.want) {
					t.Fatalf("concurrent replay error = %v, want %v", err, tt.want)
				}
			})
		}
	}
}

func TestMergerRepeatsAnErrorForever(t *testing.T) {
	// A run that rejected its input has no valid remainder. Carrying on
	// after the error would hand back events that belong to a run being
	// discarded, so every later call reports the same failure.
	last := records(3, 5, 100)[0]
	partitions := []venueFiles{
		{venue: 3, days: [][]store.Record{{last}, {last}}},
		{venue: 4, days: [][]store.Record{deltaRecords(4, 1, 0, 2000)}},
	}

	m, err := NewConcurrentMerger(cursorsFor(t, partitions), 4)
	if err != nil {
		t.Fatalf("NewConcurrentMerger() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if _, err := drainMerger(t, m); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Next() error = %v, want %v", err, ErrDuplicateKey)
	}

	for i := 0; i < 3; i++ {
		ev, ok, err := m.Next()

		if !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("Next() call %d after the error = %v, want %v", i+1, err, ErrDuplicateKey)
		}
		if ok {
			t.Errorf("Next() call %d after the error returned %+v, want no event", i+1, ev.Record)
		}
	}
}

func TestConcurrentMergerCloseWhileWorkersAreBusy(t *testing.T) {
	// Close unmaps every file. A worker still decoding from one would
	// read memory that is no longer mapped, so Close drains the pool
	// before it closes anything. The race detector in CI is what proves
	// this; here the loop only has to survive.
	ds := buildDataset(t, [][]int{{20000}, {20000}, {20000}, {20000}, {}})

	for i := 0; i < 20; i++ {
		m, err := NewConcurrentMerger(ds.openCursors(t, ds.Venues), 32)
		if err != nil {
			t.Fatalf("NewConcurrentMerger() error = %v, want nil", err)
		}
		if _, ok, err := m.Next(); err != nil || !ok {
			t.Fatalf("Next() = (_, %v, %v), want (_, true, nil)", ok, err)
		}

		if err := m.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
	}
}

func TestConcurrentMergerRejectsANonPositiveWorkerCount(t *testing.T) {
	for _, workers := range []int{0, -1} {
		t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
			cursors := cursorsFor(t, []venueFiles{{venue: 1, days: [][]store.Record{records(1, 0, 1)}}})

			_, err := NewConcurrentMerger(cursors, workers)

			if !errors.Is(err, ErrWorkerCount) {
				t.Fatalf("NewConcurrentMerger(%d) error = %v, want %v", workers, err, ErrWorkerCount)
			}
		})
	}
}

func TestConcurrentMergerCloseStopsEveryGoroutine(t *testing.T) {
	// Close is the only way to stop a run. Every goroutine it started
	// has to be gone when it returns, including a venue owner blocked
	// sending a batch the merge will never take, and a worker holding a
	// file that Close is about to unmap.
	ds := buildSynthDataset(t)

	tests := []struct {
		name string
		read int
	}{
		{name: "before_reading_anything", read: 0},
		{name: "part_way_through_the_stream", read: 10},
		{name: "well_into_the_stream", read: 3000},
	}

	for _, tt := range tests {
		for _, workers := range workerCounts {
			t.Run(tt.name+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
				before := runtime.NumGoroutine()

				m, err := NewConcurrentMerger(ds.openCursors(t, ds.Venues), workers)
				if err != nil {
					t.Fatalf("NewConcurrentMerger() error = %v, want nil", err)
				}
				for i := 0; i < tt.read; i++ {
					if _, ok, err := m.Next(); err != nil || !ok {
						t.Fatalf("Next() = (_, %v, %v) at record %d, want (_, true, nil)", ok, err, i)
					}
				}

				if err := m.Close(); err != nil {
					t.Fatalf("Close() error = %v, want nil", err)
				}

				waitForGoroutines(t, before)
				if err := m.Close(); err != nil {
					t.Errorf("second Close() error = %v, want nil", err)
				}
			})
		}
	}
}

func TestConcurrentMergerCloseAbandonsTheRestOfTheRun(t *testing.T) {
	// Close has to drain each venue's channel to release its goroutine.
	// Draining alone would be correct but would let every reader run to
	// the end of its partition first, so closing a six-hour replay after
	// ten records would cost the whole six hours of decode. The abort
	// flag is what stops that, and nothing else in this package's tests
	// would notice if it stopped working.
	//
	// The dataset has to be much larger than what the readers prefetch
	// before the merge asks for anything, or every venue would be read
	// to the end by the initial fill alone and the test would prove
	// nothing.
	ds := buildDataset(t, [][]int{{20000}, {12000, 8000}, {20000}, {20000}, {}})

	const read = 10

	m, err := NewConcurrentMerger(ds.openCursors(t, ds.Venues), 8)
	if err != nil {
		t.Fatalf("NewConcurrentMerger() error = %v, want nil", err)
	}
	for i := 0; i < read; i++ {
		if _, ok, err := m.Next(); err != nil || !ok {
			t.Fatalf("Next() = (_, %v, %v) at record %d, want (_, true, nil)", ok, err, i)
		}
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	// Safe to read now: every owner goroutine has returned.
	issued := 0
	for _, vf := range m.venues {
		for i := 0; i < vf.file && i < len(vf.readers); i++ {
			issued += vf.readers[i].Len()
		}
		if vf.file < len(vf.readers) {
			issued += vf.index
		}
	}

	// A venue can hold all feedBatches of its batches when the abort
	// lands, plus one more if it was already choosing a range. Anything
	// past that means a reader kept going instead of stopping.

	limit := read + len(ds.Venues)*(feedBatches+1)*batchRecords
	if limit >= ds.RecordCount() {
		t.Fatalf("the bound (%d) is not below the dataset size (%d); the fixture proves nothing",
			limit, ds.RecordCount())
	}
	if issued > limit {
		t.Errorf("readers issued %d of %d records after closing at record %d, want at most %d",
			issued, ds.RecordCount(), read, limit)
	}
}

func TestConcurrentMergerCloseAfterTheStreamEnds(t *testing.T) {
	ds := buildSynthDataset(t)
	before := runtime.NumGoroutine()

	m, err := NewConcurrentMerger(ds.openCursors(t, ds.Venues), 8)
	if err != nil {
		t.Fatalf("NewConcurrentMerger() error = %v, want nil", err)
	}
	if _, err := drainMerger(t, m); err != nil {
		t.Fatalf("Next() error = %v, want nil", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	waitForGoroutines(t, before)
}

// waitForGoroutines waits for the goroutine count to fall back to want.
// It yields rather than sleeping: a real sleep would read the clock,
// which this repository allows only inside internal/clock.
func waitForGoroutines(t *testing.T, want int) {
	t.Helper()

	for i := 0; i < 100000; i++ {
		got := runtime.NumGoroutine()
		if got <= want {
			return
		}
		runtime.Gosched()
	}
	t.Errorf("%d goroutines are still running, want at most %d", runtime.NumGoroutine(), want)
}
