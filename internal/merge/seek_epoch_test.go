package merge

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

// A snapshot epoch is a contiguous run of snapshot-pointer records, one
// per active instrument, all at one timestamp — see docs/book.md.
// internal/synth snapshots a single instrument at a time, so no epoch
// run exists anywhere in the standard dataset and a seek target between
// two epochs cannot be expressed on it. This fixture is that shape, and
// it is local to this file on purpose: internal/synth is replayed by
// both determinism suites and by cmd/replayd's own tests, so changing
// its output would change every committed hash in the repository.
const (
	epochInstruments = 4  // one snapshot per instrument in an epoch run
	epochDeltaRun    = 20 // delta records between one epoch and the next
)

// epochVenue is one venue of the epoch fixture: its id and the
// timestamps of its two epoch runs.
type epochVenue struct {
	venue  uint16
	epochs [2]int64
}

// epochVenues place venue 2's epochs a few ticks after venue 1's, so a
// target between the two epochs is between them for both venues, and a
// target just after the first epoch splits the pair.
var epochVenues = []epochVenue{
	{venue: 1, epochs: [2]int64{100, 200}},
	{venue: 2, epochs: [2]int64{105, 205}},
}

// writeEpochFile writes one venue's file: a delta run, an epoch run, a
// second delta run, a second epoch run, and a closing delta run. Every
// snapshot in a run carries the same ExchangeTs and its own instrument,
// which is what makes the run an epoch rather than a lone snapshot.
func writeEpochFile(t *testing.T, ev epochVenue) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "epoch.bin")
	w, err := store.NewWriter(path, ev.venue, 100)
	if err != nil {
		t.Fatalf("NewWriter() error = %v, want nil", err)
	}

	seq := uint64(0)
	delta := func(ts int64, instrument uint32) {
		rec := store.Record{
			ExchangeTs:     ts,
			SequenceNumber: seq,
			InstrumentID:   instrument,
			VenueID:        ev.venue,
			RecordType:     store.RecordTypeDelta,
			Price:          100 + ts,
			Size:           1 + int64(instrument),
		}
		seq++
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("WriteRecord(ts=%d) error = %v, want nil", ts, err)
		}
	}
	deltaRun := func(firstTs int64) {
		for i := 0; i < epochDeltaRun; i++ {
			delta(firstTs+int64(i), uint32(i%epochInstruments)+1)
		}
	}
	epochRun := func(ts int64) {
		for i := 0; i < epochInstruments; i++ {
			instrument := uint32(i) + 1
			rec := store.Record{
				ExchangeTs:     ts,
				SequenceNumber: seq,
				InstrumentID:   instrument,
				VenueID:        ev.venue,
			}
			seq++
			bids := []store.Level{{Price: 100 + int64(instrument), Size: 1 + int64(instrument)}}
			asks := []store.Level{{Price: 200 + int64(instrument), Size: 2 + int64(instrument)}}
			if err := w.WriteSnapshot(rec, bids, asks); err != nil {
				t.Fatalf("WriteSnapshot(ts=%d, instrument=%d) error = %v, want nil", ts, instrument, err)
			}
		}
	}

	deltaRun(ev.epochs[0] - 30)
	epochRun(ev.epochs[0])
	deltaRun(ev.epochs[0] + 20)
	epochRun(ev.epochs[1])
	deltaRun(ev.epochs[1] + 20)

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	return path
}

// buildEpochFiles writes one file per venue and returns their paths, in
// epochVenues order.
func buildEpochFiles(t *testing.T) []string {
	t.Helper()

	paths := make([]string, len(epochVenues))
	for i, ev := range epochVenues {
		paths[i] = writeEpochFile(t, ev)
	}
	return paths
}

// openEpochCursors opens one cursor per venue, each seeked to target.
func openEpochCursors(t *testing.T, paths []string, target int64) []*Cursor {
	t.Helper()

	cursors := make([]*Cursor, len(paths))
	for i, path := range paths {
		c := newCursorOver(t, epochVenues[i].venue, path)
		if err := c.SeekTime(target); err != nil {
			t.Fatalf("SeekTime(%d) error = %v, want nil", target, err)
		}
		cursors[i] = c
	}
	return cursors
}

// snapshotRunLengths returns the length of every maximal run of
// consecutive snapshot-pointer records in events.
func snapshotRunLengths(events []Event) []int {
	var runs []int
	run := 0
	for _, ev := range events {
		if ev.Record.RecordType == store.RecordTypeSnapshotPointer {
			run++
			continue
		}
		if run > 0 {
			runs = append(runs, run)
			run = 0
		}
	}
	if run > 0 {
		runs = append(runs, run)
	}
	return runs
}

// replayAllEventsConcurrent is replayAllEvents with a pool of workers
// decoding. A seek applied before construction reaches the concurrent
// path through the venue feed, not through the cursor, so an epoch
// boundary has to be checked on both paths.
func replayAllEventsConcurrent(t *testing.T, cursors []*Cursor, workers int) []Event {
	t.Helper()

	m, err := NewConcurrentMerger(cursors, workers)
	if err != nil {
		t.Fatalf("NewConcurrentMerger(%d) error = %v, want nil", workers, err)
	}
	return drainEvents(t, m)
}

// TestDeterminismSeekSuffixAcrossSnapshotEpochs is TestDeterminismSeekSuffix's
// exact-suffix property at the one boundary the standard dataset cannot
// express: a target between two snapshot epochs. An epoch run is several
// records at one timestamp, so a seek either takes the whole run or none
// of it, and a merge that split one would produce a stream that is no
// longer a suffix of the full replay.
func TestDeterminismSeekSuffixAcrossSnapshotEpochs(t *testing.T) {
	paths := buildEpochFiles(t)
	full := replayAllEvents(t, openEpochCursors(t, paths, 0))

	t.Run("the_fixture_really_holds_two_snapshot_epochs", func(t *testing.T) {
		// Guards every case below. Without a genuine multi-instrument run
		// at one timestamp, a target "between two epochs" would be an
		// ordinary target between two records and would test nothing new.
		want := []int{len(epochVenues) * epochInstruments, len(epochVenues) * epochInstruments}

		got := snapshotRunLengths(full)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("snapshotRunLengths(full) mismatch (-want +got):\n%s", diff)
		}
	})

	tests := []struct {
		name string
		t    int64
	}{
		{name: "before_every_record", t: 0},
		{name: "exactly_the_first_epochs_own_timestamp", t: epochVenues[0].epochs[0]},
		{name: "after_one_venues_epoch_but_before_the_others", t: epochVenues[0].epochs[0] + 1},
		{name: "strictly_between_two_snapshot_epochs", t: 150},
		{name: "between_two_epochs_inside_a_delta_run", t: 130},
		{name: "exactly_the_second_epochs_own_timestamp", t: epochVenues[0].epochs[1]},
		{name: "past_every_record", t: full[len(full)-1].Record.ExchangeTs + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := suffixFrom(full, tt.t)

			got := replayAllEvents(t, openEpochCursors(t, paths, tt.t))

			if diff := cmp.Diff(want, got, cmpEmptyEventSlices); diff != "" {
				t.Errorf("replay(from=%d) is not an exact suffix of replay(from=0) (-want +got):\n%s", tt.t, diff)
			}

			for _, workers := range workerCounts {
				t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
					got := replayAllEventsConcurrent(t, openEpochCursors(t, paths, tt.t), workers)

					if diff := cmp.Diff(want, got, cmpEmptyEventSlices); diff != "" {
						t.Errorf("replay(from=%d, workers=%d) is not an exact suffix of replay(from=0) (-want +got):\n%s",
							tt.t, workers, diff)
					}
				})
			}
		})
	}
}
