package merge

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

// replayAllEvents merges cursors and returns every event, with each
// snapshot's blob copied out from under the mmap so it survives the
// merger's Close and can be compared after the reader that produced it
// is gone.
func replayAllEvents(t *testing.T, cursors []*Cursor) []Event {
	t.Helper()

	m, err := NewMerger(cursors)
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	}()

	events := make([]Event, 0, 4096)
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			return events
		}
		if ev.Blob != nil {
			ev.Blob = bytes.Clone(ev.Blob)
		}
		events = append(events, ev)
	}
}

// cmpEmptyEventSlices treats a nil and an empty event slice as equal. A
// seek target past every record produces no output either way, and the
// distinction between the two carries no meaning here.
var cmpEmptyEventSlices = cmp.FilterValues(
	func(a, b []Event) bool { return len(a) == 0 && len(b) == 0 },
	cmp.Comparer(func(a, b []Event) bool { return true }),
)

// suffixFrom returns the events in full whose exchange_ts is at or
// after t. exchange_ts is the first field of the canonical ordering
// key, so it is non-decreasing across full and a linear scan finds the
// cut point directly — the same definition seek_test.go in
// internal/store uses for naiveSeek.
func suffixFrom(full []Event, t int64) []Event {
	for i, ev := range full {
		if ev.Record.ExchangeTs >= t {
			return full[i:]
		}
	}
	return nil
}

// TestDeterminismSeekSuffix is the strongest test in the suite: replay(from=T) at
// the merge stage must be an exact record-for-record suffix of
// replay(from=0), for every venue seeked independently by its own
// SeekTime. This is what Q1's "exchange_ts non-decreasing per venue"
// assumption buys — each venue's own records with ts >= T are exactly a
// contiguous tail of that venue's sequence, so seeking every venue and
// re-merging gives exactly the tail of the full merged stream, nothing
// more and nothing less.
func TestDeterminismSeekSuffix(t *testing.T) {
	ds := buildSynthDataset(t)
	full := replayAllEvents(t, ds.openCursors(t, ds.Venues))

	if len(full) != ds.RecordCount() {
		t.Fatalf("full replay produced %d events, want %d", len(full), ds.RecordCount())
	}

	// Find a target that lands exactly on a snapshot's own timestamp and
	// one that lands strictly between two consecutive records'
	// timestamps, so both boundary cases are exercised on real data
	// rather than a hand-built fixture.
	var onSnapshot, betweenRecords int64 = -1, -1
	for i, ev := range full {
		if onSnapshot == -1 && i > 0 && ev.Record.RecordType == store.RecordTypeSnapshotPointer {
			onSnapshot = ev.Record.ExchangeTs
		}
		if betweenRecords == -1 && i > 0 && full[i].Record.ExchangeTs > full[i-1].Record.ExchangeTs+1 {
			betweenRecords = full[i-1].Record.ExchangeTs + 1
		}
	}
	if onSnapshot == -1 || betweenRecords == -1 {
		t.Fatal("the synthetic dataset no longer exercises both boundary cases this test needs")
	}

	tests := []struct {
		name string
		t    int64
	}{
		{name: "the_very_start", t: full[0].Record.ExchangeTs},
		{name: "before_every_record", t: full[0].Record.ExchangeTs - 1},
		{name: "exactly_a_snapshots_own_timestamp", t: onSnapshot},
		{name: "strictly_between_two_records", t: betweenRecords},
		{name: "the_very_last_record", t: full[len(full)-1].Record.ExchangeTs},
		{name: "past_every_record", t: full[len(full)-1].Record.ExchangeTs + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := suffixFrom(full, tt.t)

			cursors := ds.openCursors(t, ds.Venues)
			for _, c := range cursors {
				if err := c.SeekTime(tt.t); err != nil {
					t.Fatalf("SeekTime(%d) error = %v, want nil", tt.t, err)
				}
			}
			got := replayAllEvents(t, cursors)

			if diff := cmp.Diff(want, got, cmpEmptyEventSlices); diff != "" {
				t.Errorf("replay(from=%d) is not an exact suffix of replay(from=0) (-want +got):\n%s", tt.t, diff)
			}
		})
	}
}

// TestDeterminismSeekSuffixWithTheWorkerPool repeats TestDeterminismSeekSuffix's boundary
// cases through NewConcurrentMerger, at a few worker counts. Seeking
// happens on the Cursor before either constructor runs, but the
// concurrent path reads its starting position from a venueFeed built at
// construction time, not from the cursor directly — a wiring bug there
// would only show up once a worker pool is involved.
func TestDeterminismSeekSuffixWithTheWorkerPool(t *testing.T) {
	ds := buildSynthDataset(t)
	full := replayAllEvents(t, ds.openCursors(t, ds.Venues))

	targets := []int64{
		full[0].Record.ExchangeTs,
		full[len(full)/3].Record.ExchangeTs,
		full[2*len(full)/3].Record.ExchangeTs,
		full[len(full)-1].Record.ExchangeTs + 1,
	}

	for _, target := range targets {
		want := suffixFrom(full, target)

		for _, workers := range workerCounts {
			t.Run("t_"+strconv.FormatInt(target, 10)+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
				cursors := ds.openCursors(t, ds.Venues)
				for _, c := range cursors {
					if err := c.SeekTime(target); err != nil {
						t.Fatalf("SeekTime(%d) error = %v, want nil", target, err)
					}
				}

				m, err := NewConcurrentMerger(cursors, workers)
				if err != nil {
					t.Fatalf("NewConcurrentMerger(%d) error = %v, want nil", workers, err)
				}
				defer func() { _ = m.Close() }()

				got := make([]Event, 0, len(want))
				for {
					ev, ok, err := m.Next()
					if err != nil {
						t.Fatalf("Next() error = %v, want nil", err)
					}
					if !ok {
						break
					}
					if ev.Blob != nil {
						ev.Blob = bytes.Clone(ev.Blob)
					}
					got = append(got, ev)
				}

				if diff := cmp.Diff(want, got, cmpEmptyEventSlices); diff != "" {
					t.Errorf("replay(from=%d, workers=%d) is not an exact suffix of replay(from=0) (-want +got):\n%s",
						target, workers, diff)
				}
			})
		}
	}
}

// TestDeterminismSeekSuffixAcrossVenueOrder confirms the suffix property does not
// depend on which leaf of the loser tree a venue lands on, matching the
// same check TestDeterminism already does for a plain replay.
func TestDeterminismSeekSuffixAcrossVenueOrder(t *testing.T) {
	ds := buildSynthDataset(t)
	full := replayAllEvents(t, ds.openCursors(t, ds.Venues))
	target := full[len(full)/2].Record.ExchangeTs
	want := suffixFrom(full, target)

	for i := 1; i < len(ds.Venues); i++ {
		t.Run("rotated_by_"+strconv.Itoa(i), func(t *testing.T) {
			order := append(append([]uint16{}, ds.Venues[i:]...), ds.Venues[:i]...)

			cursors := ds.openCursors(t, order)
			for _, c := range cursors {
				if err := c.SeekTime(target); err != nil {
					t.Fatalf("SeekTime(%d) error = %v, want nil", target, err)
				}
			}
			got := replayAllEvents(t, cursors)

			if diff := cmp.Diff(want, got, cmpEmptyEventSlices); diff != "" {
				t.Errorf("replay(from=%d) mismatch under a rotated venue order (-want +got):\n%s", target, diff)
			}
		})
	}
}
