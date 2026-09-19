package merge

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

// venueFiles is one venue's partition: its id and the records of each of
// its files, in day order.
type venueFiles struct {
	venue uint16
	days  [][]store.Record
}

func newMergerOver(t *testing.T, partitions []venueFiles) *Merger {
	t.Helper()

	cursors := make([]*Cursor, len(partitions))
	for i, p := range partitions {
		paths := make([]string, len(p.days))
		for d, recs := range p.days {
			paths[d] = writeVenueFile(t, "day.bin", p.venue, recs)
		}
		cursors[i] = newCursorOver(t, p.venue, paths...)
	}
	m, err := NewMerger(cursors)
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// drainMerger reads a merger to the end, or to its first error.
func drainMerger(t *testing.T, m *Merger) ([]Key, error) {
	t.Helper()

	var got []Key
	for {
		ev, ok, err := m.Next()
		if err != nil {
			return got, err
		}
		if !ok {
			return got, nil
		}
		got = append(got, keyOf(ev.Record))
		if len(got) > 1<<20 {
			t.Fatal("merger is not draining")
		}
	}
}

// records builds delta records for one venue from explicit timestamps.
// The sequence number increments, so the key always increases even when
// a timestamp repeats.
func records(venue uint16, firstSeq uint64, timestamps ...int64) []store.Record {
	recs := make([]store.Record, len(timestamps))
	for i, ts := range timestamps {
		recs[i] = store.Record{
			ExchangeTs:     ts,
			SequenceNumber: firstSeq + uint64(i),
			InstrumentID:   testInstrument,
			VenueID:        venue,
			RecordType:     store.RecordTypeDelta,
			Price:          100,
			Size:           1,
		}
	}
	return recs
}

func TestMergerEndsWhenEveryVenueIsExhausted(t *testing.T) {
	// The stream ends at the last venue to run out, not the first. A
	// venue that ends early simply stops winning matches, and its records
	// are all still emitted.
	tests := []struct {
		name       string
		partitions []venueFiles
		want       []Key
	}{
		{
			name: "one_venue_ends_long_before_the_other",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{records(1, 0, 1, 2, 3)}},
				{venue: 2, days: [][]store.Record{records(2, 0, 10, 20, 30, 40, 50)}},
			},
			want: []Key{
				mk(1, 1, 0, testInstrument), mk(2, 1, 1, testInstrument), mk(3, 1, 2, testInstrument),
				mk(10, 2, 0, testInstrument), mk(20, 2, 1, testInstrument), mk(30, 2, 2, testInstrument),
				mk(40, 2, 3, testInstrument), mk(50, 2, 4, testInstrument),
			},
		},
		{
			name: "one_venue_starts_long_after_the_other_ends",
			partitions: []venueFiles{
				{venue: 1, days: [][]store.Record{records(1, 0, 100, 200)}},
				{venue: 2, days: [][]store.Record{records(2, 0, 1, 2, 3)}},
			},
			want: []Key{
				mk(1, 2, 0, testInstrument), mk(2, 2, 1, testInstrument), mk(3, 2, 2, testInstrument),
				mk(100, 1, 0, testInstrument), mk(200, 1, 1, testInstrument),
			},
		},
		{
			name: "an_empty_partition_alongside_a_full_one",
			partitions: []venueFiles{
				{venue: 1, days: nil},
				{venue: 2, days: [][]store.Record{records(2, 0, 1, 2)}},
			},
			want: []Key{mk(1, 2, 0, testInstrument), mk(2, 2, 1, testInstrument)},
		},
		{
			name: "venues_sharing_a_timestamp_order_by_venue_id",
			partitions: []venueFiles{
				{venue: 5, days: [][]store.Record{records(5, 0, 1)}},
				{venue: 2, days: [][]store.Record{records(2, 0, 1)}},
				{venue: 9, days: [][]store.Record{records(9, 0, 1)}},
			},
			want: []Key{
				mk(1, 2, 0, testInstrument), mk(1, 5, 0, testInstrument), mk(1, 9, 0, testInstrument),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMergerOver(t, tt.partitions)

			got, err := drainMerger(t, m)

			if err != nil {
				t.Fatalf("Next() error = %v, want nil", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergerWithNothingToMerge(t *testing.T) {
	tests := []struct {
		name       string
		partitions []venueFiles
	}{
		{name: "no_venues_at_all", partitions: nil},
		{
			name: "every_venue_empty",
			partitions: []venueFiles{
				{venue: 1, days: nil},
				{venue: 2, days: [][]store.Record{{}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMergerOver(t, tt.partitions)

			got, err := drainMerger(t, m)

			if err != nil {
				t.Fatalf("Next() error = %v, want nil", err)
			}
			if len(got) != 0 {
				t.Errorf("emitted %d records, want 0", len(got))
			}
		})
	}
}

func TestMergerRejectsTwoPartitionsOfTheSameVenue(t *testing.T) {
	// One cursor per venue is what makes a cross-partition duplicate key
	// structurally impossible. Two cursors on one venue would leave that
	// claim resting on the data instead of on the partitioning.
	cursors := []*Cursor{
		newCursorOver(t, 3, writeVenueFile(t, "a.bin", 3, records(3, 0, 1, 2))),
		newCursorOver(t, 3, writeVenueFile(t, "b.bin", 3, records(3, 10, 100, 200))),
	}

	_, err := NewMerger(cursors)

	if !errors.Is(err, ErrDuplicateVenue) {
		t.Fatalf("NewMerger() error = %v, want %v", err, ErrDuplicateVenue)
	}
}

func TestMergerReportsACursorError(t *testing.T) {
	// A cursor rejects a key that does not increase across a file
	// boundary. The merge must surface that, not skip the partition.
	last := records(3, 5, 100)[0]
	m := newMergerOver(t, []venueFiles{
		{venue: 3, days: [][]store.Record{{last}, {last}}},
		{venue: 4, days: [][]store.Record{records(4, 0, 1, 2, 3)}},
	})

	_, err := drainMerger(t, m)

	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Next() error = %v, want %v", err, ErrDuplicateKey)
	}
}

func TestMergerCarriesEachSnapshotsOwnBlob(t *testing.T) {
	// The merge holds one event per venue ahead of what it emits, so a
	// blob read at emit time would come from wherever that cursor had
	// already moved on to. The payload has to travel with its record.
	venues := []uint16{1, 2}
	paths := []string{
		writeSnapshotFile(t, 1, 10, 7),
		writeSnapshotFile(t, 2, 20, 9),
	}
	cursors := make([]*Cursor, len(venues))
	for i, venue := range venues {
		cursors[i] = newCursorOver(t, venue, paths[i])
	}
	m, err := NewMerger(cursors)
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() { _ = m.Close() }()

	got := make([][]byte, len(venues))
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		if ev.Record.RecordType != store.RecordTypeSnapshotPointer {
			if ev.Blob != nil {
				t.Errorf("venue %d delta record carries a blob", ev.Record.VenueID)
			}
			continue
		}
		got[slices.Index(venues, ev.Record.VenueID)] = bytes.Clone(ev.Blob)
	}

	for i, venue := range venues {
		if diff := cmp.Diff(readSnapshotBlob(t, paths[i]), got[i]); diff != "" {
			t.Errorf("venue %d snapshot blob mismatch (-want +got):\n%s", venue, diff)
		}
	}
	if bytes.Equal(got[0], got[1]) {
		t.Error("both venues produced the same blob; the fixture no longer tells them apart")
	}
}

// writeSnapshotFile writes one venue file whose second record is a
// snapshot pointer, with a bid size unique to that venue.
func writeSnapshotFile(t *testing.T, venue uint16, firstTs int64, bidSize int64) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "snap.bin")
	w, err := store.NewWriter(path, venue, 100)
	if err != nil {
		t.Fatalf("NewWriter() error = %v, want nil", err)
	}
	if err := w.WriteRecord(records(venue, 0, firstTs)[0]); err != nil {
		t.Fatalf("WriteRecord() error = %v, want nil", err)
	}
	snap := store.Record{
		ExchangeTs:     firstTs + 1,
		SequenceNumber: 1,
		InstrumentID:   testInstrument,
		VenueID:        venue,
	}
	if err := w.WriteSnapshot(snap, []store.Level{{Price: 100, Size: bidSize}}, []store.Level{{Price: 101, Size: 1}}); err != nil {
		t.Fatalf("WriteSnapshot() error = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	return path
}

// readSnapshotBlob returns the payload of the file's only snapshot
// pointer, read straight from the store without going through a cursor.
func readSnapshotBlob(t *testing.T, path string) []byte {
	t.Helper()

	r := openVenueFile(t, path)
	for i := 0; i < r.Len(); i++ {
		rec := r.RecordAt(i)
		if rec.RecordType != store.RecordTypeSnapshotPointer {
			continue
		}
		blob, err := r.Blob(rec)
		if err != nil {
			t.Fatalf("Blob() error = %v, want nil", err)
		}
		return bytes.Clone(blob)
	}
	t.Fatalf("%s holds no snapshot pointer", path)
	return nil
}
