package merge

import (
	"encoding/hex"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

const (
	// synthSeed fixes the generated dataset. It is part of the fixture,
	// not a source of variation: two runs must build the same bytes.
	synthSeed = 0x5EED

	// synthSnapshotEvery is the snapshot epoch cadence, in records per
	// venue. It is not a multiple of the 1024-record block size, so an
	// epoch lands at a different offset in each block.
	synthSnapshotEvery = 150
)

// synthShape gives the record count of every venue's day files. The
// counts differ on purpose: venues end at different times, one day file
// is empty, one venue is a single short day, and the last venue has no
// files at all. Together they cover every case the tree's sentinel
// handling has to carry.
var synthShape = [][]int{
	{1500, 1200, 900},
	{800, 0, 1100},
	{2000, 300},
	{90},
	{},
}

// synthDataset is a generated multi-venue dataset on disk, partitioned
// by venue and day — the granularity cmd/convert produces.
type synthDataset struct {
	venues []uint16
	files  [][]string
}

// buildSynthDataset writes the dataset and returns it. Generation is a
// pure function of synthSeed and synthShape.
func buildSynthDataset(t *testing.T) *synthDataset {
	t.Helper()

	dir := t.TempDir()
	ds := &synthDataset{
		venues: make([]uint16, len(synthShape)),
		files:  make([][]string, len(synthShape)),
	}

	for v, days := range synthShape {
		venue := uint16(v + 1)
		ds.venues[v] = venue

		rng := rand.New(rand.NewPCG(synthSeed, uint64(venue)))
		ts := int64(v) * 7 // venues overlap in time but do not start together
		seq := uint64(0)
		written := 0

		for d, count := range days {
			path := filepath.Join(dir, "venue"+strconv.Itoa(int(venue))+"-day"+strconv.Itoa(d)+".bin")
			w, err := store.NewWriter(path, venue, 100)
			if err != nil {
				t.Fatalf("NewWriter(%s) error = %v, want nil", path, err)
			}

			for i := 0; i < count; i++ {
				// The timestamp may stand still, so a run of equal
				// timestamps straddles block and file boundaries. The
				// sequence number always increases, which is what keeps
				// the key strictly increasing.
				ts += rng.Int64N(4)
				rec := store.Record{
					ExchangeTs:     ts,
					SequenceNumber: seq,
					InstrumentID:   rng.Uint32N(8),
					VenueID:        venue,
				}
				seq++

				if written%synthSnapshotEvery == 0 {
					bids := []store.Level{{Price: 100, Size: 1 + rng.Int64N(9)}, {Price: 99, Size: 1 + rng.Int64N(9)}}
					asks := []store.Level{{Price: 101, Size: 1 + rng.Int64N(9)}}
					err = w.WriteSnapshot(rec, bids, asks)
				} else {
					rec.RecordType = store.RecordTypeDelta
					rec.SideFlags = uint8(rng.UintN(2))
					rec.Price = 100 + rng.Int64N(50)
					rec.Size = 1 + rng.Int64N(10)
					err = w.WriteRecord(rec)
				}
				if err != nil {
					t.Fatalf("writing venue %d day %d record %d: %v", venue, d, i, err)
				}
				written++
			}

			if err := w.Close(); err != nil {
				t.Fatalf("Close(%s) error = %v, want nil", path, err)
			}
			ds.files[v] = append(ds.files[v], path)
		}
	}
	return ds
}

// recordCount is how many records the dataset holds in total.
func (ds *synthDataset) recordCount() int {
	n := 0
	for _, days := range synthShape {
		for _, count := range days {
			n += count
		}
	}
	return n
}

// openCursors opens one cursor per venue, in the given venue order. The
// caller closes them through the Merger.
func (ds *synthDataset) openCursors(t *testing.T, order []uint16) []*Cursor {
	t.Helper()

	cursors := make([]*Cursor, 0, len(order))
	for _, venue := range order {
		v := slices.Index(ds.venues, venue)
		if v < 0 {
			t.Fatalf("venue %d is not in the dataset", venue)
		}

		readers := make([]*store.Reader, 0, len(ds.files[v]))
		for _, path := range ds.files[v] {
			r, err := store.Open(path)
			if err != nil {
				t.Fatalf("Open(%s) error = %v, want nil", path, err)
			}
			readers = append(readers, r)
		}
		c, err := NewCursor(venue, readers)
		if err != nil {
			t.Fatalf("NewCursor(%d) error = %v, want nil", venue, err)
		}
		cursors = append(cursors, c)
	}
	return cursors
}

// replayResult is everything one replay produces that the determinism
// assertions compare.
type replayResult struct {
	hash      string
	count     int
	snapshots int
	keys      []Key
}

// replaySynth merges the dataset in the given venue order and hashes the
// merged stream with the canonical_v1 projection.
func replaySynth(t *testing.T, ds *synthDataset, order []uint16) replayResult {
	t.Helper()

	m, err := NewMerger(ds.openCursors(t, order))
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Merger.Close() error = %v, want nil", err)
		}
	}()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	res := replayResult{keys: make([]Key, 0, ds.recordCount())}
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Merger.Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		h.Write(ev.Record, ev.Blob)
		res.keys = append(res.keys, keyOf(ev.Record))
		res.count++
		if ev.Record.RecordType == store.RecordTypeSnapshotPointer {
			res.snapshots++
			if len(ev.Blob) == 0 {
				t.Fatalf("record %d is a snapshot pointer with no blob payload", res.count-1)
			}
		}
	}
	res.hash = hex.EncodeToString(h.Sum(nil))
	return res
}

// TestDeterminism is this project's core assertion. At M4 there is no
// worker pool yet, so it pins the merge stage's own determinism: the
// merged stream must not depend on GOMAXPROCS, nor on which leaf of the
// loser tree a venue lands on. M5 adds the worker-count variants.
func TestDeterminism(t *testing.T) {
	ds := buildSynthDataset(t)
	want := replaySynth(t, ds, ds.venues)

	t.Run("the_dataset_exercises_what_the_hash_claims_to_cover", func(t *testing.T) {
		// Guards every case below. A dataset with no snapshots, or one
		// that fits in a single block, would still compare equal while
		// testing far less than it appears to.
		if want.count != ds.recordCount() {
			t.Fatalf("replayed %d records, want %d", want.count, ds.recordCount())
		}
		if want.snapshots < 20 {
			t.Errorf("replayed %d snapshot pointers, want at least 20", want.snapshots)
		}
		if want.count < 4*1024 {
			t.Errorf("replayed %d records, want enough to span several 1024-record blocks", want.count)
		}
	})

	t.Run("the_merged_stream_is_in_canonical_order", func(t *testing.T) {
		for i := 1; i < len(want.keys); i++ {
			if compareKey(want.keys[i-1], want.keys[i]) >= 0 {
				t.Fatalf("keys %d and %d do not increase: %v then %v",
					i-1, i, want.keys[i-1], want.keys[i])
			}
		}
	})

	t.Run("every_venues_own_order_survives_the_merge", func(t *testing.T) {
		for v, venue := range ds.venues {
			var got []Key
			for _, key := range want.keys {
				if key.VenueID == venue {
					got = append(got, key)
				}
			}
			if diff := cmp.Diff(readVenueKeys(t, ds, v), got, cmpEmptyKeySlices); diff != "" {
				t.Errorf("venue %d subsequence mismatch (-want +got):\n%s", venue, diff)
			}
		}
	})

	t.Run("the_hash_does_not_depend_on_gomaxprocs", func(t *testing.T) {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

		for _, procs := range []int{1, 2, 4, 8, 16} {
			t.Run("gomaxprocs_"+strconv.Itoa(procs), func(t *testing.T) {
				runtime.GOMAXPROCS(procs)

				got := replaySynth(t, ds, ds.venues)

				assertSameReplay(t, want, got)
			})
		}
	})

	t.Run("the_hash_does_not_depend_on_which_leaf_a_venue_lands_on", func(t *testing.T) {
		// Which cursor index a venue gets is an arbitrary choice made
		// when partitions are discovered. It must not reach the output.
		for i := 1; i < len(ds.venues); i++ {
			t.Run("rotated_by_"+strconv.Itoa(i), func(t *testing.T) {
				order := append(slices.Clone(ds.venues[i:]), ds.venues[:i]...)

				got := replaySynth(t, ds, order)

				assertSameReplay(t, want, got)
			})
		}
	})
}

func assertSameReplay(t *testing.T, want, got replayResult) {
	t.Helper()

	if got.count != want.count {
		t.Errorf("replayed %d records, want %d", got.count, want.count)
	}
	if got.hash != want.hash {
		t.Errorf("canonical hash = %s, want %s", got.hash, want.hash)
	}
	if diff := cmp.Diff(want.keys, got.keys); diff != "" {
		t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
	}
}

// readVenueKeys reads one venue's keys straight from its files, in file
// order, without going through the merge.
func readVenueKeys(t *testing.T, ds *synthDataset, v int) []Key {
	t.Helper()

	var keys []Key
	for _, path := range ds.files[v] {
		r, err := store.Open(path)
		if err != nil {
			t.Fatalf("Open(%s) error = %v, want nil", path, err)
		}
		for i := 0; i < r.Len(); i++ {
			keys = append(keys, keyOf(r.RecordAt(i)))
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close(%s) error = %v, want nil", path, err)
		}
	}
	return keys
}
