package merge

import (
	"encoding/hex"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
	"replay/internal/synth"
)

// synthDataset adds this package's own cursor-opening step to a
// generated synth.Dataset. Generation itself — the shape, the seed, the
// byte-for-byte content — lives in internal/synth, shared with
// internal/fanout's own determinism suite so the two never carry two
// copies of the same fixture generator that could silently drift apart.
type synthDataset struct {
	*synth.Dataset
}

// buildSynthDataset writes the standard dataset and returns it.
func buildSynthDataset(t *testing.T) *synthDataset {
	t.Helper()

	return &synthDataset{synth.Standard(t, t.TempDir())}
}

// buildDataset writes a dataset with the given shape and returns it.
func buildDataset(t *testing.T, shape [][]int) *synthDataset {
	t.Helper()

	return &synthDataset{synth.Build(t, t.TempDir(), shape)}
}

// openCursors opens one cursor per venue, in the given venue order. The
// caller closes them through the Merger.
func (ds *synthDataset) openCursors(t *testing.T, order []uint16) []*Cursor {
	t.Helper()

	cursors := make([]*Cursor, 0, len(order))
	for _, venue := range order {
		v := slices.Index(ds.Venues, venue)
		if v < 0 {
			t.Fatalf("venue %d is not in the dataset", venue)
		}

		readers := make([]*store.Reader, 0, len(ds.Files[v]))
		for _, path := range ds.Files[v] {
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

// replaySynth merges the dataset in the given venue order, decoding in
// the calling goroutine, and hashes the merged stream with the
// canonical_v1 projection.
func replaySynth(t *testing.T, ds *synthDataset, order []uint16) replayResult {
	t.Helper()

	m, err := NewMerger(ds.openCursors(t, order))
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	return drainReplay(t, ds, m)
}

// replaySynthConcurrent is replaySynth with a pool of workers decoding.
func replaySynthConcurrent(t *testing.T, ds *synthDataset, order []uint16, workers int) replayResult {
	t.Helper()

	m, err := NewConcurrentMerger(ds.openCursors(t, order), workers)
	if err != nil {
		t.Fatalf("NewConcurrentMerger(%d) error = %v, want nil", workers, err)
	}
	return drainReplay(t, ds, m)
}

// replaySynthShaped is replaySynthConcurrent with an explicit batch
// shape. It checks that every venue feed really carries that shape, so a
// knob that stopped being applied would fail here rather than let the
// batch-shape subtests replay the default shape and still pass.
func replaySynthShaped(t *testing.T, ds *synthDataset, order []uint16, workers int, shape batchShape) replayResult {
	t.Helper()

	m, err := newMergerShape(ds.openCursors(t, order), workers, shape)
	if err != nil {
		t.Fatalf("newMergerShape(%d, %+v) error = %v, want nil", workers, shape, err)
	}
	for _, vf := range m.venues {
		if vf.shape != shape {
			t.Fatalf("venue %d feed shape = %+v, want %+v", vf.venueID, vf.shape, shape)
		}
	}
	return drainReplay(t, ds, m)
}

func drainReplay(t *testing.T, ds *synthDataset, m *Merger) replayResult {
	t.Helper()

	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Merger.Close() error = %v, want nil", err)
		}
	}()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	res := replayResult{keys: make([]Key, 0, ds.RecordCount())}
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
	want := replaySynth(t, ds, ds.Venues)

	t.Run("the_dataset_exercises_what_the_hash_claims_to_cover", func(t *testing.T) {
		// Guards every case below. A dataset with no snapshots, or one
		// that fits in a single block, would still compare equal while
		// testing far less than it appears to.
		if want.count != ds.RecordCount() {
			t.Fatalf("replayed %d records, want %d", want.count, ds.RecordCount())
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
		for v, venue := range ds.Venues {
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

				got := replaySynth(t, ds, ds.Venues)

				assertSameReplay(t, want, got)
			})
		}
	})

	t.Run("the_hash_does_not_depend_on_which_leaf_a_venue_lands_on", func(t *testing.T) {
		// Which cursor index a venue gets is an arbitrary choice made
		// when partitions are discovered. It must not reach the output.
		for i := 1; i < len(ds.Venues); i++ {
			t.Run("rotated_by_"+strconv.Itoa(i), func(t *testing.T) {
				order := append(slices.Clone(ds.Venues[i:]), ds.Venues[:i]...)

				got := replaySynth(t, ds, order)

				assertSameReplay(t, want, got)
			})
		}
	})

	t.Run("the_hash_does_not_depend_on_the_worker_count", func(t *testing.T) {
		// The worker count changes decode and I/O concurrency only. It
		// never changes the tree's shape, and a batch carries nothing
		// that says which worker filled it.
		for _, workers := range workerCounts {
			t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
				got := replaySynthConcurrent(t, ds, ds.Venues, workers)

				assertSameReplay(t, want, got)
			})
		}
	})

	t.Run("the_hash_does_not_depend_on_the_worker_count_and_gomaxprocs_together", func(t *testing.T) {
		// Varying one at a time can hide a dependency on their ratio:
		// with GOMAXPROCS pinned to 1, many workers never truly overlap.
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

		for _, procs := range []int{1, 4, 16} {
			for _, workers := range workerCounts {
				t.Run("gomaxprocs_"+strconv.Itoa(procs)+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
					runtime.GOMAXPROCS(procs)

					got := replaySynthConcurrent(t, ds, ds.Venues, workers)

					assertSameReplay(t, want, got)
				})
			}
		}
	})

	t.Run("the_hash_does_not_depend_on_the_batch_shape", func(t *testing.T) {
		// Batch size and feed-channel depth change only how decode work is
		// cut up and how far a reader may run ahead of the merge. One
		// record per batch puts a batch boundary between every pair of
		// records; one batch per dataset puts none inside a file at all.
		for _, bs := range batchShapes(ds.RecordCount()) {
			for _, workers := range workerCounts {
				t.Run(bs.name+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
					got := replaySynthShaped(t, ds, ds.Venues, workers, bs.shape)

					assertSameReplay(t, want, got)
				})
			}
		}
	})

	t.Run("the_worker_pool_agrees_with_inline_decoding_on_every_venue_order", func(t *testing.T) {
		for i := 1; i < len(ds.Venues); i++ {
			t.Run("rotated_by_"+strconv.Itoa(i), func(t *testing.T) {
				order := append(slices.Clone(ds.Venues[i:]), ds.Venues[:i]...)

				got := replaySynthConcurrent(t, ds, order, 8)

				assertSameReplay(t, want, got)
			})
		}
	})
}

// workerCounts are the worker counts make determinism replays at.
var workerCounts = []int{1, 4, 16, 64}

// shapeCase is one batch geometry the determinism suite replays at.
type shapeCase struct {
	name  string
	shape batchShape
}

// batchShapes are the batch geometries make determinism replays a
// dataset of n records at. Both extremes are covered on purpose: a batch
// of one record is where an off-by-one at a batch boundary shows up,
// and a batch larger than the dataset is where no boundary exists to
// hide one. A depth of two is the shallowest feed a venue can make
// progress with, so it is the most backpressure a reader goroutine can
// be put under.
func batchShapes(n int) []shapeCase {
	return []shapeCase{
		{name: "one_record_per_batch", shape: batchShape{records: 1, batches: feedBatches}},
		{name: "one_record_per_batch_with_the_shallowest_feed", shape: batchShape{records: 1, batches: 2}},
		{name: "a_batch_that_divides_no_block", shape: batchShape{records: 97, batches: 2}},
		{name: "the_default_size_with_the_shallowest_feed", shape: batchShape{records: batchRecords, batches: 2}},
		{name: "one_batch_for_the_whole_dataset", shape: batchShape{records: n + 1, batches: feedBatches}},
		{name: "one_batch_for_the_whole_dataset_with_the_shallowest_feed", shape: batchShape{records: n + 1, batches: 2}},
	}
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
	for _, path := range ds.Files[v] {
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
