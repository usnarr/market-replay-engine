package fanout

import (
	"encoding/hex"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"replay/internal/merge"
	"replay/internal/store"
	"replay/internal/synth"
)

// openCursors opens one cursor per venue from ds, in ds.Venues order.
// The caller closes them through the Merger it builds from them. This
// mirrors internal/merge's own openCursors, using the same exported
// merge.NewCursor a real caller outside this repository's test tree
// would use — fanout has no special access to merge's internals.
func openCursors(t *testing.T, ds *synth.Dataset) []*merge.Cursor {
	t.Helper()

	cursors := make([]*merge.Cursor, 0, len(ds.Venues))
	for v, venue := range ds.Venues {
		readers := make([]*store.Reader, 0, len(ds.Files[v]))
		for _, path := range ds.Files[v] {
			r, err := store.Open(path)
			if err != nil {
				t.Fatalf("Open(%s) error = %v, want nil", path, err)
			}
			readers = append(readers, r)
		}
		c, err := merge.NewCursor(venue, readers)
		if err != nil {
			t.Fatalf("NewCursor(%d) error = %v, want nil", venue, err)
		}
		cursors = append(cursors, c)
	}
	return cursors
}

// wantResult is the merged stream's own hash and record count, computed
// with no ring involved at all — the ground truth every fan-out
// configuration in this file is checked against.
type wantResult struct {
	hash  string
	count int
}

func computeWant(t *testing.T, ds *synth.Dataset) wantResult {
	t.Helper()

	m, err := merge.NewMerger(openCursors(t, ds))
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	}()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	res := wantResult{}
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		h.Write(ev.Record, ev.Blob)
		res.count++
	}
	res.hash = hex.EncodeToString(h.Sum(nil))
	return res
}

// subResult is what one subscriber's drain produced.
type subResult struct {
	hash     string
	received int
	gaps     []Gap
}

// drainSubscriber reads sub to the end of stream, hashing every record
// it receives with the same canonical projection computeWant uses, and
// collecting every gap it was ever handed.
func drainSubscriber(t *testing.T, sub *Subscriber, perturb func()) subResult {
	t.Helper()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	var res subResult
	for {
		if perturb != nil {
			perturb()
		}
		d, ok, err := sub.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		if d.HasGap {
			res.gaps = append(res.gaps, d.Gap)
		}
		h.Write(d.Record, d.Blob)
		res.received++
	}
	res.hash = hex.EncodeToString(h.Sum(nil))
	return res
}

// runFanout replays ds through a Merger with the given worker count,
// into a fresh Ring of the given capacity, with nBlock Block subscribers
// and nDrop Drop subscribers all starting at Beginning. It runs the
// emit loop and every subscriber's drain concurrently — a Block
// subscriber can only make progress if something is reading while Run
// is still writing — and returns once everything has finished.
func runFanout(t *testing.T, ds *synth.Dataset, workers, capacity, nBlock, nDrop int, perturb func() func()) (blocked, dropped []subResult) {
	t.Helper()

	r, err := NewRing(Config{Capacity: capacity, MaxBlobBytes: 256})
	if err != nil {
		t.Fatalf("NewRing() error = %v, want nil", err)
	}

	blockSubs := make([]*Subscriber, nBlock)
	for i := range blockSubs {
		s, err := r.Subscribe(ModeBlock, Beginning())
		if err != nil {
			t.Fatalf("Subscribe(Block) error = %v, want nil", err)
		}
		blockSubs[i] = s
	}
	dropSubs := make([]*Subscriber, nDrop)
	for i := range dropSubs {
		s, err := r.Subscribe(ModeDrop, Beginning())
		if err != nil {
			t.Fatalf("Subscribe(Drop) error = %v, want nil", err)
		}
		dropSubs[i] = s
	}

	m, err := merge.NewConcurrentMerger(openCursors(t, ds), workers)
	if err != nil {
		t.Fatalf("NewConcurrentMerger(%d) error = %v, want nil", workers, err)
	}

	var wg sync.WaitGroup
	blocked = make([]subResult, nBlock)
	dropped = make([]subResult, nDrop)
	for i, s := range blockSubs {
		wg.Add(1)
		go func(i int, s *Subscriber) {
			defer wg.Done()
			var p func()
			if perturb != nil {
				p = perturb()
			}
			blocked[i] = drainSubscriber(t, s, p)
		}(i, s)
	}
	for i, s := range dropSubs {
		wg.Add(1)
		go func(i int, s *Subscriber) {
			defer wg.Done()
			var p func()
			if perturb != nil {
				p = perturb()
			}
			dropped[i] = drainSubscriber(t, s, p)
		}(i, s)
	}

	if err := r.Run(m); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	wg.Wait()

	if err := m.Close(); err != nil {
		t.Errorf("Merger.Close() error = %v, want nil", err)
	}
	return blocked, dropped
}

func assertBlockMatchesMerged(t *testing.T, want wantResult, got subResult) {
	t.Helper()
	if len(got.gaps) != 0 {
		t.Errorf("Block subscriber reported %d gaps, want 0: Block promises no loss", len(got.gaps))
	}
	if got.received != want.count {
		t.Errorf("Block subscriber received %d records, want %d", got.received, want.count)
	}
	if got.hash != want.hash {
		t.Errorf("Block subscriber hash = %s, want %s (the merged stream's own hash)", got.hash, want.hash)
	}
}

func assertDropAccountsForEveryRecord(t *testing.T, want wantResult, got subResult) {
	t.Helper()
	var missed uint64
	var prevLast uint64
	for i, g := range got.gaps {
		if g.Count != g.LastMissedIndex-g.FirstMissedIndex+1 {
			t.Errorf("gap %+v has Count %d, want LastMissedIndex-FirstMissedIndex+1", g, g.Count)
		}
		if i > 0 && g.FirstMissedIndex <= prevLast {
			t.Errorf("gap %+v overlaps or does not follow the previous gap ending at %d", g, prevLast)
		}
		prevLast = g.LastMissedIndex
		missed += g.Count
	}
	if uint64(got.received)+missed != uint64(want.count) {
		t.Errorf("Drop subscriber received %d + missed %d = %d, want %d (the merged stream's total)",
			got.received, missed, uint64(got.received)+missed, want.count)
	}
}

func TestDeterminism(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	want := computeWant(t, ds)

	t.Run("the_dataset_exercises_what_the_hash_claims_to_cover", func(t *testing.T) {
		if want.count != ds.RecordCount() {
			t.Fatalf("replayed %d records, want %d", want.count, ds.RecordCount())
		}
		if want.count < 4*1024 {
			t.Errorf("replayed %d records, want enough to span several 1024-record blocks", want.count)
		}
	})

	// baseline holds the fixed choices every axis-variation subtest
	// below keeps constant while it varies its own one axis.
	const (
		baseWorkers  = 4
		baseCapacity = 64
		baseNBlock   = 2
		baseNDrop    = 2
	)

	runAndAssert := func(t *testing.T, workers, capacity, nBlock, nDrop int) {
		t.Helper()
		blocked, dropped := runFanout(t, ds, workers, capacity, nBlock, nDrop, nil)

		if len(blocked) == 0 && len(dropped) == 0 {
			t.Fatal("this configuration subscribes nobody; it proves nothing")
		}
		for i, got := range blocked {
			t.Run("block_"+strconv.Itoa(i), func(t *testing.T) { assertBlockMatchesMerged(t, want, got) })
		}
		for i, got := range dropped {
			t.Run("drop_"+strconv.Itoa(i), func(t *testing.T) { assertDropAccountsForEveryRecord(t, want, got) })
		}
	}

	t.Run("baseline", func(t *testing.T) {
		runAndAssert(t, baseWorkers, baseCapacity, baseNBlock, baseNDrop)
	})

	t.Run("varying_worker_count", func(t *testing.T) {
		for _, workers := range []int{1, 4, 16, 64} {
			t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
				runAndAssert(t, workers, baseCapacity, baseNBlock, baseNDrop)
			})
		}
	})

	t.Run("varying_gomaxprocs", func(t *testing.T) {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
		for _, procs := range []int{1, 4, 16} {
			t.Run("gomaxprocs_"+strconv.Itoa(procs), func(t *testing.T) {
				runtime.GOMAXPROCS(procs)
				runAndAssert(t, baseWorkers, baseCapacity, baseNBlock, baseNDrop)
			})
		}
	})

	t.Run("varying_ring_capacity", func(t *testing.T) {
		// 2 and 8 force heavy lapping for every Drop subscriber and
		// heavy barrier engagement for every Block subscriber; 8192
		// is larger than the dataset, so neither ever engages at all.
		// Both extremes must still agree with the merged stream.
		for _, capacity := range []int{2, 8, 8192} {
			t.Run("capacity_"+strconv.Itoa(capacity), func(t *testing.T) {
				runAndAssert(t, baseWorkers, capacity, baseNBlock, baseNDrop)
			})
		}
	})

	t.Run("varying_subscriber_mix", func(t *testing.T) {
		mixes := []struct{ nBlock, nDrop int }{
			{1, 0}, {0, 1}, {8, 0}, {0, 8}, {1, 8}, {8, 1},
		}
		for _, mix := range mixes {
			t.Run("block_"+strconv.Itoa(mix.nBlock)+"_drop_"+strconv.Itoa(mix.nDrop), func(t *testing.T) {
				runAndAssert(t, baseWorkers, baseCapacity, mix.nBlock, mix.nDrop)
			})
		}
	})

}

func TestDeterminismChaos(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	want := computeWant(t, ds)

	seeds := []uint64{1, 2, 3, 4}
	for _, seed := range seeds {
		for _, capacity := range []int{2, 64} {
			t.Run("seed_"+strconv.FormatUint(seed, 10)+"_capacity_"+strconv.Itoa(capacity), func(t *testing.T) {
				var nextID int64
				perturb := func() func() {
					// Each subscriber goroutine gets its own
					// math/rand/v2 source, seeded from (seed,
					// subscriber-registration-order) -- never from a
					// goroutine-start-order-derived value -- matching
					// internal/merge's own chaos test convention.
					id := nextID
					nextID++
					rng := rand.New(rand.NewPCG(seed, uint64(id)))
					return func() {
						if rng.Uint64()&1 == 0 {
							runtime.Gosched()
						}
					}
				}

				blocked, dropped := runFanout(t, ds, 8, capacity, 2, 2, perturb)
				for _, got := range blocked {
					assertBlockMatchesMerged(t, want, got)
				}
				for _, got := range dropped {
					assertDropAccountsForEveryRecord(t, want, got)
				}
			})
		}
	}
}
