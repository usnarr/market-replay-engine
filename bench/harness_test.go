package bench

import (
	"encoding/hex"
	"errors"
	"strconv"
	"testing"

	"replay/internal/fanout"
	"replay/internal/merge"
	"replay/internal/store"
	"replay/internal/synth"
)

// mergedStream is a dataset's canonical hash and record count, computed
// straight from internal/synth's own file list with no ring and no
// harness in the way. It is the ground truth every case below is checked
// against, and it is built from ds.Files in ds.Files' order, so it also
// pins down the file order the harness has to rediscover from content
// alone.
func mergedStream(t *testing.T, ds *synth.Dataset) (hash string, count int) {
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

	m, err := merge.NewMerger(cursors)
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	}()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		h.Write(ev.Record, ev.Blob)
		count++
	}
	return hex.EncodeToString(h.Sum(nil)), count
}

// runHarness replays cfg and fails the test if it returns an error.
func runHarness(t *testing.T, cfg Config) Result {
	t.Helper()

	res, err := Run(cfg)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	return res
}

// assertDelivered checks one replay against the merged stream: every
// Block subscriber received it exactly, and every Drop subscriber
// accounts for every record it did not receive.
func assertDelivered(t *testing.T, res Result, wantHash string, wantCount int) {
	t.Helper()

	if res.Records != wantCount {
		t.Errorf("headers count %d records, want %d", res.Records, wantCount)
	}
	if res.Emitted != uint64(wantCount) {
		t.Errorf("emitted %d records, want %d", res.Emitted, wantCount)
	}

	for i, got := range res.Block {
		if got.Mode != fanout.ModeBlock {
			t.Errorf("block subscriber %d has mode %v, want ModeBlock", i, got.Mode)
		}
		if got.Gaps != 0 {
			t.Errorf("block subscriber %d reported %d gaps, want 0: Block promises no loss", i, got.Gaps)
		}
		if got.Received != uint64(wantCount) {
			t.Errorf("block subscriber %d received %d records, want %d", i, got.Received, wantCount)
		}
		if got.Hash != wantHash {
			t.Errorf("block subscriber %d hash = %s, want %s (the merged stream's own hash)", i, got.Hash, wantHash)
		}
	}
	for i, got := range res.Drop {
		if got.Received+got.Missed != uint64(wantCount) {
			t.Errorf("drop subscriber %d received %d + missed %d = %d, want %d (the merged stream's total)",
				i, got.Received, got.Missed, got.Received+got.Missed, wantCount)
		}
	}
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	wantHash, wantCount := mergedStream(t, ds)

	// base is what every case below starts from, varying one field at a
	// time.
	base := Config{
		Dir:              dir,
		Workers:          4,
		Capacity:         64,
		MaxBlobBytes:     256,
		BlockSubscribers: 2,
		DropSubscribers:  2,
		Hash:             true,
	}

	t.Run("the_dataset_exercises_what_the_hash_claims_to_cover", func(t *testing.T) {
		if wantCount != ds.RecordCount() {
			t.Fatalf("merged %d records, want %d", wantCount, ds.RecordCount())
		}
		if wantCount < 4*1024 {
			t.Errorf("merged %d records, want enough to span several 1024-record blocks", wantCount)
		}
	})

	t.Run("baseline", func(t *testing.T) {
		assertDelivered(t, runHarness(t, base), wantHash, wantCount)
	})

	t.Run("varying_worker_count", func(t *testing.T) {
		// 0 is the inline merger, which starts no reader goroutine at
		// all; the rest decode through a worker pool. None of them may
		// change the stream.
		for _, workers := range []int{0, 1, 4, 16} {
			t.Run("workers_"+strconv.Itoa(workers), func(t *testing.T) {
				cfg := base
				cfg.Workers = workers

				assertDelivered(t, runHarness(t, cfg), wantHash, wantCount)
			})
		}
	})

	t.Run("varying_ring_capacity", func(t *testing.T) {
		// 2 laps every Drop subscriber constantly and parks the writer at
		// the Block barrier for most of the run; 8192 is larger than the
		// dataset, so neither ever engages.
		for _, capacity := range []int{2, 8192} {
			t.Run("capacity_"+strconv.Itoa(capacity), func(t *testing.T) {
				cfg := base
				cfg.Capacity = capacity

				assertDelivered(t, runHarness(t, cfg), wantHash, wantCount)
			})
		}
	})

	t.Run("varying_subscriber_mix", func(t *testing.T) {
		mixes := []struct{ nBlock, nDrop int }{{1, 0}, {0, 1}, {4, 4}}
		for _, mix := range mixes {
			t.Run("block_"+strconv.Itoa(mix.nBlock)+"_drop_"+strconv.Itoa(mix.nDrop), func(t *testing.T) {
				cfg := base
				cfg.BlockSubscribers = mix.nBlock
				cfg.DropSubscribers = mix.nDrop

				assertDelivered(t, runHarness(t, cfg), wantHash, wantCount)
			})
		}
	})

	t.Run("pacing_does_not_change_the_stream", func(t *testing.T) {
		speed, err := fanout.NewSpeed(100, 1)
		if err != nil {
			t.Fatalf("NewSpeed(100, 1) error = %v, want nil", err)
		}
		cfg := base
		cfg.Speed = speed

		assertDelivered(t, runHarness(t, cfg), wantHash, wantCount)
	})

	t.Run("partitions_are_grouped_by_the_venue_id_in_each_header", func(t *testing.T) {
		wantVenues := 0
		for _, files := range ds.Files {
			if len(files) > 0 {
				wantVenues++
			}
		}

		res := runHarness(t, base)

		// The standard shape's last venue has no files at all, so it
		// contributes no partition here, and the merged stream is the
		// same either way: an empty partition emits nothing.
		if res.Venues != wantVenues {
			t.Errorf("found %d venue partitions, want %d", res.Venues, wantVenues)
		}
	})
}

// TestRunOrdersFilesByContent uses an eleven-day venue, whose file names
// sort day0, day1, day10, day2 ... while its records run day0, day1,
// day2 ... day10. A harness that ordered a partition's files by name
// would hand merge.NewCursor a partition whose keys go backwards at the
// day10 seam, and the cursor would reject it.
func TestRunOrdersFilesByContent(t *testing.T) {
	dir := t.TempDir()
	days := make([]int, 11)
	for i := range days {
		days[i] = 200
	}
	ds := synth.Build(t, dir, [][]int{days})
	wantHash, wantCount := mergedStream(t, ds)

	res := runHarness(t, Config{
		Dir:              dir,
		Workers:          4,
		Capacity:         64,
		MaxBlobBytes:     256,
		BlockSubscribers: 1,
		DropSubscribers:  1,
		Hash:             true,
	})

	assertDelivered(t, res, wantHash, wantCount)
}

func TestRunRejectsADirectoryWithNoVenueFiles(t *testing.T) {
	_, err := Run(Config{Dir: t.TempDir(), Capacity: 64, BlockSubscribers: 1})

	if !errors.Is(err, ErrNoVenues) {
		t.Fatalf("Run() error = %v, want ErrNoVenues", err)
	}
}
