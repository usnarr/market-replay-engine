package fanout

import (
	"encoding/hex"
	"testing"

	"replay/internal/clock"
	"replay/internal/merge"
	"replay/internal/store"
	"replay/internal/synth"
)

// runDigest replays ds into a fresh ring through RunDigest, with the
// given pacer and hasher, and returns how many records the ring
// received. No subscriber is attached: the digest must come from the
// emit loop itself, not from what a subscriber is re-delivered.
func runDigest(t *testing.T, ds *synth.Dataset, p *Pacer, h *store.CanonicalHasher) uint64 {
	t.Helper()

	r, err := NewRing(Config{Capacity: 64, MaxBlobBytes: 256})
	if err != nil {
		t.Fatalf("NewRing() error = %v, want nil", err)
	}
	m, err := merge.NewMerger(openCursors(t, ds))
	if err != nil {
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}

	if err := r.RunDigest(m, p, h); err != nil {
		t.Fatalf("RunDigest() error = %v, want nil", err)
	}

	if err := m.Close(); err != nil {
		t.Errorf("Merger.Close() error = %v, want nil", err)
	}
	return r.WriteIndex()
}

// digestPacer is a Pacer over SimClock, which has no autonomous time, so
// no record ever actually waits — the pacing path is exercised without
// the test depending on real scheduling. See docs/clock.md.
func digestPacer(t *testing.T, num, den int64) *Pacer {
	t.Helper()

	s, err := NewSpeed(num, den)
	if err != nil {
		t.Fatalf("NewSpeed(%d, %d) error = %v, want nil", num, den, err)
	}
	return NewPacer(&clock.SimClock{}, s)
}

func TestRunDigest(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	want := computeWant(t, ds)

	t.Run("an_unpaced_run_digests_the_merged_stream", func(t *testing.T) {
		h := store.NewCanonicalHasher(store.CanonicalCRC32C)

		written := runDigest(t, ds, nil, h)

		if written != uint64(want.count) {
			t.Errorf("RunDigest() wrote %d records, want %d", written, want.count)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != want.hash {
			t.Errorf("RunDigest() hash = %s, want %s (the merged stream's own hash)", got, want.hash)
		}
	})

	t.Run("a_paced_run_digests_the_same_stream", func(t *testing.T) {
		h := store.NewCanonicalHasher(store.CanonicalCRC32C)

		written := runDigest(t, ds, digestPacer(t, 10000, 1), h)

		if written != uint64(want.count) {
			t.Errorf("RunDigest() wrote %d records, want %d", written, want.count)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != want.hash {
			t.Errorf("paced RunDigest() hash = %s, want %s (pacing never changes content)", got, want.hash)
		}
	})

	t.Run("a_nil_hasher_still_drains_the_whole_stream", func(t *testing.T) {
		written := runDigest(t, ds, nil, nil)

		if written != uint64(want.count) {
			t.Errorf("RunDigest(nil hasher) wrote %d records, want %d", written, want.count)
		}
	})
}
