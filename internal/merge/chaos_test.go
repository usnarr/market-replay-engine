package merge

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// TestDeterminismChaos is the chaos variant of TestDeterminism. Every
// venue's owner goroutine gets its own runtime.Gosched perturbation,
// gated by a math/rand/v2 source seeded from (chaosSeed, venueID) —
// never from a worker index or start order. If the merged stream ever
// depended on how these goroutines happen to interleave, this is what
// would catch it: TestDeterminism's GOMAXPROCS and worker-count
// variants only change how much concurrency exists, never where in a
// goroutine's own loop it happens to be paused.
func TestDeterminismChaos(t *testing.T) {
	ds := buildSynthDataset(t)
	want := replaySynth(t, ds, ds.venues)

	chaosSeeds := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	for _, seed := range chaosSeeds {
		for _, workers := range workerCounts {
			t.Run("seed_"+strconv.FormatUint(seed, 10)+"_workers_"+strconv.Itoa(workers), func(t *testing.T) {
				m, err := newMergerChaos(ds.openCursors(t, ds.venues), workers, seed)
				if err != nil {
					t.Fatalf("newMergerChaos() error = %v, want nil", err)
				}

				got := drainReplay(t, ds, m)

				assertSameReplay(t, want, got)
			})
		}
	}
}

// TestDeterminismChaosAcrossVenueOrder pins the chaos perturbation to the
// venue id, not the leaf index: rotating which leaf a venue lands on
// must not change which perturbation sequence it gets, and the hash
// must still match.
func TestDeterminismChaosAcrossVenueOrder(t *testing.T) {
	ds := buildSynthDataset(t)
	want := replaySynth(t, ds, ds.venues)

	for i := 1; i < len(ds.venues); i++ {
		t.Run("rotated_by_"+strconv.Itoa(i), func(t *testing.T) {
			order := append(append([]uint16{}, ds.venues[i:]...), ds.venues[:i]...)

			m, err := newMergerChaos(ds.openCursors(t, order), 16, 0x5EED)
			if err != nil {
				t.Fatalf("newMergerChaos() error = %v, want nil", err)
			}

			got := drainReplay(t, ds, m)

			assertSameReplay(t, want, got)
		})
	}
}

func TestChaosRandIsSeededByVenueNotByCallOrder(t *testing.T) {
	// Two venues built from the same chaos seed must draw from different
	// sequences — otherwise every venue would pause at the same points
	// in its own loop and the chaos test would exercise far less
	// interleaving than it looks like it does.
	const draws = 64

	a := drawUint64s(newChaosRand(1, 1), draws)
	b := drawUint64s(newChaosRand(1, 2), draws)

	if a == b {
		t.Error("venue 1 and venue 2 drew the same sequence from the same chaos seed")
	}
}

func TestChaosRandIsReproducible(t *testing.T) {
	// The chaos test's own seeds have to reproduce a failure, or a rare
	// interleaving bug they catch once could never be caught again.
	const draws = 64

	first := drawUint64s(newChaosRand(42, 7), draws)
	second := drawUint64s(newChaosRand(42, 7), draws)

	if first != second {
		t.Error("the same (seed, venueID) pair produced two different sequences")
	}
}

// drawUint64s draws n values and folds them into one comparable value,
// so two sequences can be compared with ==.
func drawUint64s(rng *rand.Rand, n int) uint64 {
	var acc uint64
	for i := 0; i < n; i++ {
		acc = acc*31 + rng.Uint64()
	}
	return acc
}
