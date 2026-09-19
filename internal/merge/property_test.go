package merge

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// streamShape describes one family of generated inputs. The shapes below
// cover the distributions that stress different parts of the tree: dense
// timestamp ties across venues, venues whose ranges do not overlap at
// all, empty partitions, and cursor counts that are not powers of two.
type streamShape struct {
	name     string
	k        int
	maxLen   int
	tsSpan   int64 // how far one record's timestamp may advance on the last
	tsStride int64 // how far apart consecutive venues' ranges start
	emptyNth int   // every nth venue is empty; 0 means none are
}

// genStreams builds k independently sorted key streams, one per venue.
// No key is shared between two streams, because venue ids are distinct —
// the same property the real partitioning gives. Within a stream the
// timestamp is non-decreasing and the sequence number always increases,
// so every key strictly increases.
func genStreams(rng *rand.Rand, s streamShape) [][]Key {
	streams := make([][]Key, s.k)
	for v := range streams {
		if s.emptyNth > 0 && v%s.emptyNth == 0 {
			streams[v] = []Key{}
			continue
		}

		n := rng.IntN(s.maxLen + 1)
		ts := int64(v) * s.tsStride
		keys := make([]Key, 0, n)
		for i := 0; i < n; i++ {
			ts += rng.Int64N(s.tsSpan + 1)
			keys = append(keys, Key{
				ExchangeTs:     ts,
				VenueID:        uint16(v + 1),
				SequenceNumber: uint64(i),
				InstrumentID:   rng.Uint32N(4),
			})
		}
		streams[v] = keys
	}
	return streams
}

func TestMergeEqualsTheSortedConcatenation(t *testing.T) {
	shapes := []streamShape{
		{name: "a_single_venue", k: 1, maxLen: 200, tsSpan: 3},
		{name: "two_venues_fully_overlapping", k: 2, maxLen: 200, tsSpan: 3},
		{name: "many_short_venues", k: 17, maxLen: 5, tsSpan: 3},
		{name: "a_few_long_venues", k: 3, maxLen: 400, tsSpan: 3},
		{name: "every_record_in_a_venue_shares_one_timestamp", k: 5, maxLen: 60, tsSpan: 0, tsStride: 1},
		{name: "venues_whose_time_ranges_do_not_overlap", k: 6, maxLen: 40, tsSpan: 2, tsStride: 10000},
		{name: "half_the_venues_empty", k: 8, maxLen: 50, tsSpan: 3, emptyNth: 2},
		{name: "a_cursor_count_one_past_a_power_of_two", k: 9, maxLen: 30, tsSpan: 5},
		{name: "a_cursor_count_one_below_a_power_of_two", k: 15, maxLen: 30, tsSpan: 5},
	}
	seeds := []uint64{1, 7, 99, 4242}

	for _, shape := range shapes {
		for _, seed := range seeds {
			t.Run(shape.name+"_seed_"+strconv.FormatUint(seed, 10), func(t *testing.T) {
				streams := genStreams(rand.New(rand.NewPCG(seed, uint64(shape.k))), shape)
				want := make([]Key, 0)
				for _, s := range streams {
					want = append(want, s...)
				}
				slices.SortFunc(want, compareKey)

				got := drainTree(t, streams)

				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
				}
				for i := 1; i < len(got); i++ {
					if compareKey(got[i-1], got[i]) >= 0 {
						t.Fatalf("keys %d and %d do not increase: %v then %v", i-1, i, got[i-1], got[i])
					}
				}
			})
		}
	}
}

func TestMergePreservesEachVenuesOwnOrder(t *testing.T) {
	// The sorted-concatenation oracle above would still pass if the merge
	// swapped two records within one venue and the oracle sorted them back
	// into place. Check the per-venue subsequence directly instead.
	shape := streamShape{name: "mixed", k: 11, maxLen: 80, tsSpan: 4, tsStride: 3}

	for _, seed := range []uint64{1, 2, 3, 4, 5} {
		t.Run("seed_"+strconv.FormatUint(seed, 10), func(t *testing.T) {
			streams := genStreams(rand.New(rand.NewPCG(seed, 0)), shape)

			got := drainTree(t, streams)

			for v, want := range streams {
				venue := uint16(v + 1)
				subsequence := make([]Key, 0, len(want))
				for _, key := range got {
					if key.VenueID == venue {
						subsequence = append(subsequence, key)
					}
				}
				if diff := cmp.Diff(want, subsequence, cmpEmptyKeySlices); diff != "" {
					t.Errorf("venue %d subsequence mismatch (-want +got):\n%s", venue, diff)
				}
			}
		})
	}
}

// cmpEmptyKeySlices treats a nil and an empty key slice as equal. A venue
// with no records is generated as an empty slice and collected as one, so
// the distinction carries no meaning here.
var cmpEmptyKeySlices = cmp.FilterValues(
	func(a, b []Key) bool { return len(a) == 0 && len(b) == 0 },
	cmp.Comparer(func(a, b []Key) bool { return true }),
)
