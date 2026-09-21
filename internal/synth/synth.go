// Package synth generates the synthetic multi-venue dataset the
// determinism suites in internal/merge and internal/fanout both replay.
// It exists so the two suites share one fixture generator instead of
// each carrying its own copy that could silently drift apart.
//
// synth imports only internal/store. It is a test-support package,
// imported only from other packages' _test.go files, never from
// production code — hence the otherwise unusual dependency on the
// standard library's testing package from a non-_test.go file.
package synth

import (
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"

	"replay/internal/store"
)

const (
	// Seed fixes the generated dataset. It is part of the fixture, not
	// a source of variation: two runs must build the same bytes.
	Seed = 0x5EED

	// SnapshotEvery is the snapshot epoch cadence, in records per
	// venue. It is not a multiple of the 1024-record block size, so an
	// epoch lands at a different offset in each block.
	SnapshotEvery = 150
)

// Shape gives the record count of every venue's day files, for the
// standard dataset callers build with Build(t, dir, Shape). The counts
// differ on purpose: venues end at different times, one day file is
// empty, one venue is a single short day, and the last venue has no
// files at all. Together they cover every case a k-way merge's sentinel
// handling has to carry.
var Shape = [][]int{
	{1500, 1200, 900},
	{800, 0, 1100},
	{2000, 300},
	{90},
	{},
}

// Dataset is a generated multi-venue dataset on disk, partitioned by
// venue and day — the granularity cmd/convert produces.
type Dataset struct {
	Shape [][]int

	// Venues[v] is the venue ID for Shape[v] and Files[v]. Venue IDs are
	// assigned as v+1, in Shape's order.
	Venues []uint16

	// Files[v] are venue Venues[v]'s day files, in day order.
	Files [][]string
}

// Build writes a dataset with the given shape to dir and returns it.
// Generation is a pure function of Seed and shape: two calls with the
// same shape produce byte-identical files, which is what lets two
// suites replay "the same dataset" without one importing the other's
// test files.
//
// t is testing.TB, not *testing.T, so a benchmark can build the same
// dataset a test would: internal/merge's own hot-path benchmarks need
// the same shape internal/merge and internal/fanout's determinism
// suites already replay, not a third one.
func Build(t testing.TB, dir string, shape [][]int) *Dataset {
	t.Helper()

	ds := &Dataset{
		Shape:  shape,
		Venues: make([]uint16, len(shape)),
		Files:  make([][]string, len(shape)),
	}

	for v, days := range shape {
		venue := uint16(v + 1)
		ds.Venues[v] = venue

		rng := rand.New(rand.NewPCG(Seed, uint64(venue)))
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

				if written%SnapshotEvery == 0 {
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
			ds.Files[v] = append(ds.Files[v], path)
		}
	}
	return ds
}

// Standard writes the standard dataset (Shape) and returns it.
func Standard(t testing.TB, dir string) *Dataset {
	t.Helper()
	return Build(t, dir, Shape)
}

// RecordCount is how many records the dataset holds in total.
func (ds *Dataset) RecordCount() int {
	n := 0
	for _, days := range ds.Shape {
		for _, count := range days {
			n += count
		}
	}
	return n
}
