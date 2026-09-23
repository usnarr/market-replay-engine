package bench

import (
	"testing"

	"replay/internal/fanout"
	"replay/internal/synth"
)

// benchShape is the load harness's own dataset: 400,000 records over four
// venues and six files. It is much larger than internal/synth's standard
// determinism fixture, so that one replay's fixed setup — opening six
// files, building the tree, starting four subscriber goroutines — stays a
// small share of what is measured. The venues still end at different
// times, so the tree's sentinel handling is exercised rather than
// stepped around.
var benchShape = [][]int{{100_000, 50_000}, {120_000, 30_000}, {80_000}, {20_000}}

// BenchmarkEndToEndReplay measures one whole replay through the whole
// in-process stack, from the memory-mapped files to the last record every
// subscriber received. One iteration is one complete pass over the
// dataset, so ns/op is the cost of a replay and the reported records/s is
// the end-to-end replay rate.
//
// This benchmark is deliberately not allocation-gated. The project's
// zero-allocation invariant is scoped to the in-process path up to the
// fan-out boundary (see CLAUDE.md), and this measurement spans past it:
// it opens files, builds a merge tree, starts one goroutine per
// subscriber, and delivers to each of them. Gating it would assert
// something the invariant never claimed. internal/store, internal/merge
// and internal/fanout carry the gate on the per-record paths where the
// claim does apply. This follows docs/book.md's "Off the hot path: no
// allocation gate" precedent: state the reason, never silently omit the
// gate.
func BenchmarkEndToEndReplay(b *testing.B) {
	dir := b.TempDir()
	ds := synth.Build(b, dir, benchShape)

	paced, err := fanout.NewSpeed(100, 1)
	if err != nil {
		b.Fatalf("NewSpeed(100, 1) error = %v, want nil", err)
	}

	cases := []struct {
		name  string
		speed fanout.Speed
	}{
		// The zero Speed builds no pacer at all, so this case is the
		// stack's own maximum rate. The paced case is the same replay
		// with fanout.Pacer in the emit loop.
		{"unpaced_max_rate", fanout.Speed{}},
		{"paced_100x", paced},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg := Config{
				Dir:              dir,
				Workers:          4,
				Capacity:         1024,
				MaxBlobBytes:     256,
				BlockSubscribers: 2,
				DropSubscribers:  2,
				Speed:            tc.speed,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := Run(cfg)
				if err != nil {
					b.Fatalf("Run() error = %v, want nil", err)
				}
				// A replay that ended early would otherwise report a
				// throughput it never reached.
				if res.Emitted != uint64(ds.RecordCount()) {
					b.Fatalf("emitted %d records, want %d", res.Emitted, ds.RecordCount())
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(b.N)*float64(ds.RecordCount())/b.Elapsed().Seconds(), "records/s")
		})
	}
}
