package merge

import (
	"strconv"
	"testing"

	"replay/internal/store"
	"replay/internal/synth"
)

// Sinks stop the compiler from proving a benchmarked result is unused.
var (
	sinkInt int
	sinkKey Key
)

// BenchmarkLoserTreePop is the gate target plans/13-bench-and-profiles.md
// names explicitly: the loser tree's own per-pop cost, isolated from
// decode and I/O. Each iteration pops the current winner and feeds its
// cursor a strictly larger key than any fed so far, which keeps every
// Advance call's ordering check satisfied — see LoserTree.Advance's own
// doc comment — without ever exhausting the tree mid-benchmark.
func BenchmarkLoserTreePop(b *testing.B) {
	for _, k := range []int{2, 4, 8, 16, 64} {
		b.Run("k_"+strconv.Itoa(k), func(b *testing.B) {
			tree := NewLoserTree(k)
			for i := 0; i < k; i++ {
				tree.SetKey(i, mk(int64(i), 1, 0, 0))
			}
			tree.Init()
			next := int64(k)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, _ := tree.Winner()
				if err := tree.Advance(mk(next, 1, 0, 0)); err != nil {
					b.Fatalf("Advance() error = %v, want nil", err)
				}
				next++
				sinkInt = w
			}
		})
	}
}

// BenchmarkCompareKey is the innermost operation in the merge stage: it
// runs on every tree comparison and every ordering check.
func BenchmarkCompareKey(b *testing.B) {
	a := mk(1000, 1, 500, 7)
	c := mk(1000, 1, 501, 7) // differs only in the third field, the most
	// expensive case: the first two fields must both compare equal
	// before this one is even reached.

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkInt = compareKey(a, c)
	}
}

// BenchmarkMergerNext replays the standard synth dataset end to end, at
// each worker count make determinism itself varies. The merger and its
// cursors are rebuilt outside the timer whenever the dataset is
// exhausted, so this measures steady-state per-record throughput, not
// one pass through a fixed-size dataset amortized over a much larger
// b.N.
func BenchmarkMergerNext(b *testing.B) {
	dir := b.TempDir()
	ds := synth.Standard(b, dir)

	for _, workers := range []int{1, 4, 16} {
		b.Run("workers_"+strconv.Itoa(workers), func(b *testing.B) {
			newMerger := func() *Merger {
				cursors := openCursorsForBench(b, ds)
				m, err := NewConcurrentMerger(cursors, workers)
				if err != nil {
					b.Fatalf("NewConcurrentMerger(%d) error = %v, want nil", workers, err)
				}
				return m
			}

			m := newMerger()
			b.Cleanup(func() { _ = m.Close() })

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ev, ok, err := m.Next()
				if err != nil {
					b.Fatalf("Next() error = %v, want nil", err)
				}
				if !ok {
					b.StopTimer()
					_ = m.Close()
					m = newMerger()
					b.StartTimer()
					continue
				}
				sinkKey = keyOf(ev.Record)
			}
		})
	}
}

// openCursorsForBench mirrors synthDataset.openCursors, for a benchmark
// that only has a *testing.B (synthDataset's own version takes a
// *testing.T, matching every other caller in this package's tests).
func openCursorsForBench(b *testing.B, ds *synth.Dataset) []*Cursor {
	b.Helper()

	cursors := make([]*Cursor, 0, len(ds.Venues))
	for v, venue := range ds.Venues {
		readers := make([]*store.Reader, 0, len(ds.Files[v]))
		for _, path := range ds.Files[v] {
			r, err := store.Open(path)
			if err != nil {
				b.Fatalf("Open(%s) error = %v, want nil", path, err)
			}
			readers = append(readers, r)
		}
		c, err := NewCursor(venue, readers)
		if err != nil {
			b.Fatalf("NewCursor(%d) error = %v, want nil", venue, err)
		}
		cursors = append(cursors, c)
	}
	return cursors
}
