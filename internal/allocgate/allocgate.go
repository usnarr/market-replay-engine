// Package allocgate is the project's zero-allocation gate: it asserts
// that a benchmarked operation allocates nothing in steady state. The
// claim it enforces is scoped to the in-process path up to the fan-out
// boundary — gRPC's own codec, compressor, and transport allocate
// regardless of anything this project does. See CLAUDE.md's Project
// invariants.
//
// This package imports only the standard library, deliberately: it is
// called from _test.go files in internal/store, internal/merge, and
// internal/fanout, and importing any of those back from here would be
// an import cycle. Keeping the dependency direction one-way, permanently,
// is worth more than sharing a few lines with those packages' own test
// helpers.
package allocgate

import (
	"runtime"
	"testing"
)

// assertRuns is how many times AssertZero calls fn (after each pass's
// own warm-up call) when measuring. High enough that a single stray
// allocation among otherwise-zero calls still shows up as a clear
// nonzero average, not lost to rounding.
const assertRuns = 200

// AssertZero fails b unless fn allocates nothing. It measures twice: the
// allocation count, with the standard library's testing.AllocsPerRun,
// and the allocated bytes, with bytesPerRun. The project's invariant
// names both, and the two passes do not see the same things — see
// bytesPerRun.
//
// An earlier version of this gate ran fn inside its own nested
// testing.Benchmark call, to assert the raw MemAllocs/MemBytes counts
// directly rather than the divided AllocsPerOp value (which can round a
// genuine single allocation down to a reported zero). That version
// deadlocks: testing.(*B).runN serializes every benchmark run in the
// process through one package-level mutex, and a benchmark calling
// testing.Benchmark from inside its own already-running measurement
// tries to acquire that same mutex a second time on the same call
// stack. This was caught by actually running it, not by reasoning about
// it. testing.AllocsPerRun sidesteps the problem entirely — it has
// nothing to do with the benchmark-timing machinery — and gets the same
// protection against the rounding bug for a different reason: a float64
// average of run counts across assertRuns calls cannot round a single
// genuine allocation down to exactly 0.0 the way integer division can.
//
// fn must allocate nothing per call once warm: build every input,
// output buffer, and object under test before calling AssertZero, and
// write results to a package-level concrete sink (see
// internal/store/bench_test.go's sinkRecord/sinkBytes convention).
//
// AssertZero does not touch b's own timer or iteration count. Call it
// once, before or after b's own throughput loop — testing.AllocsPerRun
// runs its own fixed number of iterations independently of b.N, so it
// costs the same whether b.N is 1 or a million, and does not need the
// "-benchtime=Nx multiplies an expensive nested ramp" caution the
// deadlocking version would have needed.
//
// AllocsPerRun forces GOMAXPROCS to 1 for the duration of the
// measurement and restores it afterward (see its own doc comment) — a
// side effect of the standard library function, not of this package,
// and not observable by fn for any operation in this project's hot
// paths, none of which are GOMAXPROCS-sensitive on their own.
//
// AssertZero never runs under the race build tag: the race detector's
// own instrumentation allocates and would fail every gated benchmark
// regardless of the code under test. make bench, which never runs under
// -race, is what actually enforces this gate; make test's -race run
// never executes a Benchmark body at all, since it passes no -bench
// flag. The skip here guards a developer who runs
// `go test -race -bench=.` by hand.
func AssertZero(b *testing.B, fn func()) {
	b.Helper()
	if raceEnabled {
		b.Skip("allocgate: skipped under -race, whose own instrumentation allocates")
	}

	if got := testing.AllocsPerRun(assertRuns, fn); got != 0 {
		b.Fatalf("allocgate: expected zero allocations, got %.4f allocs/op averaged over %d runs", got, assertRuns)
	}
	if got := bytesPerRun(assertRuns, fn); got != 0 {
		b.Fatalf("allocgate: expected zero bytes allocated, got %d bytes over %d runs", got, assertRuns)
	}
}

// bytesPerRun returns how many bytes fn allocates over runs calls. It is
// testing.AllocsPerRun's own measurement shape — warm up once, read
// runtime.MemStats, call fn, read again — with TotalAlloc in place of
// Mallocs, because AllocsPerRun reports a count and has no way to report
// bytes.
//
// It is a second pass over fn rather than a wider reading of the first,
// for two reasons. AllocsPerRun exposes nothing to read bytes from. And
// it pins GOMAXPROCS to 1 for its own measurement, so an allocation that
// appears only when two goroutines genuinely run at once — a contended
// pool miss, a retry path that boxes — never shows up in the count. This
// pass leaves GOMAXPROCS alone, so the benchmark's real parallelism is
// what fn runs under.
//
// The GC call settles allocations already pending when the pass starts,
// including finalizers queued by the benchmark's own setup. There is
// deliberately no second GC before the final reading: a collection there
// could run a finalizer inside the measured window and charge its
// allocation to fn, and ReadMemStats already flushes each P's allocation
// cache, so a second GC would buy nothing and add noise.
//
// TotalAlloc counts the whole process, so another goroutine allocating
// during the window would be charged to fn. Every call site in this
// project calls AssertZero with no other goroutine of its own running,
// and AllocsPerRun's Mallocs reading has carried exactly the same
// exposure since this gate was written.
func bytesPerRun(runs int, fn func()) uint64 {
	fn()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
