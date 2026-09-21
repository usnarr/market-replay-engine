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

import "testing"

// assertRuns is how many times AssertZero calls fn (after
// testing.AllocsPerRun's own internal warm-up call) when computing the
// average allocation count. High enough that a single stray allocation
// among otherwise-zero calls still shows up as a clear nonzero average,
// not lost to rounding.
const assertRuns = 200

// AssertZero fails b unless fn allocates nothing, measured with the
// standard library's testing.AllocsPerRun.
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
}
