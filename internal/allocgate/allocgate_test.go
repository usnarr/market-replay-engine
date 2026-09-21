package allocgate

import (
	"runtime"
	"testing"
)

// Sinks are package-level write targets, so a closure that allocates
// cannot have that allocation optimised away as dead: a discarded local
// (`_ = x`) is not a reliable way to keep make() live.
var (
	sinkBytes []byte
	sinkInt   int
)

func TestBytesPerRun(t *testing.T) {
	const size = 64

	tests := []struct {
		name        string
		fn          func()
		wantAtLeast uint64
	}{
		{name: "a_function_that_allocates_nothing", fn: func() { sinkInt++ }},
		{
			name:        "a_function_that_allocates_a_fixed_size",
			fn:          func() { sinkBytes = make([]byte, size) },
			wantAtLeast: size * assertRuns,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bytesPerRun(assertRuns, tt.fn)

			if got < tt.wantAtLeast {
				t.Errorf("bytesPerRun(%d, fn) = %d, want at least %d", assertRuns, got, tt.wantAtLeast)
			}
			if tt.wantAtLeast == 0 && got != 0 {
				t.Errorf("bytesPerRun(%d, fn) = %d, want 0", assertRuns, got)
			}
		})
	}

	t.Run("measures_at_the_ambient_gomaxprocs", func(t *testing.T) {
		// This is what makes the byte pass more than a second opinion on
		// the count pass: testing.AllocsPerRun pins GOMAXPROCS to 1 for
		// its own measurement, so an allocation that appears only when
		// two goroutines genuinely run at once cannot show up there.
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
		const want = 4
		runtime.GOMAXPROCS(want)
		got := 0

		bytesPerRun(2, func() { got = runtime.GOMAXPROCS(0) })

		if got != want {
			t.Errorf("bytesPerRun ran fn at GOMAXPROCS %d, want %d", got, want)
		}
	})
}

func TestAssertZero(t *testing.T) {
	t.Run("passes_for_a_function_that_does_not_allocate", func(t *testing.T) {
		x := 0
		res := testing.Benchmark(func(b *testing.B) {
			AssertZero(b, func() { x++ })
		})
		_ = x

		// Under -race, AssertZero's own doc comment says it skips rather
		// than measures, so res.N == 0 here is the correct outcome, not
		// the failure the non-race branch below checks for.
		if raceEnabled {
			if res.N != 0 {
				t.Fatalf("testing.Benchmark(...) ran %d iterations under -race, want 0: AssertZero should have skipped", res.N)
			}
			return
		}
		if res.N == 0 {
			t.Fatal("testing.Benchmark(...) ran 0 iterations: AssertZero must have failed unexpectedly")
		}
	})

	t.Run("fails_for_a_function_that_allocates", func(t *testing.T) {
		res := testing.Benchmark(func(b *testing.B) {
			AssertZero(b, func() { sinkBytes = make([]byte, 64) })
		})

		// Under -race, AssertZero skips before it ever measures, so
		// res.N == 0 here would hold regardless of whether the
		// allocation was detected. That would pass this assertion for
		// the wrong reason, so skip it instead.
		if raceEnabled {
			t.Skip("allocgate: skipped under -race, see AssertZero's own -race skip")
		}

		// AssertZero calls b.Fatalf when fn allocates. testing.Benchmark
		// surfaces a failed run only as a zero-value BenchmarkResult —
		// there is no error return to check directly.
		if res.N != 0 {
			t.Errorf("testing.Benchmark(...) ran %d iterations, want 0: AssertZero should have failed", res.N)
		}
	})
}
