package allocgate

import "testing"

// sinkBytes is a package-level write target, so a closure that
// allocates cannot have that allocation optimised away as dead: a
// discarded local (`_ = x`) is not a reliable way to keep make() live.
var sinkBytes []byte

func TestAssertZero(t *testing.T) {
	t.Run("passes_for_a_function_that_does_not_allocate", func(t *testing.T) {
		x := 0
		res := testing.Benchmark(func(b *testing.B) {
			AssertZero(b, func() { x++ })
		})
		_ = x

		if res.N == 0 {
			t.Fatal("testing.Benchmark(...) ran 0 iterations: AssertZero must have failed unexpectedly")
		}
	})

	t.Run("fails_for_a_function_that_allocates", func(t *testing.T) {
		res := testing.Benchmark(func(b *testing.B) {
			AssertZero(b, func() { sinkBytes = make([]byte, 64) })
		})

		// AssertZero calls b.Fatalf when fn allocates. testing.Benchmark
		// surfaces a failed run only as a zero-value BenchmarkResult —
		// there is no error return to check directly.
		if res.N != 0 {
			t.Errorf("testing.Benchmark(...) ran %d iterations, want 0: AssertZero should have failed", res.N)
		}
	})
}
