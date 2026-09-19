package clock

import (
	"sync"
	"testing"
)

// TestSimClockConcurrentUse exercises NewTimer, Advance, Now, Reset, and
// Stop from many goroutines at once. It makes no ordering assertions of
// its own -- SimClock's ordering guarantees are already covered by
// TestSimClock's single-goroutine table -- its only job is to give
// `go test -race` shared state to find a data race in.
func TestSimClockConcurrentUse(t *testing.T) {
	const goroutines = 32
	const timersPerGoroutine = 50

	sc := &SimClock{}
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < timersPerGoroutine; i++ {
				tm := sc.NewTimer(int64(i))
				sc.Now()
				tm.Reset(int64(i + 1))
				sc.Advance(1)
				select {
				case <-tm.C():
				default:
				}
				tm.Stop()
			}
		}(g)
	}
	wg.Wait()
}
