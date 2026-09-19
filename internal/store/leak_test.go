package store

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// The leak detector is test-only, and deliberately so. Unmapping from a
// finalizer would tie the moment a file stops being mapped to GC timing,
// which is nondeterministic — and on Windows that would make file
// deletion nondeterministic too, which is a Windows-only intermittent CI
// failure waiting to happen. Reader.Close is the only close path.

// leakTracker records Readers that were collected without being closed.
type leakTracker struct {
	mu     sync.Mutex
	leaked []string
}

func (lt *leakTracker) report(where string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.leaked = append(lt.leaked, where)
}

func (lt *leakTracker) count() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return len(lt.leaked)
}

// track arms the detector on r. The finalizer closure captures the
// tracker and the call site, never r itself: a finalizer that held its
// own object would keep it alive forever and never run.
func track(lt *leakTracker, r *Reader) {
	_, file, line, _ := runtime.Caller(1)
	where := fmt.Sprintf("%s:%d", file, line)
	runtime.SetFinalizer(r, func(collected *Reader) {
		if !collected.closed {
			lt.report(where)
		}
	})
}

// collect runs enough GC cycles for any pending finalizer to have run.
// It yields between cycles rather than sleeping: time.Sleep is banned
// repository-wide, and yielding is what lets the finalizer goroutine
// make progress anyway.
func collect(lt *leakTracker, want int) {
	for i := 0; i < 100 && lt.count() < want; i++ {
		runtime.GC()
		runtime.Gosched()
	}
}

func TestReaderLeakDetector(t *testing.T) {
	f := writeFixture(t, 2)

	t.Run("a_reader_that_is_never_closed_is_reported", func(t *testing.T) {
		// Proves the detector works. Without this case a silent detector
		// and a leak-free package look identical.
		lt := &leakTracker{}
		func() {
			r, err := Open(f.path)
			if err != nil {
				t.Fatalf("Open() error = %v, want nil", err)
			}
			track(lt, r)
			_ = r.RecordAt(0)
		}()

		collect(lt, 1)

		if lt.count() != 1 {
			t.Errorf("leaked = %d, want 1 — the detector did not fire", lt.count())
		}
	})

	t.Run("a_closed_reader_is_not_reported", func(t *testing.T) {
		lt := &leakTracker{}
		func() {
			r, err := Open(f.path)
			if err != nil {
				t.Fatalf("Open() error = %v, want nil", err)
			}
			track(lt, r)
			if err := r.Close(); err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}
		}()

		collect(lt, 1)

		if n := lt.count(); n != 0 {
			t.Errorf("leaked = %d, want 0: %v", n, lt.leaked)
		}
	})

	t.Run("every_reader_this_package_opens_in_tests_is_closed", func(t *testing.T) {
		// The safety net itself: open one of each shape this package's
		// own tests use, close them all, and assert nothing is reported.
		lt := &leakTracker{}
		func() {
			paths := []string{f.path}
			w, empty := newTestWriter(t, 4)
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}
			paths = append(paths, empty)

			for _, p := range paths {
				r, err := Open(p)
				if err != nil {
					t.Fatalf("Open(%s) error = %v, want nil", p, err)
				}
				track(lt, r)
				if err := r.Close(); err != nil {
					t.Fatalf("Close() error = %v, want nil", err)
				}
			}
		}()

		collect(lt, 1)

		if n := lt.count(); n != 0 {
			t.Errorf("leaked = %d, want 0: %v", n, lt.leaked)
		}
	})
}
