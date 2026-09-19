package clock

import "sync"

// SimClock is a Clock for tests and the determinism suite. It never sleeps
// in real time: SleepUntil returns immediately without advancing the
// clock, and simulated time only moves when a caller calls Advance. See
// docs/clock.md for the content/timing separation invariant this exists to
// support.
//
// Pending timers live in a sorted slice, not a heap and not a map:
// container/heap is banned repository-wide, and a map would put unordered
// iteration into a package other code might copy from as a pattern. Timer
// counts in tests are small, so a linear insert into a sorted slice is
// simpler than a heap and fast enough.
type SimClock struct {
	mu     sync.Mutex
	now    int64
	nextID uint64
	timers []*simTimer // sorted by (deadline, id), ascending
}

// simTimer is SimClock's Timer implementation. Its registration id is
// assigned once, by NewTimer, and never changes across Reset calls — this
// is what lets Advance break a tie between two identical deadlines by
// creation order instead of by goroutine scheduling.
type simTimer struct {
	id       uint64
	deadline int64
	c        chan int64
	sc       *SimClock
}

// Now returns the current simulated time. It changes only via Advance.
func (sc *SimClock) Now() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.now
}

// SleepUntil returns immediately, regardless of deadline. It never
// advances simulated time as a side effect: if it did, concurrent callers
// would move now in goroutine-scheduling order, which is exactly the
// nondeterminism this package exists to remove. Advance is the only way
// now moves.
func (sc *SimClock) SleepUntil(deadline int64) {}

// NewTimer creates a timer that fires when simulated time reaches
// now + d. A non-positive d fires immediately, before NewTimer returns.
func (sc *SimClock) NewTimer(d int64) Timer {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	t := &simTimer{id: sc.nextID, deadline: sc.now + d, c: make(chan int64, 1), sc: sc}
	sc.nextID++
	sc.insertLocked(t)
	sc.fireDueLocked()
	return t
}

// Advance moves simulated time forward by d, then fires every timer whose
// deadline is now due, in ascending (deadline, registration id) order.
func (sc *SimClock) Advance(d int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.now += d
	sc.fireDueLocked()
}

// insertLocked inserts t into sc.timers, keeping it sorted by (deadline,
// id) ascending. Callers hold sc.mu.
func (sc *SimClock) insertLocked(t *simTimer) {
	i := 0
	for i < len(sc.timers) {
		other := sc.timers[i]
		if other.deadline > t.deadline || (other.deadline == t.deadline && other.id > t.id) {
			break
		}
		i++
	}
	sc.timers = append(sc.timers, nil)
	copy(sc.timers[i+1:], sc.timers[i:])
	sc.timers[i] = t
}

// fireDueLocked sends each due timer's own deadline on its channel and
// removes it from sc.timers. sc.timers is kept sorted, so due timers are
// always the leading prefix. Callers hold sc.mu.
func (sc *SimClock) fireDueLocked() {
	i := 0
	for ; i < len(sc.timers); i++ {
		t := sc.timers[i]
		if t.deadline > sc.now {
			break
		}
		select {
		case t.c <- t.deadline:
		default:
		}
	}
	sc.timers = sc.timers[i:]
}

// removeLocked removes t from sc.timers if present, reporting whether it
// was found. Callers hold sc.mu.
func (sc *SimClock) removeLocked(t *simTimer) bool {
	for i, other := range sc.timers {
		if other == t {
			sc.timers = append(sc.timers[:i], sc.timers[i+1:]...)
			return true
		}
	}
	return false
}

func (t *simTimer) C() <-chan int64 {
	return t.c
}

// Reset reschedules t to fire at now + d, keeping its original
// registration id so its tie-break position relative to other timers is
// unaffected by the reschedule. It reports whether t was still pending
// before the call; like time.Timer.Reset, that can be false for a timer
// that already fired, and t is still rescheduled in that case.
func (t *simTimer) Reset(d int64) bool {
	sc := t.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()

	active := sc.removeLocked(t)
	select {
	case <-t.c:
	default:
	}
	t.deadline = sc.now + d
	sc.insertLocked(t)
	sc.fireDueLocked()
	return active
}

// Stop prevents t from firing. It reports whether t was still pending
// before the call.
func (t *simTimer) Stop() bool {
	sc := t.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()

	active := sc.removeLocked(t)
	select {
	case <-t.c:
	default:
	}
	return active
}
