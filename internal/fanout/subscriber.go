package fanout

import (
	"runtime"
	"sync/atomic"

	"replay/internal/store"
)

// BackpressureMode selects what the writer does when a subscriber falls
// behind. It has no meaningful zero value: Subscribe rejects ModeUnset,
// which is what makes "Block or Drop, never a default that silently
// picks one" true structurally rather than by convention.
type BackpressureMode uint8

const (
	ModeUnset BackpressureMode = iota
	ModeBlock
	ModeDrop
)

// StartKind is which start position a StartAt names.
type StartKind uint8

const (
	startUnset StartKind = iota
	startBeginning
	startExchangeTs
	startEmitIndex
	startLive
)

// StartAt is a subscriber's start position, explicit at subscribe time
// the same way its backpressure mode is explicit. Build one with
// Beginning, ExchangeTs, EmitIndex or Live; the zero value names no
// position and Subscribe rejects it. Only Live is not reproducible: its
// exact starting point depends on the wall-clock timing of the
// subscribe call. See docs/backpressure.md.
type StartAt struct {
	kind       StartKind
	exchangeTs int64
	emitIndex  uint64
}

// Beginning starts a subscriber at the first record the ring still
// holds, or the gap that precedes it if the writer has already moved on.
func Beginning() StartAt { return StartAt{kind: startBeginning} }

// ExchangeTs starts a subscriber at the first record whose exchange
// timestamp is at or after t.
func ExchangeTs(t int64) StartAt { return StartAt{kind: startExchangeTs, exchangeTs: t} }

// EmitIndex starts a subscriber at the given global emit index, exactly
// as merge produced it. A future index is legal: the subscriber waits.
func EmitIndex(i uint64) StartAt { return StartAt{kind: startEmitIndex, emitIndex: i} }

// Live starts a subscriber at whatever the current write index is at the
// moment Subscribe runs. It is the one StartAt kind this project does
// not claim is reproducible.
func Live() StartAt { return StartAt{kind: startLive} }

// Kind reports which start position s names.
func (s StartAt) Kind() StartKind { return s.kind }

// Delivery is one unit of what a subscriber receives: a record, and the
// gap that immediately preceded it when the writer had already lapped
// this subscriber. Blob aliases the subscriber's own buffer and is valid
// only until that subscriber's next call to Next, exactly as
// store.Reader.Blob documents for the mmap it aliases.
type Delivery struct {
	Record store.Record
	Blob   []byte
	Gap    Gap
	HasGap bool
}

// Subscriber is one reader of a Ring. One goroutine owns it and calls
// Next; a later commit's Block barrier is the only other code that ever
// touches it, and it only ever loads Cursor.
type Subscriber struct {
	ring  *Ring
	mode  BackpressureMode
	start StartAt

	// cursor is the next emit index this subscriber wants, published
	// after each successful read for the writer's Block-barrier
	// min-cursor computation. This subscriber's own goroutine is the
	// only writer to it.
	cursor atomic.Uint64

	// evicted is set by the watchdog (see barrier.go) when this
	// subscriber has held the Block barrier with no progress past its
	// configured timeout. Checked by this subscriber's own Next.
	evicted atomic.Bool

	// canceled is set by Cancel, from any goroutine, when this one
	// subscriber is to stop. It is a second flag rather than a second
	// meaning for evicted, so the two out-of-band stops stay
	// distinguishable in the error a caller receives. Both are one-way:
	// neither is ever cleared, so no ordering between them can produce a
	// state Next reads inconsistently.
	canceled atomic.Bool

	// Owned solely by this subscriber's own goroutine from here down.
	pos     uint64
	blobBuf []byte
}

// Mode returns the backpressure mode fixed at Subscribe.
func (s *Subscriber) Mode() BackpressureMode { return s.mode }

// Cursor returns the next emit index this subscriber wants. Safe to call
// from any goroutine; only this subscriber's own goroutine ever advances
// it.
func (s *Subscriber) Cursor() uint64 { return s.cursor.Load() }

// Cancel stops this one subscriber: its current or next call to Next
// returns ErrCanceled, and every call after that does too. It is safe to
// call from any goroutine, any number of times, and it never affects
// what any other subscriber receives. It does not remove s from the
// Block barrier, so a caller stopping a Block subscriber for good calls
// Ring.Unsubscribe, which does both. See docs/backpressure.md.
func (s *Subscriber) Cancel() { s.canceled.Store(true) }

// Subscribe registers a new subscriber against r, starting at the
// position start names, with the given backpressure mode. Before
// StartEmitting has been called, it resolves the starting position and
// returns immediately — the common case (see docs/backpressure.md) of a
// subscriber set fixed before the first event. Once r is being
// actively emitted, the request is queued and resolved by the emit
// goroutine between records instead, so a joining goroutine never
// mutates the subscriber set while the emit goroutine might be
// iterating it.
//
// A Block subscriber whose resolved start position has already been
// overwritten is rejected with ErrStartLapped: Block promises no loss,
// and the ring cannot reproduce data it no longer holds. A Drop
// subscriber in the same situation needs no special handling at all —
// its first call to Next runs the ordinary lapping protocol and reports
// the exact gap.
func (r *Ring) Subscribe(mode BackpressureMode, start StartAt) (*Subscriber, error) {
	if mode != ModeBlock && mode != ModeDrop {
		return nil, ErrModeUnset
	}
	if start.kind == startUnset {
		return nil, ErrStartUnset
	}

	r.ctrl.mu.Lock()
	switch {
	case r.ctrl.closed:
		r.ctrl.mu.Unlock()
		return nil, ErrClosed
	case r.ctrl.started:
		reply := make(chan controlReply, 1)
		r.ctrl.pending = append(r.ctrl.pending, control{kind: controlSubscribe, mode: mode, start: start, reply: reply})
		r.ctrl.mu.Unlock()
		res := <-reply
		return res.sub, res.err
	default:
		r.ctrl.mu.Unlock()
		return r.subscribeNow(mode, start)
	}
}

// subscribeNow resolves start immediately against r's current state and
// registers the resulting Subscriber. Called directly by Subscribe
// before StartEmitting, and by applyControl once it has been called.
func (r *Ring) subscribeNow(mode BackpressureMode, start StartAt) (*Subscriber, error) {
	var cursor uint64
	switch start.kind {
	case startBeginning:
		cursor = 0
	case startExchangeTs:
		w := r.writeSeq.Load()
		if found, ok := r.findFirstAtOrAfter(start.exchangeTs, r.oldestIndex(), w); ok {
			cursor = found
		} else {
			// No record already written satisfies ts. exchange_ts is
			// non-decreasing across the whole merged stream (see
			// docs/format.md), so every future record does, and
			// starting from the live edge is exactly correct rather
			// than an approximation.
			cursor = w
		}
	case startEmitIndex:
		cursor = start.emitIndex
	case startLive:
		cursor = r.writeSeq.Load()
	default:
		return nil, ErrStartUnset
	}

	if mode == ModeBlock && start.kind != startLive && cursor < r.oldestIndex() {
		return nil, ErrStartLapped
	}

	s := &Subscriber{ring: r, mode: mode, start: start, pos: cursor}
	s.cursor.Store(cursor)
	if r.maxBlob > 0 {
		// Sized once, up front, to the ring's own configured maximum,
		// so a subscriber's first snapshot read does not grow the
		// buffer on what is otherwise a zero-allocation path.
		s.blobBuf = make([]byte, 0, r.maxBlob)
	}

	if mode == ModeBlock {
		r.blocking = append(r.blocking, s)
	}
	return s, nil
}

// oldestIndex returns the oldest emit index the ring might still hold.
// Anything before it has certainly been overwritten.
func (r *Ring) oldestIndex() uint64 {
	w := r.writeSeq.Load()
	capacity := uint64(r.Capacity())
	if w > capacity {
		return w - capacity
	}
	return 0
}

// findFirstAtOrAfter scans [from, to) for the first record whose
// exchange timestamp is at or after t. It reports false if it cannot
// confirm an answer — either nothing in range qualifies, or a concurrent
// writer lapped the scan itself — in which case the caller falls back to
// starting from the live edge.
func (r *Ring) findFirstAtOrAfter(t int64, from, to uint64) (uint64, bool) {
	var rec store.Record
	for i := from; i < to; i++ {
		_, _, complete := r.read(i, &rec, nil)
		if !complete {
			return 0, false
		}
		if rec.ExchangeTs >= t {
			return i, true
		}
	}
	return 0, false
}

// Next returns this subscriber's next delivery, blocking (without
// reading a clock, and without holding a core through a busy spin) until
// one is ready, the ring's declared end is reached, or this subscriber
// is stopped out of band. It reports false, with a nil error, at a clean
// end of stream, false with ErrEvicted if the stalled-Block-subscriber
// watchdog evicted it, and false with ErrCanceled if Cancel was called.
// Those two are the only errors this package's own code ever produces,
// because they are the two places a run's outcome is not fully
// determined by content and order alone. See docs/backpressure.md.
func (s *Subscriber) Next() (Delivery, bool, error) {
	for {
		// Both stops are checked before attempting a read, not after:
		// the ordinary lapping protocol could technically still serve
		// this subscriber's stale cursor through a catch-up (whatever
		// now occupies the slot it wanted), and that would make a stop a
		// one-time skip rather than the final severing it is meant to
		// be. Once stopped, this subscriber never receives another
		// record. Eviction is tested first so a subscriber that is both
		// reports the same error every time, by the order of these two
		// lines rather than by which flag happened to be stored first.
		if s.evicted.Load() {
			return Delivery{}, false, ErrEvicted
		}
		if s.canceled.Load() {
			return Delivery{}, false, ErrCanceled
		}
		var rec store.Record
		blob, gap, hasGap, next, ready := s.ring.next(s.pos, &rec, s.blobBuf)
		if ready {
			if blob != nil {
				// Capture any growth readBlob did, so a later, larger
				// snapshot does not need to grow again.
				s.blobBuf = blob
			}
			s.pos = next
			s.cursor.Store(next)
			return Delivery{Record: rec, Blob: blob, Gap: gap, HasGap: hasGap}, true, nil
		}
		if end, has := s.ring.End(); has && s.pos >= end {
			return Delivery{}, false, nil
		}
		runtime.Gosched()
	}
}
