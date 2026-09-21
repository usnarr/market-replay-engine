package fanout

import "sync"

// controlKind is which request a queued control entry carries.
type controlKind uint8

const (
	controlSubscribe controlKind = iota + 1
	controlUnsubscribe
)

// control is one queued join or leave request, applied by the emit
// goroutine between records rather than by the requesting goroutine
// mutating the subscriber set directly while the emit goroutine might be
// iterating it.
type control struct {
	kind  controlKind
	mode  BackpressureMode
	start StartAt
	sub   *Subscriber // set for controlUnsubscribe

	// reply is buffered to one: applyControl's send never blocks, even
	// if the requesting goroutine has stopped waiting for some reason.
	reply chan controlReply
}

type controlReply struct {
	sub *Subscriber
	err error
}

// controlQueue is a mutex-guarded slice, not a channel: Subscribe and
// Unsubscribe need to tell "queue this" apart from "the ring is closed"
// under one lock, which a channel-based queue cannot do without a
// second select case on the receive — exactly the construct this
// package's no-multi-select rule exists to keep out. See
// docs/backpressure.md.
type controlQueue struct {
	mu      sync.Mutex
	pending []control
	started bool
	closed  bool
}

// StartEmitting marks r as actively being driven by an emit loop: from
// this point on, Subscribe and Unsubscribe queue their requests instead
// of mutating the subscriber set directly, and the driving loop is
// responsible for applying them between records by calling applyControl.
// Call it exactly once, from the single goroutine about to start
// emitting, before its first Write.
func (r *Ring) StartEmitting() {
	r.ctrl.mu.Lock()
	r.ctrl.started = true
	r.ctrl.mu.Unlock()
}

// applyControl applies every control request queued since the last
// call, in the order they were queued. It is cheap to call whether or
// not anything is pending, so the emit loop calls it between every
// record and the Block barrier calls it on every spin iteration — the
// second of those is what lets a departing Block subscriber release a
// writer parked waiting for it.
func (r *Ring) applyControl() {
	r.ctrl.mu.Lock()
	pending := r.ctrl.pending
	r.ctrl.pending = nil
	r.ctrl.mu.Unlock()

	for _, c := range pending {
		switch c.kind {
		case controlSubscribe:
			s, err := r.subscribeNow(c.mode, c.start)
			c.reply <- controlReply{sub: s, err: err}
		case controlUnsubscribe:
			r.unsubscribeNow(c.sub)
			c.reply <- controlReply{}
		}
	}
}

// closeControl marks the control queue closed and fails every request
// still pending with ErrClosed. Called once, when the data stream ends;
// see SetEnd.
func (r *Ring) closeControl() {
	r.ctrl.mu.Lock()
	r.ctrl.closed = true
	pending := r.ctrl.pending
	r.ctrl.pending = nil
	r.ctrl.mu.Unlock()

	for _, c := range pending {
		switch c.kind {
		case controlSubscribe:
			c.reply <- controlReply{err: ErrClosed}
		case controlUnsubscribe:
			c.reply <- controlReply{}
		}
	}
}

// Unsubscribe removes s from r and cancels it, so a goroutine parked in
// s.Next is released with ErrCanceled instead of waiting for the whole
// ring to end. Once r is being actively emitted (see StartEmitting), the
// removal is queued and applied between records, exactly like a join;
// before that, it happens immediately.
//
// The cancellation is deferred to after the removal on every return
// path, so the writer never observes a canceled subscriber that still
// counts toward the Block barrier — a cursor that has stopped advancing
// while it is still registered is exactly what the barrier waits on
// forever. See docs/backpressure.md.
func (r *Ring) Unsubscribe(s *Subscriber) error {
	defer s.Cancel()

	r.ctrl.mu.Lock()
	if !r.ctrl.started {
		r.ctrl.mu.Unlock()
		r.unsubscribeNow(s)
		return nil
	}
	if r.ctrl.closed {
		r.ctrl.mu.Unlock()
		return nil
	}
	reply := make(chan controlReply, 1)
	r.ctrl.pending = append(r.ctrl.pending, control{kind: controlUnsubscribe, sub: s, reply: reply})
	r.ctrl.mu.Unlock()

	<-reply
	return nil
}

// unsubscribeNow removes s from r.blocking, if it is a Block subscriber.
// A plain slice search and delete, never a map: the subscriber count in
// any real run is small, and iteration order here has never carried
// meaning.
func (r *Ring) unsubscribeNow(s *Subscriber) {
	if s.mode != ModeBlock {
		return
	}
	for i, b := range r.blocking {
		if b == s {
			r.blocking = append(r.blocking[:i], r.blocking[i+1:]...)
			return
		}
	}
}
