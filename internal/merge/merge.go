package merge

import (
	"slices"
	"sync/atomic"
)

// feed is the merge's view of one venue: the batch it is walking, and
// the way it gets the next one. The choice between decoding inline and
// receiving from a reader goroutine is made once per batch, never per
// record.
type feed struct {
	held *batch
	pos  int

	cursor *Cursor // set when the merge decodes inline
	ch     <-chan *batch
	free   chan<- *batch
}

// next returns this venue's next event, reporting false once the venue
// is exhausted.
func (f *feed) next() (Event, bool, error) {
	if f.held != nil && f.pos < len(f.held.events) {
		ev := f.held.events[f.pos]
		f.pos++
		return ev, true, nil
	}
	return f.refill()
}

func (f *feed) refill() (Event, bool, error) {
	if f.cursor != nil {
		if err := f.held.fillFromCursor(f.cursor); err != nil {
			return Event{}, false, err
		}
	} else {
		// Hand the spent batch back before blocking, so this venue's
		// reader goroutine can refill it while the merge waits.
		if f.held != nil {
			f.free <- f.held
			f.held = nil
		}

		// A single-case blocking receive. Never a select across cursors,
		// and never a second case for cancellation: the runtime picks
		// pseudo-randomly among ready cases, and an aborting run could
		// then emit one more event or none depending on scheduling. The
		// channel closing is the only signal, and Close discards the
		// whole run rather than keeping a valid prefix.
		b, ok := <-f.ch
		if !ok {
			return Event{}, false, nil
		}
		f.held = b
		if b.err != nil {
			// The records before the error in this batch are not
			// emitted. A rejected artifact produces no output.
			return Event{}, false, b.err
		}
	}

	f.pos = 0
	if len(f.held.events) == 0 {
		return Event{}, false, nil
	}
	f.pos = 1
	return f.held.events[0], true, nil
}

// Merger merges one cursor per venue into the canonical replay stream.
// It holds one event per venue ahead of what it emits, because the loser
// tree needs every venue's next key to pick a winner.
//
// A Merger is not safe for concurrent use: one goroutine calls Next and
// Close.
type Merger struct {
	cursors []*Cursor
	feeds   []feed
	venues  []*venueFeed // nil when decoding inline
	tree    *LoserTree
	pending []Event
	pool    *pool
	abort   atomic.Bool
	err     error
	closed  bool
}

// NewMerger returns a merger that decodes in the calling goroutine, with
// no worker pool. Use it when a replay should start no goroutines at
// all; the output is identical to NewConcurrentMerger's.
func NewMerger(cursors []*Cursor) (*Merger, error) {
	return newMerger(cursors, 0, nil)
}

// NewConcurrentMerger returns a merger whose records are decoded by a
// shared pool of workers goroutines, one owner goroutine per venue
// keeping that venue's batches in file order. The worker count changes
// decode and I/O concurrency only: it never changes the merged stream.
func NewConcurrentMerger(cursors []*Cursor, workers int) (*Merger, error) {
	if workers < 1 {
		return nil, ErrWorkerCount
	}
	return newMerger(cursors, workers, nil)
}

// newMergerChaos is NewConcurrentMerger with scheduling perturbation
// injected into each venue's owner goroutine, for the chaos test only.
// Each venue gets its own math/rand/v2 source seeded from (seed,
// venueID) — never from a worker index or start order, which would
// reintroduce exactly the goroutine-identity dependency the chaos test
// exists to rule out.
func newMergerChaos(cursors []*Cursor, workers int, seed uint64) (*Merger, error) {
	if workers < 1 {
		return nil, ErrWorkerCount
	}
	return newMerger(cursors, workers, &seed)
}

// newMerger takes ownership of the cursors. It reads one event from each
// venue to seed the tree, so an unreadable partition fails here rather
// than on the first Next.
//
// Two cursors may not hold the same venue. The claim that two live
// cursors can never tie on the ordering key rests on their venue ids
// differing, and so does the claim that a duplicate key can only come
// from inside one partition.
func newMerger(cursors []*Cursor, workers int, chaosSeed *uint64) (*Merger, error) {
	venues := make([]uint16, len(cursors))
	for i, c := range cursors {
		venues[i] = c.VenueID()
	}
	slices.Sort(venues)
	for i := 1; i < len(venues); i++ {
		if venues[i] == venues[i-1] {
			return nil, ErrDuplicateVenue
		}
	}

	m := &Merger{
		cursors: cursors,
		feeds:   make([]feed, len(cursors)),
		tree:    NewLoserTree(len(cursors)),
		pending: make([]Event, len(cursors)),
	}
	if workers > 0 {
		m.pool = newPool(workers)
		m.venues = make([]*venueFeed, len(cursors))
		for i, c := range cursors {
			vf := newVenueFeed(c, &m.abort)
			if chaosSeed != nil {
				// vf.venueID, not the loop index i or any other
				// start-order-derived value: see chaosPerturb's doc
				// comment for why.
				vf.perturb = chaosPerturb(*chaosSeed, vf.venueID)
			}
			m.venues[i] = vf
			m.feeds[i] = feed{ch: vf.ch, free: vf.free}
			go vf.run(m.pool)
		}
	} else {
		for i, c := range cursors {
			m.feeds[i] = feed{cursor: c, held: newBatch(c.VenueID())}
		}
	}

	for i := range m.feeds {
		ev, ok, err := m.feeds[i].next()
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		if !ok {
			// The venue keeps its leaf and its sentinel key.
			continue
		}
		m.pending[i] = ev
		m.tree.SetKey(i, keyOf(ev.Record))
	}
	m.tree.Init()
	return m, nil
}

// Next returns the stream's next event, reporting false at the end. The
// stream ends when every venue is exhausted, never when the first one
// is: a venue that runs out early simply stops winning.
//
// An error is final. Next keeps returning it, because a run that
// rejected its input has no valid remainder to hand back: the events
// already emitted are the start of a run to discard, not a prefix to
// keep.
func (m *Merger) Next() (Event, bool, error) {
	if m.err != nil {
		return Event{}, false, m.err
	}

	i, key := m.tree.Winner()
	if key == SentinelKey {
		return Event{}, false, nil
	}
	ev := m.pending[i]

	next, ok, err := m.feeds[i].next()
	if err != nil {
		m.err = err
		return Event{}, false, err
	}
	nextKey := SentinelKey
	if ok {
		m.pending[i] = next
		nextKey = keyOf(next.Record)
	}
	if err := m.tree.Advance(nextKey); err != nil {
		m.err = err
		return Event{}, false, err
	}
	return ev, true, nil
}

// Close stops every goroutine the merger started and releases every
// venue's files. It is idempotent, and Next must not be called after it.
//
// Closing part way through a replay discards the run: the events emitted
// so far are not a valid prefix to keep, they are the start of a run
// that was abandoned. Making an abort graceful would need a second
// select case on the receive in refill, which is exactly the construct
// that would make the abandoned run's length depend on scheduling.
//
// The abort flag below is what stops the reader goroutines from
// decoding the rest of the dataset on the way out. Draining alone would
// finish the run correctly but would make Close cost the whole
// remaining replay.
func (m *Merger) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true

	if m.pool != nil {
		m.abort.Store(true)
		for _, vf := range m.venues {
			// Draining is enough to release the owner, and it never
			// deadlocks on the free channel: an owner waits there only
			// while it holds fewer than feedBatches-1 batches, so with
			// the one the merge may be holding, at least one is always
			// free. Handing the merge's batch back first would be a
			// no-op that looked load-bearing.
			for b := range vf.ch {
				vf.free <- b
			}
		}
		// Every owner has returned, so nothing can queue more work. The
		// work already queued must finish before the files it decodes
		// from are unmapped.
		m.pool.stop()
	}

	var err error
	for _, c := range m.cursors {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
