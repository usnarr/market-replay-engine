package merge

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"

	"replay/internal/store"
)

const (
	// batchRecords is how many records one unit of decode work covers.
	// It matches the store's default block size, so a range starting at
	// a multiple of it starts on a block boundary and each block is
	// checksummed by exactly one worker. A file written with another
	// block size still decodes correctly; some blocks are just verified
	// more than once.
	batchRecords = 1024

	// feedBatches is how many batches one venue owns. One is being
	// decoded, one is queued behind it, and one is in the merge's hand.
	// They rotate through a free channel and are never reallocated, so
	// steady-state decode allocates nothing.
	feedBatches = 3
)

// A venue issues work only while it holds fewer than feedBatches-1
// batches, so two is the smallest value that lets it issue any at all.
// This fails to compile below that.
const _ = uint(feedBatches - 2)

// batch is a run of consecutive records from one venue, decoded by one
// worker. It carries its records and nothing else: no worker id, no
// arena index, no goroutine-assigned counter. Decode is a pure function
// of the file bytes and the record range, so nothing about which worker
// ran, or when, can reach the merged stream. That is what makes the
// worker count irrelevant to the output.
type batch struct {
	venueID uint16
	events  []Event
	err     error

	// done carries one value per unit of work, from the worker that
	// filled this batch to the goroutine that owns the venue. It is
	// buffered and reused: a batch is re-issued only after its owner has
	// received the previous value, so the send never blocks.
	done chan struct{}
}

func newBatch(venueID uint16) *batch {
	return &batch{
		venueID: venueID,
		events:  make([]Event, 0, batchRecords),
		done:    make(chan struct{}, 1),
	}
}

// fill decodes count records from r, starting at index start. It records
// the first problem it finds in b.err and stops there; a caller must not
// emit a batch whose err is set, even the part before the error.
func (b *batch) fill(r *store.Reader, start, count int) {
	b.events = b.events[:0]
	b.err = nil

	block := -1
	var last Key
	for i := start; i < start+count; i++ {
		if n := i / int(r.BlockSizeRecords()); n != block {
			if err := r.VerifyBlock(n); err != nil {
				b.err = err
				return
			}
			block = n
		}

		rec := r.RecordAt(i)
		if rec.VenueID != b.venueID {
			b.err = ErrVenueMismatch
			return
		}
		// This can only fire on a file some other tool wrote: this
		// project's own writer refuses a key that does not increase, and
		// the block checksum above has already passed.
		key := keyOf(rec)
		if i > start {
			if err := checkOrder(last, key); err != nil {
				b.err = err
				return
			}
		}
		last = key

		ev := Event{Record: rec}
		if rec.RecordType == store.RecordTypeSnapshotPointer {
			blob, err := r.Blob(rec)
			if err != nil {
				b.err = err
				return
			}
			ev.Blob = blob
		}
		b.events = append(b.events, ev)
	}
}

// fillFromCursor refills b by reading c in the calling goroutine. A
// short fill means the cursor is exhausted.
func (b *batch) fillFromCursor(c *Cursor) error {
	b.events = b.events[:0]
	for len(b.events) < cap(b.events) {
		ev, ok, err := c.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		b.events = append(b.events, ev)
	}
	return nil
}

// workItem is one range of one file, to be decoded into one batch. It
// names the work by position, never by who will do it.
type workItem struct {
	reader *store.Reader
	start  int
	count  int
	batch  *batch
}

// pool is a fixed set of decode workers sharing one queue. Workers are
// interchangeable: none of them carries an index, and nothing a worker
// produces depends on which one it is.
type pool struct {
	queue chan workItem
	wg    sync.WaitGroup
}

func newPool(workers int) *pool {
	p := &pool{queue: make(chan workItem, workers)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer p.wg.Done()
			for item := range p.queue {
				item.batch.fill(item.reader, item.start, item.count)
				item.batch.done <- struct{}{}
			}
		}()
	}
	return p
}

// stop ends every worker once the queue is drained. Every venue's owner
// goroutine must have returned first, so nothing can still be queueing
// work — and the queued work must finish before the files it reads are
// closed.
func (p *pool) stop() {
	close(p.queue)
	p.wg.Wait()
}

// venueFeed owns one venue's batch channel and its send order. Only this
// goroutine sends on the channel, and it sends batches in file and
// record order, never in the order workers finish them. Letting a pool
// worker send directly would put an out-of-order stream in front of the
// loser tree, which nothing downstream re-sorts.
type venueFeed struct {
	venueID uint16
	readers []*store.Reader
	abort   *atomic.Bool

	file  int
	index int

	ch   chan *batch
	free chan *batch

	last    Key
	hasLast bool

	// perturb is nil in every production replay. The chaos test is the
	// only caller that sets it, to a runtime.Gosched call gated by its
	// own venue-seeded math/rand/v2 source, so it can shift this
	// goroutine's interleaving with the merge, the other venues, and the
	// worker pool without touching a clock.
	perturb func()
}

func newVenueFeed(c *Cursor, abort *atomic.Bool) *venueFeed {
	f := &venueFeed{
		venueID: c.venueID,
		readers: c.readers,
		abort:   abort,
		ch:      make(chan *batch),
		free:    make(chan *batch, feedBatches),
	}
	for i := 0; i < feedBatches; i++ {
		f.free <- newBatch(c.venueID)
	}
	return f
}

// nextRange returns the next range of records to decode, walking the
// venue's files in order. It reports false once every file is covered.
func (f *venueFeed) nextRange() (*store.Reader, int, int, bool) {
	for f.file < len(f.readers) {
		r := f.readers[f.file]
		if f.index >= r.Len() {
			f.file++
			f.index = 0
			continue
		}

		start := f.index
		count := batchRecords
		if rem := r.Len() - start; count > rem {
			count = rem
		}
		f.index += count
		return r, start, count, true
	}
	return nil, 0, 0, false
}

// run issues this venue's decode work and sends the finished batches in
// order. It always closes the channel on the way out, including for a
// venue with no records at all: the merge's blocking receive on an empty
// partition would otherwise never return.
func (f *venueFeed) run(p *pool) {
	defer close(f.ch)

	inflight := make([]*batch, 0, feedBatches)
	for {
		// Issue ahead, but leave one batch for the merge to hold. That
		// keeps this goroutine's only wait the send below, where
		// backpressure from a slow merge belongs. Claiming every batch
		// still works — the merge frees one before it blocks on the
		// receive — but it parks the owner on the free channel instead,
		// with nothing decoded and ready.
		for len(inflight) < feedBatches-1 && !f.abort.Load() {
			r, start, count, ok := f.nextRange()
			if !ok {
				break
			}
			f.doPerturb()
			b := <-f.free
			f.doPerturb()
			p.queue <- workItem{reader: r, start: start, count: count, batch: b}
			inflight = append(inflight, b)
		}
		if len(inflight) == 0 {
			return
		}

		b := inflight[0]
		inflight = inflight[1:]
		f.doPerturb()
		<-b.done
		if f.abort.Load() {
			return
		}
		if b.err == nil {
			b.err = f.checkSeam(b)
		}
		f.doPerturb()
		f.ch <- b
	}
}

// doPerturb calls perturb if the chaos test set one. It is nil in every
// production replay, so this is one nil check with no call behind it.
func (f *venueFeed) doPerturb() {
	if f.perturb != nil {
		f.perturb()
	}
}

// newChaosRand returns the math/rand/v2 source one venue's perturbation
// draws from. Seeding by (seed, venueID) — never a worker index or a
// start-order-derived value — is what keeps the chaos test's own
// randomness free of the goroutine-identity dependency it exists to
// rule out elsewhere.
func newChaosRand(seed uint64, venueID uint16) *rand.Rand {
	return rand.New(rand.NewPCG(seed, uint64(venueID)))
}

// chaosPerturb returns a perturb func for one venue feed: at each call,
// it yields the processor with even odds, using a source owned by this
// one goroutine. rand/v2's *rand.Rand has no internal lock, so sharing
// one across goroutines would be a data race; a separate instance per
// venue avoids that instead of serializing on one.
//
// runtime.Gosched, never time.Sleep: a real sleep reads the clock
// indirectly through the scheduler, which this repository allows only
// inside internal/clock, and it would slow the test for no benefit
// Gosched does not also give.
func chaosPerturb(seed uint64, venueID uint16) func() {
	rng := newChaosRand(seed, venueID)
	return func() {
		if rng.Uint64()&1 == 0 {
			runtime.Gosched()
		}
	}
}

// checkSeam validates a batch's first key against the last key this
// venue produced. A worker validates inside its own batch; this
// goroutine is the only one that ever sees two consecutive batches, so
// the seam between them is checked here. A file boundary always falls on
// a seam.
func (f *venueFeed) checkSeam(b *batch) error {
	if len(b.events) == 0 {
		return nil
	}
	if f.hasLast {
		if err := checkOrder(f.last, keyOf(b.events[0].Record)); err != nil {
			return err
		}
	}
	f.last = keyOf(b.events[len(b.events)-1].Record)
	f.hasLast = true
	return nil
}
