// Package book reconstructs a per-instrument L2 orderbook from the
// snapshot and delta records internal/store serves. It is a library used
// by cmd/convert (emitting snapshot epochs), by seek warm-up
// (reconstructing a valid book before a mid-stream replay starts), and
// optionally by an external client replaying its own received stream. It
// must never be invoked per-record on the shared fan-out path: if it
// were, its cost would multiply by subscribers x instruments x levels,
// which is exactly the throughput bottleneck the merge and fan-out stages
// are designed to avoid. See docs/book.md.
//
// This package's own allocation profile is deliberately NOT gated by the
// M7 zero-allocation benchmarks: those assert on the hot path up to the
// fan-out boundary, and this package sits entirely outside it. Do not add
// an allocation-gate benchmark here on the mistaken assumption that it
// needs one.
package book

import (
	"fmt"

	"replay/internal/store"
)

// Book holds one instrument's reconstructed order book: a bid side and an
// ask side. One Book covers exactly one instrument, by InstrumentID —
// this is what makes epoch warm-up tractable even though
// store.Reader.SnapshotBefore answers "the nearest preceding snapshot
// pointer" without regard to instrument. See docs/book.md.
type Book struct {
	instrumentID uint32
	bids         side
	asks         side
}

// NewBook returns an empty book for instrumentID.
func NewBook(instrumentID uint32) *Book {
	return &Book{instrumentID: instrumentID}
}

// InstrumentID returns the instrument this book covers.
func (b *Book) InstrumentID() uint32 { return b.instrumentID }

// Bids returns the bid side ordered best-first: highest price first.
func (b *Book) Bids() []store.Level {
	levels := b.bids.snapshot()
	reverseLevels(levels)
	return levels
}

// Asks returns the ask side ordered best-first: lowest price first.
func (b *Book) Asks() []store.Level {
	return b.asks.snapshot()
}

// BestBid returns the highest-priced bid level, and whether the bid side
// holds any level at all.
func (b *Book) BestBid() (store.Level, bool) {
	if len(b.bids.levels) == 0 {
		return store.Level{}, false
	}
	return b.bids.levels[len(b.bids.levels)-1], true
}

// BestAsk returns the lowest-priced ask level, and whether the ask side
// holds any level at all.
func (b *Book) BestAsk() (store.Level, bool) {
	if len(b.asks.levels) == 0 {
		return store.Level{}, false
	}
	return b.asks.levels[0], true
}

// ApplySnapshot replaces the book's state with levels, the first bidCount
// of which are bids and the rest asks — the shape
// store.Reader.AppendLevels returns. A caller decodes a snapshot pointer
// record's blob with the Reader's own method and passes the result
// straight in, rather than this package duplicating blob-parsing logic
// that already lives in internal/store.
func (b *Book) ApplySnapshot(levels []store.Level, bidCount int) error {
	if bidCount < 0 || bidCount > len(levels) {
		return ErrBidCountOutOfRange
	}
	if err := b.bids.reset(levels[:bidCount]); err != nil {
		return err
	}
	return b.asks.reset(levels[bidCount:])
}

// Apply applies one Delta record to the book. Size is the level's new
// size at Price, replacing it in place; a zero Size removes the level.
// See docs/book.md for why this is a replace, not an increment. Apply
// returns ErrWrongRecordType if rec is not a Delta record, and
// ErrWrongInstrument if rec.InstrumentID does not match this book.
func (b *Book) Apply(rec store.Record) error {
	if rec.RecordType != store.RecordTypeDelta {
		return ErrWrongRecordType
	}
	if rec.InstrumentID != b.instrumentID {
		return ErrWrongInstrument
	}

	s := &b.bids
	if rec.SideFlags == store.SideAsk {
		s = &b.asks
	}
	if err := s.upsert(rec.Price, rec.Size); err != nil {
		return fmt.Errorf("book: apply delta (instrument=%d, price=%d): %w", rec.InstrumentID, rec.Price, err)
	}
	return nil
}

// reverseLevels reverses levels in place.
func reverseLevels(levels []store.Level) {
	for i, j := 0, len(levels)-1; i < j; i, j = i+1, j-1 {
		levels[i], levels[j] = levels[j], levels[i]
	}
}
