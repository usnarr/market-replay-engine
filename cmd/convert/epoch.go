package main

import (
	"cmp"
	"slices"

	"replay/internal/book"
	"replay/internal/store"
)

// DefaultEpochEvery is the snapshot epoch cadence, in records per venue.
// It matches the cadence docs/format.md's seek benchmarks assume. A
// cadence is what bounds seek cost to "rewind to one epoch, then replay
// forward"; see docs/convert.md for why an epoch is emitted at the next
// timestamp boundary rather than exactly here.
const DefaultEpochEvery = 1000

// venueState is what the converter remembers about one venue across all
// of that venue's day files: the ordering-key position it has reached,
// the books it reconstructs to emit epochs from, and how far it is from
// the next epoch.
type venueState struct {
	id      uint16
	lastTs  int64
	lastSeq uint64
	seen    bool

	// last is the file the venue's most recent record went into. An epoch
	// is emitted there, not into the file the record that triggered it
	// belongs to, because the epoch carries the previous record's
	// timestamp and so belongs to that record's day.
	last *partition

	// books holds one book per instrument seen in this venue, sorted by
	// instrument ID. Sorted slice, never a map: the order decides the
	// sequence numbers an epoch's records are assigned, and a map would
	// make two conversions of the same input disagree.
	books []*book.Book

	since   int
	pending bool
}

// venue returns the state for venueID, creating it on first use.
func (c *converter) venue(venueID uint16) *venueState {
	i, found := slices.BinarySearchFunc(c.venues, venueID, func(v *venueState, id uint16) int {
		return cmp.Compare(v.id, id)
	})
	if found {
		return c.venues[i]
	}
	v := &venueState{id: venueID}
	c.venues = slices.Insert(c.venues, i, v)
	return v
}

// book returns instrumentID's book for this venue, creating it on first
// use and keeping books sorted by instrument ID.
func (v *venueState) book(instrumentID uint32) *book.Book {
	i, found := slices.BinarySearchFunc(v.books, instrumentID, func(b *book.Book, id uint32) int {
		return cmp.Compare(b.InstrumentID(), id)
	})
	if found {
		return v.books[i]
	}
	b := book.NewBook(instrumentID)
	v.books = slices.Insert(v.books, i, b)
	return b
}

// epochDue reports whether an epoch is waiting and rec is the record
// that closes the timestamp it belongs to. An epoch is emitted only at a
// strict timestamp increase, so every record it precedes sorts after it
// on exchange_ts alone.
func (v *venueState) epochDue(rec store.Record) bool {
	return v.pending && v.seen && rec.ExchangeTs > v.lastTs
}

// writeEpoch emits one snapshot pointer per instrument this venue has
// seen, all at the previous record's timestamp, with converter-assigned
// sequence numbers running on from that record's own. See
// docs/convert.md for why those two choices make the epoch's keys
// strictly increasing by construction.
func (v *venueState) writeEpoch() error {
	seq := v.lastSeq
	for _, b := range v.books {
		seq++
		rec := store.Record{
			ExchangeTs:     v.lastTs,
			SequenceNumber: seq,
			InstrumentID:   b.InstrumentID(),
			VenueID:        v.id,
		}
		if err := v.last.w.WriteSnapshot(rec, b.Bids(), b.Asks()); err != nil {
			return err
		}
	}
	v.lastSeq = seq
	v.since = 0
	v.pending = false
	return nil
}

// advance records rec as this venue's latest position and moves it
// towards the next epoch.
func (v *venueState) advance(rec store.Record, p *partition, epochEvery int) {
	v.lastTs, v.lastSeq, v.seen, v.last = rec.ExchangeTs, rec.SequenceNumber, true, p
	v.since++
	if v.since >= epochEvery {
		v.pending = true
	}
}

// applyToBook folds rec into the book the next epoch will snapshot. A
// trade changes no level, so it is not applied at all.
func (v *venueState) applyToBook(rec store.Record, bids, asks []store.Level) error {
	switch rec.RecordType {
	case store.RecordTypeDelta:
		return v.book(rec.InstrumentID).Apply(rec)
	case store.RecordTypeSnapshotPointer:
		levels := make([]store.Level, 0, len(bids)+len(asks))
		levels = append(levels, bids...)
		levels = append(levels, asks...)
		return v.book(rec.InstrumentID).ApplySnapshot(levels, len(bids))
	}
	return nil
}
