// Package merge orders many venues' records into one canonical stream.
// The k-way merge is a fixed-size loser tree, never container/heap: the
// tree's shape is settled by the venue count at construction and never
// changes as records are consumed, which is what makes the merged order
// independent of how many workers decode. See docs/merge.md.
package merge

import (
	"math"

	"replay/internal/store"
)

// Key is the total ordering key of the replay stream. The fields are
// declared in key order, and compareKey is the only thing that defines
// that order: nothing outside these four fields may influence it — not
// arrival order, not goroutine identity, not a clock. See CLAUDE.md.
type Key struct {
	ExchangeTs     int64
	VenueID        uint16
	SequenceNumber uint64
	InstrumentID   uint32
}

// SentinelKey is the key of an empty or exhausted cursor. It sorts
// after every real key, so such a cursor loses every match and is never
// selected. That is what lets a partition stay in the tree once it runs
// out, instead of being removed from it, and keeps the tree's shape a
// function of the venue count alone.
var SentinelKey = Key{
	ExchangeTs:     math.MaxInt64,
	VenueID:        math.MaxUint16,
	SequenceNumber: math.MaxUint64,
	InstrumentID:   math.MaxUint32,
}

// keyOf returns rec's ordering key.
func keyOf(rec store.Record) Key {
	return Key{
		ExchangeTs:     rec.ExchangeTs,
		VenueID:        rec.VenueID,
		SequenceNumber: rec.SequenceNumber,
		InstrumentID:   rec.InstrumentID,
	}
}

// checkOrder reports whether next may follow prev in the stream. Every
// layer that validates the ordering key calls this one function: the
// cursor between two records, a worker inside its batch, a venue at the
// seam between two batches, and the loser tree across the merged
// stream. A key that repeats is ErrDuplicateKey and one that goes
// backwards is ErrOutOfOrder.
func checkOrder(prev, next Key) error {
	switch c := compareKey(prev, next); {
	case c == 0:
		return ErrDuplicateKey
	case c > 0:
		return ErrOutOfOrder
	}
	return nil
}

// compareKey returns a negative value, zero, or a positive value as a
// sorts before, equal to, or after b. Equal keys are a hard error
// everywhere this package compares them: two records sharing a key are
// a corrupt artifact, not a tie to break.
func compareKey(a, b Key) int {
	if a.ExchangeTs != b.ExchangeTs {
		if a.ExchangeTs < b.ExchangeTs {
			return -1
		}
		return 1
	}
	if a.VenueID != b.VenueID {
		if a.VenueID < b.VenueID {
			return -1
		}
		return 1
	}
	if a.SequenceNumber != b.SequenceNumber {
		if a.SequenceNumber < b.SequenceNumber {
			return -1
		}
		return 1
	}
	if a.InstrumentID != b.InstrumentID {
		if a.InstrumentID < b.InstrumentID {
			return -1
		}
		return 1
	}
	return 0
}
