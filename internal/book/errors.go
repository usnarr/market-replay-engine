package book

import "errors"

// Errors this package returns. Unlike internal/merge, this package is off
// the hot path (see book.go), so wrapping a sentinel with fmt.Errorf for
// context costs nothing that matters here. Compare with errors.Is.
var (
	// ErrLevelNotFound is returned when a delta removes a price that is
	// not currently in the book.
	ErrLevelNotFound = errors.New("book: delta removes a price level that is not present")

	// ErrDuplicateLevel is returned when a snapshot's decoded levels hold
	// two entries at the same price on one side.
	ErrDuplicateLevel = errors.New("book: snapshot has two levels at the same price on one side")

	// ErrWrongRecordType is returned by Apply when rec is not a Delta
	// record.
	ErrWrongRecordType = errors.New("book: record is not a delta")

	// ErrWrongInstrument is returned by Apply when rec's instrument does
	// not match the Book it is applied to.
	ErrWrongInstrument = errors.New("book: record instrument does not match the book")

	// ErrBidCountOutOfRange is returned by ApplySnapshot when bidCount is
	// negative or larger than the decoded level slice.
	ErrBidCountOutOfRange = errors.New("book: bid count exceeds the decoded levels")
)
