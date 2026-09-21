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
)
