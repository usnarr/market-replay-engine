package main

import (
	"fmt"
	"time"
)

// Partitioning is by venue and UTC day: one output file holds one
// venue's records for one day. This is a working assumption, not a
// settled answer — see the M10 plan's Q2 discussion, which leaves the
// granularity (venue alone, venue and day, or venue and instrument) open
// until real archive data says which one the source is organized by.
// Venue and day is the most common shape for market data archives and
// keeps one file at a manageable size, so it is what this converter
// implements until that question is answered.
//
// The format itself already requires one venue per file (docs/format.md),
// so only the day half of this is an assumption.

// nanosPerDay is the length of one UTC day. Market data timestamps are
// nanoseconds since the Unix epoch, per docs/format.md.
const nanosPerDay = 24 * 60 * 60 * 1_000_000_000

// PartitionKey identifies one output file: one venue, one UTC day.
type PartitionKey struct {
	VenueID uint16

	// Day is the whole UTC day exchangeTs falls in, counted from the Unix
	// epoch. It is negative before 1970.
	Day int64
}

// PartitionOf returns the partition a record with this venue and
// timestamp belongs to.
func PartitionOf(venueID uint16, exchangeTs int64) PartitionKey {
	return PartitionKey{VenueID: venueID, Day: floorDiv(exchangeTs, nanosPerDay)}
}

// FileName returns the output file's name: the venue, then the day as a
// date. A date rather than a day number, because the operator reading a
// directory listing is looking for a session, not an epoch offset.
func (k PartitionKey) FileName() string {
	date := time.Unix(k.Day*(nanosPerDay/1_000_000_000), 0).UTC()
	return fmt.Sprintf("venue-%d-%s.bin", k.VenueID, date.Format("2006-01-02"))
}

// Compare orders two partition keys by venue, then by day. It is what
// keeps the converter's open files, and the paths it reports, in one
// order that does not depend on the order rows happened to arrive in.
func (k PartitionKey) Compare(other PartitionKey) int {
	switch {
	case k.VenueID != other.VenueID:
		if k.VenueID < other.VenueID {
			return -1
		}
		return 1
	case k.Day != other.Day:
		if k.Day < other.Day {
			return -1
		}
		return 1
	}
	return 0
}

// floorDiv divides rounding towards negative infinity. Go's / truncates
// towards zero, which would put every timestamp in the last hours before
// the Unix epoch into day 0 alongside the first hours after it.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
