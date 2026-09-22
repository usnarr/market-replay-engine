package bench

import "slices"

// The partition set is a sorted slice, so the order venues enter the
// merge tree is a function of the data alone.
type partition struct {
	venueID uint16
	records int
}

func totalRecordsSorted(parts []partition) int {
	total := 0
	for _, p := range parts {
		total += p.records
	}
	return total
}

func sortPartitions(parts []partition) {
	slices.SortFunc(parts, func(a, b partition) int {
		return int(a.venueID) - int(b.venueID)
	})
}

// lookup only, never ranged: iteration order never enters this function.
func recordCount(m map[uint16]int, venueID uint16) (int, bool) {
	n, ok := m[venueID]
	return n, ok
}
