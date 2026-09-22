package convert

import "slices"

// The partition set is a sorted slice, so the order files are written in
// is a function of the data alone.
type partition struct {
	venueID uint16
	path    string
}

func paths(parts []partition) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p.path)
	}
	return out
}

func find(parts []partition, venueID uint16) (partition, bool) {
	i, ok := slices.BinarySearchFunc(parts, venueID, func(p partition, id uint16) int {
		return int(p.venueID) - int(id)
	})
	if !ok {
		return partition{}, false
	}
	return parts[i], true
}

// lookup only, never ranged: iteration order never enters this function.
func priceScale(m map[uint16]int64, venueID uint16) (int64, bool) {
	v, ok := m[venueID]
	return v, ok
}
