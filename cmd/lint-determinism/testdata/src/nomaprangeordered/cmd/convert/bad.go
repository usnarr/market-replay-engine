// Package convert is analysistest fixture data for the no-map-range-ordered
// rule. cmd/convert is in scope because it must produce byte-identical
// artifacts.
package convert

func venueIDs(m map[uint16]string) []uint16 {
	var out []uint16
	for id := range m { // want `no-map-range-ordered`
		out = append(out, id)
	}
	return out
}

func totalRecords(m map[uint16]int) int {
	total := 0
	for _, n := range m { // want `no-map-range-ordered`
		total += n
	}
	return total
}
