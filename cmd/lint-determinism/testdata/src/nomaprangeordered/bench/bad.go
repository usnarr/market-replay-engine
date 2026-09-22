// Package bench is analysistest fixture data for the no-map-range-ordered
// rule. bench is in scope because the load harness chooses the order
// venues enter the merge tree and the order subscribers register.
package bench

func venueIDs(m map[uint16]int) []uint16 {
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
