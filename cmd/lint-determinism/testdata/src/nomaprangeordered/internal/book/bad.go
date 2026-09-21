// Package book is analysistest fixture data for the no-map-range-ordered
// rule.
package book

func sumValues(m map[string]int) int {
	total := 0
	for _, v := range m { // want `no-map-range-ordered`
		total += v
	}
	return total
}

func keys(m map[string]int) []string {
	var out []string
	for k := range m { // want `no-map-range-ordered`
		out = append(out, k)
	}
	return out
}
