package merge

// lookup only, never ranged: iteration order never enters this function.
func lookup(m map[string]int, key string) (int, bool) {
	v, ok := m[key]
	return v, ok
}

func sumSlice(xs []int) int {
	total := 0
	for _, v := range xs {
		total += v
	}
	return total
}

func sumOrdered(order []string, m map[string]int) int {
	total := 0
	for _, k := range order {
		total += m[k]
	}
	return total
}
