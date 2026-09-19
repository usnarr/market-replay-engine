package merge

func readOne(a chan int) int {
	select {
	case v := <-a:
		return v
	}
}

// readOrDefault has one communication clause plus default. Not a runtime
// choice between two ready channels, so it is not flagged.
func readOrDefault(a chan int) (int, bool) {
	select {
	case v := <-a:
		return v, true
	default:
		return 0, false
	}
}
