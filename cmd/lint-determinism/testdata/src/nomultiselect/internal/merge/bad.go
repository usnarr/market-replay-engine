// Package merge is analysistest fixture data for the no-multi-select rule.
package merge

func pickEither(a, b chan int) int {
	select { // want `no-multi-select`
	case v := <-a:
		return v
	case v := <-b:
		return v
	}
}

func pickAnyOfThree(a, b, c chan int) int {
	select { // want `no-multi-select`
	case v := <-a:
		return v
	case v := <-b:
		return v
	case v := <-c:
		return v
	}
}
