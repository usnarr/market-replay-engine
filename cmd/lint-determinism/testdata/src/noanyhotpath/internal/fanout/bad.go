// Package fanout is analysistest fixture data for the no-any-hot-path
// rule.
package fanout

func publish(v any) { // want `no-any-hot-path`
	_ = v
}

func fetch() interface{} { // want `no-any-hot-path`
	return nil
}

func widen(v int) any { // want `no-any-hot-path`
	return v
}

var boxed any = 42

func unwrap() int {
	n, ok := boxed.(int) // want `no-any-hot-path`
	if !ok {
		return 0
	}
	return n
}

func switchOn() int {
	switch x := boxed.(type) { // want `no-any-hot-path`
	case int:
		return x
	default:
		return 0
	}
}
