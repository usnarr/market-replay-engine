// Package cmd is outside the rule's scope (internal/merge, internal/fanout
// only). A multi-clause select here is not flagged.
package cmd

func pickEither(a, b chan int) int {
	select {
	case v := <-a:
		return v
	case v := <-b:
		return v
	}
}
