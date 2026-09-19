// Package merge is analysistest fixture data for the no-fmt-sprint-hot-path
// rule.
package merge

import "fmt"

func describe(n int) string {
	return fmt.Sprint(n) // want `no-fmt-sprint-hot-path`
}

func describef(n int) string {
	return fmt.Sprintf("n=%d", n) // want `no-fmt-sprint-hot-path`
}

func fail(n int) error {
	return fmt.Errorf("bad value: %d", n) // want `no-fmt-sprint-hot-path`
}
