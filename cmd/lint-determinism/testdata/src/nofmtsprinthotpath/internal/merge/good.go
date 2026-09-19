package merge

import (
	"fmt"
	"strconv"
)

// strconv, not fmt, is the hot-path way to format a number.
func describeInt(n int) string {
	return strconv.Itoa(n)
}

// fmt.Println is not in the banned list; only Sprint, Sprintf, and Errorf
// are.
func log(n int) {
	fmt.Println(n)
}
