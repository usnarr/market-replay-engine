//go:build !race

package allocgate

// raceEnabled is true when this binary was built with -race. See
// AssertZero's doc comment for why that disables the gate.
const raceEnabled = false
