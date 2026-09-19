package norandv1

// math/rand/v2 is a distinct import path from math/rand; confirms the rule
// matches the exact path, not a prefix.
import "math/rand/v2"

var _ = rand.Int
