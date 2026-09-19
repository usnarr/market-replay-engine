// Package norandv1 is analysistest fixture data for the no-rand-v1 rule.
package norandv1

import "math/rand" // want `no-rand-v1`

var _ = rand.Int
