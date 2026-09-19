// Package clock is analysistest fixture data for the no-time-now rule's one
// exception: internal/clock/realclock.go is where time.Now is allowed to
// live in the real repository.
package clock

import "time"

// RealClock reads the wall clock. This file is the one place time.Now is
// not flagged.
type RealClock struct{}

func (RealClock) Now() int64 {
	return time.Now().UnixNano()
}
