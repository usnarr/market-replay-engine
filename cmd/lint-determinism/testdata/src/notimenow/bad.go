// Package notimenow is analysistest fixture data for the no-time-now rule.
package notimenow

import "time"

func now() time.Time {
	return time.Now() // want `no-time-now`
}

func since(t time.Time) time.Duration {
	return time.Since(t) // want `no-time-now`
}

func after() <-chan time.Time {
	return time.After(time.Second) // want `no-time-now`
}

func tick() <-chan time.Time {
	return time.Tick(time.Second) // want `no-time-now`
}

func sleep() {
	time.Sleep(time.Second) // want `no-time-now`
}

func newTimer() *time.Timer {
	return time.NewTimer(time.Second) // want `no-time-now`
}

func newTicker() *time.Ticker {
	return time.NewTicker(time.Second) // want `no-time-now`
}
