package notimenow

// clock is a lookalike with its own Now method. Confirms the rule resolves
// the call through type information rather than matching the selector name
// alone -- clock.Now() here is not time.Now.
type clock struct{ frozen int64 }

func (c clock) Now() int64 { return c.frozen }

func useClock(c clock) int64 {
	return c.Now()
}
