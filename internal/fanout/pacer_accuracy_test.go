package fanout

import (
	"sort"
	"testing"

	"replay/internal/clock"
)

const (
	// accuracyRecords and accuracySourceSpacing size the synthetic
	// stream BenchmarkPacingAccuracy paces: at accuracySpeed (10x),
	// accuracyRecords * accuracySourceSpacing / accuracySpeed is close
	// to one second of real wall-clock time, which is what lets the
	// benchmark harness settle at N=1 the same way allocgate.AssertZero
	// does — one call already costs close to the default -benchtime.
	accuracyRecords       = 1000
	accuracySourceSpacing = 10_000_000 // 10ms of source time per record
	accuracySpeed         = 10

	// rateToleranceThousandths bounds |observedSpan-scheduledSpan| as a
	// fraction of scheduledSpan, in thousandths (10 == 1.0%). Set from
	// the distribution recorded in BENCHMARKS.md's Pacing accuracy
	// section: 20 runs on the machine that section documents measured a
	// worst case of 0.7 thousandths (0.07%), with a rare window-
	// measurement noise spike; this leaves roughly 28x margin above
	// that. If this benchmark starts flaking, widen it and record the
	// new distribution there — never delete the assertion it backs.
	rateToleranceThousandths = 20
)

// BenchmarkPacingAccuracy paces a synthetic 1000-record stream at 10x
// real time and checks the result against the schedule
// Speed.DeliveryTime computes. It is a benchmark, not a Test,
// specifically so it never runs under make test, the CI test/race
// jobs, or make determinism: none of those pass -bench, and a
// benchmark's body never executes without it (see
// testing.runBenchmarks). This is a real-clock, real-wall-time
// measurement — the one place in this package's test suite that is
// inherently timing-sensitive — and Q7's own answer is what it checks:
// 1x-10,000x is a target rate, not a per-event scheduling guarantee, so
// only two things are asserted as hard failures:
//
//   - never more than one window early: release-batching (see Wait's own
//     doc comment) intentionally lets a record release up to one
//     measured window ahead of its own schedule, so that is the actual
//     bound to check, not zero. Earlier than that is a pacer bug;
//     lateness is the operating system's scheduler, which this project
//     does not control.
//   - rate accuracy over the whole run: the observed span from first to
//     last release must track the scheduled span within
//     rateToleranceThousandths.
//
// Per-event lateness (max/p99/mean) is reported via b.ReportMetric,
// never asserted: that is exactly the quantity Q7 says is not a
// guarantee, and asserting a tight per-event bound would make this
// benchmark flake on a busy CI runner for reasons that have nothing to
// do with a real regression.
func BenchmarkPacingAccuracy(b *testing.B) {
	rc := clock.RealClock{}
	s, err := NewSpeed(accuracySpeed, 1)
	if err != nil {
		b.Fatalf("NewSpeed() error = %v, want nil", err)
	}
	p := NewPacer(rc, s)
	base := rc.Now()
	p.Start(base)

	lateness := make([]int64, 0, accuracyRecords)
	var firstActual, lastActual int64

	b.ResetTimer()
	for i := 0; i < accuracyRecords; i++ {
		ts := base + int64(i+1)*accuracySourceSpacing
		scheduled := s.DeliveryTime(base, base, ts)

		if ok := p.Wait(ts); !ok {
			b.Fatalf("Wait(%d) = false, want true", ts)
		}
		actual := rc.Now()
		if i == 0 {
			firstActual = actual
		}
		lastActual = actual

		late := actual - scheduled
		if late < -p.Window() {
			b.Fatalf("record %d released %dns early (scheduled=%d, actual=%d), more than one window (%dns): release-batching must never release earlier than that", i, -late, scheduled, actual, p.Window())
		}
		lateness = append(lateness, late)
	}
	b.StopTimer()

	scheduledLast := s.DeliveryTime(base, base, base+int64(accuracyRecords)*accuracySourceSpacing)
	scheduledSpan := scheduledLast - scheduledFirst(s, base)
	observedSpan := lastActual - firstActual
	rateErrThousandths := abs64(observedSpan-scheduledSpan) * 1000 / max64(scheduledSpan, 1)
	if rateErrThousandths > rateToleranceThousandths {
		b.Fatalf("rate error %d.%02d%% over %d records, want at most %d.%02d%% (scheduledSpan=%d observedSpan=%d)",
			rateErrThousandths/10, rateErrThousandths%10*10, accuracyRecords,
			rateToleranceThousandths/10, rateToleranceThousandths%10*10, scheduledSpan, observedSpan)
	}

	sort.Slice(lateness, func(i, j int) bool { return lateness[i] < lateness[j] })
	var sum int64
	for _, l := range lateness {
		sum += l
	}
	p99 := lateness[len(lateness)*99/100]
	max := lateness[len(lateness)-1]
	mean := sum / int64(len(lateness))

	b.ReportMetric(float64(max), "max_late_ns")
	b.ReportMetric(float64(p99), "p99_late_ns")
	b.ReportMetric(float64(mean), "mean_late_ns")
	b.ReportMetric(float64(p.Window()), "window_ns")
	b.ReportMetric(float64(rateErrThousandths)/10, "rate_err_pct")
}

// scheduledFirst is the schedule's own delivery time for the first
// record, used as the span's start so the rate check is not thrown off
// by however long Start's own anchoring took.
func scheduledFirst(s Speed, base int64) int64 {
	return s.DeliveryTime(base, base, base+accuracySourceSpacing)
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
