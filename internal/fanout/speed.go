package fanout

import (
	"math"
	"math/bits"
)

// Speed is an exact rational replay-speed multiplier, num/den: 1/1 is
// real time, 10000/1 is ten thousand times real time. It is a rational
// and never a float64. Go may fuse floating-point multiply-add across
// statements, and the compiler emits FMA on arm64, ppc64, s390x and
// riscv64 but not on amd64, so the same source expression can round
// differently depending on the build's target architecture. Record
// content stays safe either way, but pacing timing decides when a Drop
// subscriber gets lapped, so an architecture-dependent rounding
// difference would make Drop-subscriber gap output architecture-
// dependent too — a real, user-visible nondeterminism, not a
// theoretical one. See docs/clock.md.
//
// There is no float input to this package at all, and no float
// boundary anywhere else in the project either: cmd/replayd's gRPC
// Subscribe request carries speed_num and speed_den as two int64
// fields, which go straight to NewSpeed. A double on the wire would
// reintroduce exactly the rounding hazard this type exists to remove,
// one layer out. See api/replay.proto and docs/clock.md.
type Speed struct {
	num int64
	den int64
}

const (
	// maxSpeedTerm bounds each of Speed's reduced terms. It keeps
	// dt*den — the 128-bit product DeliveryTime computes — comfortably
	// representable for any real dataset span, well before the ratio
	// bounds below are even considered.
	maxSpeedTerm = 1 << 31

	// maxSpeedUp and maxSlowdown bound num/den's ratio in each
	// direction. The project's stated range is 1x-10,000x; these leave
	// two orders of magnitude of headroom on the fast side and cover a
	// slow-motion range no real use case needs beyond. Both exist to
	// keep DeliveryTime's quotient inside int64 for any realistic
	// dataset span, not to police what an operator "should" ask for —
	// a speed outside this range would still only saturate
	// DeliveryTime's output rather than misbehave, but is rejected
	// here so a caller finds out at construction time, not partway
	// through a replay.
	maxSpeedUp  = 1_000_000
	maxSlowdown = 1_024
)

// NewSpeed returns num/den, reduced to lowest terms, as a Speed. It
// rejects a non-positive num or den, a reduced term above
// maxSpeedTerm, or a ratio outside [1/maxSlowdown, maxSpeedUp], all as
// ErrInvalidSpeed — there is no valid interpretation of a zero,
// negative, or unbounded speed for this arithmetic to degrade
// gracefully into, so rejecting early keeps invalid input from ever
// reaching DeliveryTime.
//
// Reducing by GCD first is what makes NewSpeed(2, 1) == NewSpeed(10000,
// 5000): cmd/replayd's run manifest records Num()/Den(), and two
// requests for "the same speed" written two different ways must
// produce a byte-identical manifest.
func NewSpeed(num, den int64) (Speed, error) {
	if num <= 0 || den <= 0 {
		return Speed{}, ErrInvalidSpeed
	}

	if g := gcdInt64(num, den); g > 1 {
		num, den = num/g, den/g
	}
	if num > maxSpeedTerm || den > maxSpeedTerm {
		return Speed{}, ErrInvalidSpeed
	}
	// Both products are bounded well inside int64 given the term check
	// above: maxSpeedUp*den <= 10^6 * 2^31 < 2^51, maxSlowdown*num <=
	// 2^10 * 2^31 = 2^41.
	if num > maxSpeedUp*den {
		return Speed{}, ErrInvalidSpeed
	}
	if den > maxSlowdown*num {
		return Speed{}, ErrInvalidSpeed
	}

	return Speed{num: num, den: den}, nil
}

// gcdInt64 returns the greatest common divisor of two positive int64
// values.
func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// Num and Den return s's terms, exactly as constructed. cmd/replayd
// records these in the run manifest, never the float a client sent: a
// manifest holding a float is not reproducible across architectures the
// way an exact num/den pair is.
func (s Speed) Num() int64 { return s.num }
func (s Speed) Den() int64 { return s.den }

// DeliveryTime returns when the record with exchange timestamp ts is
// scheduled for delivery, given that the record with exchange timestamp
// base was delivered at t0:
//
//	t0 + (ts - base) * den / num
//
// computed with a 128-bit intermediate product and no float anywhere. A
// ts at or before base returns t0 unchanged — the anchor record and
// everything before it are always due immediately. A schedule that would
// not fit in an int64 saturates at math.MaxInt64 rather than wrapping;
// see docs/clock.md for why a per-record scheduling path clamps instead
// of returning an error.
func (s Speed) DeliveryTime(t0, base, ts int64) int64 {
	dt := ts - base
	if dt <= 0 {
		return t0
	}

	hi, lo := bits.Mul64(uint64(dt), uint64(s.den))
	// bits.Div64 panics if the divisor is zero or if the quotient would
	// overflow 64 bits — precisely "y == 0 or y <= hi" per its own doc
	// comment, with y = uint64(s.num) here. This one comparison closes
	// both: for the overflow case it is that exact predicate, and for
	// num == 0 (reachable only through the zero Speed{} value, since
	// NewSpeed rejects it) hi >= 0 is always true for an unsigned hi, so
	// the saturating return fires before bits.Div64 is ever called. Do
	// not "simplify" this by adding a separate num > 0 check first.
	if hi >= uint64(s.num) {
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, uint64(s.num))
	if q > uint64(math.MaxInt64) {
		return math.MaxInt64
	}

	d := int64(q)
	if t0 > math.MaxInt64-d {
		return math.MaxInt64
	}
	return t0 + d
}
