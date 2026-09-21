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
// There is no float input to this package at all. The one float
// boundary in this project is cmd/replayd's gRPC speed request field,
// which converts to a Speed and validates once, at that one boundary.
type Speed struct {
	num int64
	den int64
}

// NewSpeed returns num/den as a Speed, or ErrInvalidSpeed if either is
// not positive. A later commit extends this with GCD reduction (so
// manifest-equivalent speeds compare equal) and bounds on the ratio's
// extremes; this is the minimum a Speed needs to be safe for
// DeliveryTime to use.
func NewSpeed(num, den int64) (Speed, error) {
	if num <= 0 || den <= 0 {
		return Speed{}, ErrInvalidSpeed
	}
	return Speed{num: num, den: den}, nil
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
