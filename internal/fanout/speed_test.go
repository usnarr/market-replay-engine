package fanout

import (
	"math"
	"math/big"
	"math/rand/v2"
	"testing"

	"replay/internal/allocgate"
)

// sinkInt64 stops the compiler from proving a benchmarked result is
// unused.
var sinkInt64 int64

// deliveryTimeRef is DeliveryTime computed in exact arbitrary-precision
// arithmetic. Test-only: math/big allocates on every operation and never
// belongs on this package's hot path. It floors (truncates toward zero)
// the same way bits.Div64 does for the non-negative operands this
// formula is defined on.
func deliveryTimeRef(t0, base, ts, num, den int64) *big.Int {
	dt := new(big.Int).Sub(big.NewInt(ts), big.NewInt(base))
	if dt.Sign() <= 0 {
		return big.NewInt(t0)
	}
	q := new(big.Int).Mul(dt, big.NewInt(den))
	q.Quo(q, big.NewInt(num))
	return q.Add(q, big.NewInt(t0))
}

func TestSpeedDeliveryTime(t *testing.T) {
	t.Run("matches_the_big_rat_reference_for_a_range_of_triples", func(t *testing.T) {
		const t0 = 1_700_000_000_000_000_000
		cases := []struct {
			num, den int64
		}{
			{1, 1}, {10000, 1}, {1, 1000}, {3, 7}, {7, 3},
		}
		rng := rand.New(rand.NewPCG(1, 2))

		for _, c := range cases {
			s, err := NewSpeed(c.num, c.den)
			if err != nil {
				t.Fatalf("NewSpeed(%d, %d) error = %v, want nil", c.num, c.den, err)
			}
			for _, dt := range []int64{1, 2, 999, 1_000_000, 1_000_000_000} {
				got := s.DeliveryTime(t0, 0, dt)
				want := deliveryTimeRef(t0, 0, dt, c.num, c.den)
				if big.NewInt(got).Cmp(want) != 0 {
					t.Fatalf("DeliveryTime(num=%d den=%d dt=%d) = %d, want %s", c.num, c.den, dt, got, want.String())
				}
			}
			for i := 0; i < 50; i++ {
				dt := int64(rng.Uint64N(1 << 40))
				got := s.DeliveryTime(t0, 0, dt)
				want := deliveryTimeRef(t0, 0, dt, c.num, c.den)
				if big.NewInt(got).Cmp(want) != 0 {
					t.Fatalf("DeliveryTime(num=%d den=%d dt=%d) = %d, want %s", c.num, c.den, dt, got, want.String())
				}
			}
		}
	})

	t.Run("a_timestamp_at_the_anchor_returns_the_anchor_time", func(t *testing.T) {
		s, _ := NewSpeed(10000, 1)
		if got := s.DeliveryTime(500, 1000, 1000); got != 500 {
			t.Errorf("DeliveryTime() = %d, want 500", got)
		}
	})

	t.Run("a_timestamp_before_the_anchor_returns_the_anchor_time", func(t *testing.T) {
		s, _ := NewSpeed(1, 1)
		if got := s.DeliveryTime(500, 1000, 999); got != 500 {
			t.Errorf("DeliveryTime() = %d, want 500", got)
		}
	})

	t.Run("an_int64_overflowing_span_returns_the_anchor_time", func(t *testing.T) {
		s, _ := NewSpeed(1, 1)
		// ts - base overflows int64 and wraps negative, which the dt <= 0
		// guard catches the same way an ordinary negative span does.
		if got := s.DeliveryTime(500, math.MinInt64, math.MaxInt64); got != 500 {
			t.Errorf("DeliveryTime() = %d, want 500", got)
		}
	})

	t.Run("a_schedule_beyond_int64_saturates_instead_of_wrapping", func(t *testing.T) {
		// speed 1/maxSlowdown: the slowest ratio NewSpeed still accepts.
		// Even within the supported range, the largest possible span
		// (math.MaxInt64 nanoseconds, ~292 years) overflows the
		// quotient at this ratio, which is exactly why DeliveryTime
		// saturates instead of trusting bits.Div64 not to panic.
		s, err := NewSpeed(1, maxSlowdown)
		if err != nil {
			t.Fatalf("NewSpeed(1, %d) error = %v, want nil", maxSlowdown, err)
		}
		got := s.DeliveryTime(0, 0, math.MaxInt64)
		if got != math.MaxInt64 {
			t.Errorf("DeliveryTime() = %d, want MaxInt64", got)
		}
	})

	t.Run("the_final_addition_saturates_instead_of_wrapping", func(t *testing.T) {
		// speed 1/1: the quotient itself fits easily, but adding it to a
		// t0 already near MaxInt64 would overflow the return value.
		s, _ := NewSpeed(1, 1)
		got := s.DeliveryTime(math.MaxInt64-10, 0, 1000)
		if got != math.MaxInt64 {
			t.Errorf("DeliveryTime() = %d, want MaxInt64", got)
		}
	})

	t.Run("the_zero_speed_value_never_panics", func(t *testing.T) {
		var s Speed // zero value, never constructed through NewSpeed
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("DeliveryTime() panicked: %v", p)
			}
		}()
		if got := s.DeliveryTime(0, 0, 1); got != math.MaxInt64 {
			t.Errorf("DeliveryTime() = %d, want MaxInt64", got)
		}
	})

	t.Run("one_times_speed_is_the_identity_on_the_span", func(t *testing.T) {
		s, _ := NewSpeed(1, 1)
		if got := s.DeliveryTime(1000, 0, 12345); got != 1000+12345 {
			t.Errorf("DeliveryTime() = %d, want %d", got, 1000+12345)
		}
	})

	t.Run("delivery_time_scales_exactly_with_speed", func(t *testing.T) {
		slow, _ := NewSpeed(1, 1)
		fast, _ := NewSpeed(10000, 1)
		const dt = 50_000_000 // divisible by 10000, no truncation noise

		dSlow := slow.DeliveryTime(0, 0, dt) // = dt
		dFast := fast.DeliveryTime(0, 0, dt) // = dt / 10000

		if dSlow != dt {
			t.Fatalf("1x DeliveryTime() = %d, want %d", dSlow, dt)
		}
		if dFast != dt/10000 {
			t.Fatalf("10000x DeliveryTime() = %d, want %d", dFast, dt/10000)
		}
	})
}

func BenchmarkSpeedDeliveryTime(b *testing.B) {
	s, err := NewSpeed(10000, 1)
	if err != nil {
		b.Fatalf("NewSpeed() error = %v, want nil", err)
	}
	call := func() { sinkInt64 = s.DeliveryTime(0, 0, 1_000_000_000) }
	allocgate.AssertZero(b, call)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		call()
	}
}

func TestNewSpeed(t *testing.T) {
	tests := []struct {
		name     string
		num, den int64
		wantErr  bool
	}{
		{"rejects_a_zero_numerator", 0, 1, true},
		{"rejects_a_zero_denominator", 1, 0, true},
		{"rejects_a_negative_numerator", -1, 1, true},
		{"rejects_a_negative_denominator", 1, -1, true},
		{"rejects_min_int64", math.MinInt64, 1, true},
		{"rejects_a_term_above_the_bound_after_reduction", 1 << 32, 1, true},
		{"rejects_a_speed_up_beyond_the_supported_range", maxSpeedUp + 1, 1, true},
		{"rejects_a_slowdown_beyond_the_supported_range", 1, maxSlowdown + 1, true},
		{"accepts_one_times", 1, 1, false},
		{"accepts_ten_thousand_times", 10000, 1, false},
		{"accepts_a_fractional_speed", 1, 2, false},
		{"accepts_the_maximum_supported_speed_up", maxSpeedUp, 1, false},
		{"accepts_the_maximum_supported_slowdown", 1, maxSlowdown, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSpeed(tt.num, tt.den)
			if tt.wantErr {
				if err != ErrInvalidSpeed {
					t.Fatalf("NewSpeed(%d, %d) error = %v, want ErrInvalidSpeed", tt.num, tt.den, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSpeed(%d, %d) error = %v, want nil", tt.num, tt.den, err)
			}
		})
	}

	t.Run("reduces_equal_speeds_to_the_same_value", func(t *testing.T) {
		a, err := NewSpeed(2, 1)
		if err != nil {
			t.Fatalf("NewSpeed(2, 1) error = %v, want nil", err)
		}
		b, err := NewSpeed(10000, 5000)
		if err != nil {
			t.Fatalf("NewSpeed(10000, 5000) error = %v, want nil", err)
		}
		if a != b {
			t.Errorf("NewSpeed(2, 1) = %+v, NewSpeed(10000, 5000) = %+v, want equal", a, b)
		}
		if a.Num() != 2 || a.Den() != 1 {
			t.Errorf("reduced terms = (%d, %d), want (2, 1)", a.Num(), a.Den())
		}
	})
}
