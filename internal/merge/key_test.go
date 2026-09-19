package merge

import (
	"testing"

	"replay/internal/store"
)

func TestCompareKey(t *testing.T) {
	base := mk(10, 2, 3, 4)

	tests := []struct {
		name string
		a    Key
		b    Key
		want int
	}{
		{name: "identical_keys_are_equal", a: base, b: base, want: 0},
		{name: "exchange_ts_outranks_every_later_field", a: mk(9, 9, 9, 9), b: base, want: -1},
		{name: "venue_id_breaks_an_exchange_ts_tie", a: mk(10, 1, 9, 9), b: base, want: -1},
		{name: "sequence_number_breaks_a_venue_id_tie", a: mk(10, 2, 2, 9), b: base, want: -1},
		{name: "instrument_id_breaks_the_last_tie", a: mk(10, 2, 3, 3), b: base, want: -1},
		{name: "a_negative_exchange_ts_sorts_first", a: mk(-1, 2, 3, 4), b: base, want: -1},
		{name: "the_sentinel_sorts_after_every_real_key", a: base, b: SentinelKey, want: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareKey(tt.a, tt.b)
			reversed := compareKey(tt.b, tt.a)

			if sign(got) != tt.want {
				t.Errorf("compareKey(%v, %v) = %d, want sign %d", tt.a, tt.b, got, tt.want)
			}
			if sign(reversed) != -tt.want {
				t.Errorf("compareKey(%v, %v) = %d, want sign %d", tt.b, tt.a, reversed, -tt.want)
			}
		})
	}
}

func TestKeyOf(t *testing.T) {
	rec := store.Record{
		ExchangeTs:     10,
		SequenceNumber: 3,
		InstrumentID:   4,
		VenueID:        2,
		RecordType:     store.RecordTypeDelta,
		Price:          100,
		Size:           5,
	}

	got := keyOf(rec)

	if want := mk(10, 2, 3, 4); got != want {
		t.Errorf("keyOf(%+v) = %v, want %v", rec, got, want)
	}
}

func TestSentinelKeyIsTheLargestKey(t *testing.T) {
	// Every cursor that runs out holds this key, so anything that could
	// outrank it would win a match it must always lose.
	larger := []Key{
		mk(SentinelKey.ExchangeTs, SentinelKey.VenueID, SentinelKey.SequenceNumber, SentinelKey.InstrumentID-1),
		mk(SentinelKey.ExchangeTs, SentinelKey.VenueID, SentinelKey.SequenceNumber-1, SentinelKey.InstrumentID),
		mk(SentinelKey.ExchangeTs, SentinelKey.VenueID-1, SentinelKey.SequenceNumber, SentinelKey.InstrumentID),
		mk(SentinelKey.ExchangeTs-1, SentinelKey.VenueID, SentinelKey.SequenceNumber, SentinelKey.InstrumentID),
	}

	for _, key := range larger {
		if compareKey(key, SentinelKey) >= 0 {
			t.Errorf("compareKey(%v, sentinel) = %d, want negative", key, compareKey(key, SentinelKey))
		}
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	}
	return 0
}
