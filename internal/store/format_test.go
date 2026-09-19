package store

import "testing"

func TestRecordLayout(t *testing.T) {
	fields := []struct {
		name string
		off  int
		size int
	}{
		{"exchange_ts", offExchangeTs, 8},
		{"sequence_number", offSequenceNumber, 8},
		{"instrument_id", offInstrumentID, 4},
		{"venue_id", offVenueID, 2},
		{"record_type", offRecordType, 1},
		{"side_flags", offSideFlags, 1},
		{"price", offPrice, 8},
		{"size", offSize, 8},
		{"blob_offset", offBlobOffset, 8},
		{"blob_len", offBlobLen, 4},
		{"level_count", offLevelCount, 2},
		{"reserved", offReserved, lenReserved},
	}

	t.Run("fields_tile_the_record_with_no_gap_or_overlap", func(t *testing.T) {
		next := 0
		for _, f := range fields {
			if f.off != next {
				t.Errorf("%s starts at %d, want %d", f.name, f.off, next)
			}
			next = f.off + f.size
		}

		if next != RecordSize {
			t.Errorf("fields cover %d bytes, want RecordSize = %d", next, RecordSize)
		}
	})

	t.Run("ordering_key_fields_lead_the_record", func(t *testing.T) {
		// A key comparison must touch only the leading bytes, so the four
		// key fields are the first four fields, in key order.
		want := []string{"exchange_ts", "sequence_number", "instrument_id", "venue_id"}

		for i, name := range want {
			if fields[i].name != name {
				t.Errorf("field %d is %s, want %s", i, fields[i].name, name)
			}
		}
	})

	t.Run("record_types_are_dense_from_zero", func(t *testing.T) {
		got := []uint8{RecordTypeDelta, RecordTypeTrade, RecordTypeSnapshotPointer}

		for i, rt := range got {
			if int(rt) != i {
				t.Errorf("record type %d has value %d, want %d", i, rt, i)
			}
		}
		if len(got) != recordTypeCount {
			t.Errorf("recordTypeCount = %d, want %d", recordTypeCount, len(got))
		}
	})

	t.Run("side_values_fit_the_defined_flag_bits", func(t *testing.T) {
		for _, side := range []uint8{SideBid, SideAsk} {
			if side&^sideFlagsMask != 0 {
				t.Errorf("side value %#x sets a bit outside sideFlagsMask %#x", side, sideFlagsMask)
			}
		}
	})
}

func TestCompareKey(t *testing.T) {
	base := Record{ExchangeTs: 100, VenueID: 7, SequenceNumber: 500, InstrumentID: 42}

	tests := []struct {
		name string
		a, b Record
		want int
	}{
		{
			name: "equal_keys_compare_equal",
			a:    base,
			b:    base,
			want: 0,
		},
		{
			name: "fields_outside_the_key_do_not_affect_order",
			a:    base,
			b:    Record{ExchangeTs: 100, VenueID: 7, SequenceNumber: 500, InstrumentID: 42, Price: 99, Size: 99, RecordType: RecordTypeTrade},
			want: 0,
		},
		{
			name: "exchange_ts_outranks_every_other_field",
			a:    Record{ExchangeTs: 99, VenueID: 9, SequenceNumber: 999, InstrumentID: 99},
			b:    base,
			want: -1,
		},
		{
			name: "venue_id_breaks_an_exchange_ts_tie",
			a:    Record{ExchangeTs: 100, VenueID: 6, SequenceNumber: 999, InstrumentID: 99},
			b:    base,
			want: -1,
		},
		{
			name: "sequence_number_breaks_a_venue_id_tie",
			a:    Record{ExchangeTs: 100, VenueID: 7, SequenceNumber: 499, InstrumentID: 99},
			b:    base,
			want: -1,
		},
		{
			name: "instrument_id_breaks_a_sequence_number_tie",
			a:    Record{ExchangeTs: 100, VenueID: 7, SequenceNumber: 500, InstrumentID: 41},
			b:    base,
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareKey(tt.a, tt.b)
			rev := compareKey(tt.b, tt.a)

			if sign(got) != tt.want {
				t.Errorf("compareKey(a, b) = %d, want sign %d", got, tt.want)
			}
			if sign(rev) != -tt.want {
				t.Errorf("compareKey(b, a) = %d, want sign %d", rev, -tt.want)
			}
		})
	}
}

// sign reduces a comparison result to -1, 0 or 1 so a test can assert on
// the ordering without depending on the magnitude.
func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}
