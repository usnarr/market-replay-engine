package main

import "testing"

// day2024 is 2024-01-01T00:00:00Z as a day count from the Unix epoch.
const day2024 = 19723

func TestPartitionOf(t *testing.T) {
	tests := []struct {
		name       string
		venueID    uint16
		exchangeTs int64
		wantDay    int64
		wantFile   string
	}{
		{
			name:     "the_first_nanosecond_of_the_unix_epoch",
			venueID:  7,
			wantDay:  0,
			wantFile: "venue-7-1970-01-01.bin",
		},
		{
			name:       "the_last_nanosecond_of_a_day",
			venueID:    7,
			exchangeTs: nanosPerDay - 1,
			wantDay:    0,
			wantFile:   "venue-7-1970-01-01.bin",
		},
		{
			name:       "the_first_nanosecond_of_the_next_day",
			venueID:    7,
			exchangeTs: nanosPerDay,
			wantDay:    1,
			wantFile:   "venue-7-1970-01-02.bin",
		},
		{
			name:       "one_nanosecond_before_the_epoch_is_the_day_before",
			venueID:    7,
			exchangeTs: -1,
			wantDay:    -1,
			wantFile:   "venue-7-1969-12-31.bin",
		},
		{
			name:       "mid_session_on_a_leap_day",
			venueID:    258,
			exchangeTs: (day2024 + 59) * nanosPerDay,
			wantDay:    day2024 + 59,
			wantFile:   "venue-258-2024-02-29.bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PartitionOf(tt.venueID, tt.exchangeTs)

			if got.VenueID != tt.venueID || got.Day != tt.wantDay {
				t.Errorf("PartitionOf(%d, %d) = %+v, want venue %d day %d", tt.venueID, tt.exchangeTs, got, tt.venueID, tt.wantDay)
			}
			if name := got.FileName(); name != tt.wantFile {
				t.Errorf("PartitionOf(%d, %d).FileName() = %q, want %q", tt.venueID, tt.exchangeTs, name, tt.wantFile)
			}
		})
	}
}

func TestPartitionKeyCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b PartitionKey
		want int
	}{
		{name: "equal_keys", a: PartitionKey{7, 1}, b: PartitionKey{7, 1}, want: 0},
		{name: "a_lower_venue_sorts_first", a: PartitionKey{7, 9}, b: PartitionKey{9, 0}, want: -1},
		{name: "a_higher_venue_sorts_last", a: PartitionKey{9, 0}, b: PartitionKey{7, 9}, want: 1},
		{name: "an_earlier_day_sorts_first_within_a_venue", a: PartitionKey{7, 0}, b: PartitionKey{7, 1}, want: -1},
		{name: "a_later_day_sorts_last_within_a_venue", a: PartitionKey{7, 1}, b: PartitionKey{7, 0}, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Compare(tt.b)

			if got != tt.want {
				t.Errorf("PartitionKey%+v.Compare(%+v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
