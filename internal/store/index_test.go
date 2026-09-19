package store

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// goldenTimeIndex is a four-block index whose entries repeat. Two blocks
// start on the same timestamp, which is what a long run of equal
// exchange_ts values looks like from the index's point of view.
var goldenTimeIndex = []int64{100, 200, 200, 400}

func TestTimeIndexRoundTrip(t *testing.T) {
	buf := make([]byte, len(goldenTimeIndex)*indexEntrySize)
	got := make([]int64, len(goldenTimeIndex))

	encodeTimeIndex(buf, goldenTimeIndex)
	err := decodeTimeIndex(got, buf)

	if err != nil {
		t.Fatalf("decodeTimeIndex() error = %v, want nil", err)
	}
	if diff := cmp.Diff(goldenTimeIndex, got); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

func TestSnapshotIndexRoundTrip(t *testing.T) {
	want := []uint64{0, 5, 11, 12}
	buf := make([]byte, len(want)*indexEntrySize)
	got := make([]uint64, len(want))

	encodeSnapshotIndex(buf, want)
	err := decodeSnapshotIndex(got, buf, 13)

	if err != nil {
		t.Fatalf("decodeSnapshotIndex() error = %v, want nil", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

func TestDecodeTimeIndexRejects(t *testing.T) {
	buf := make([]byte, len(goldenTimeIndex)*indexEntrySize)
	encodeTimeIndex(buf, goldenTimeIndex)

	t.Run("region_shorter_than_the_entry_count", func(t *testing.T) {
		err := decodeTimeIndex(make([]int64, len(goldenTimeIndex)), buf[:len(buf)-1])

		if !errors.Is(err, ErrShortIndex) {
			t.Errorf("decodeTimeIndex() error = %v, want %v", err, ErrShortIndex)
		}
	})

	t.Run("entries_that_decrease", func(t *testing.T) {
		bad := make([]byte, len(buf))
		copy(bad, buf)
		encodeTimeIndex(bad, []int64{100, 200, 199, 400})

		err := decodeTimeIndex(make([]int64, 4), bad)

		if !errors.Is(err, ErrIndexOrder) {
			t.Errorf("decodeTimeIndex() error = %v, want %v", err, ErrIndexOrder)
		}
	})

	t.Run("equal_adjacent_entries_are_accepted", func(t *testing.T) {
		// A timestamp run longer than one block is normal data.
		err := decodeTimeIndex(make([]int64, 4), buf)

		if err != nil {
			t.Errorf("decodeTimeIndex() error = %v, want nil", err)
		}
	})
}

func TestDecodeSnapshotIndexRejects(t *testing.T) {
	tests := []struct {
		name        string
		entries     []uint64
		recordCount uint64
		want        error
	}{
		{
			name:        "entries_that_decrease",
			entries:     []uint64{0, 5, 4},
			recordCount: 13,
			want:        ErrIndexOrder,
		},
		{
			name:        "the_same_record_listed_twice",
			entries:     []uint64{0, 5, 5},
			recordCount: 13,
			want:        ErrIndexOrder,
		},
		{
			name:        "an_entry_past_the_last_record",
			entries:     []uint64{0, 5, 13},
			recordCount: 13,
			want:        ErrIndexRange,
		},
		{
			name:        "any_entry_in_a_file_with_no_records",
			entries:     []uint64{0},
			recordCount: 0,
			want:        ErrIndexRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, len(tt.entries)*indexEntrySize)
			encodeSnapshotIndex(buf, tt.entries)

			err := decodeSnapshotIndex(make([]uint64, len(tt.entries)), buf, tt.recordCount)

			if !errors.Is(err, tt.want) {
				t.Errorf("decodeSnapshotIndex() error = %v, want %v", err, tt.want)
			}
		})
	}

	t.Run("region_shorter_than_the_entry_count", func(t *testing.T) {
		err := decodeSnapshotIndex(make([]uint64, 2), make([]byte, indexEntrySize), 13)

		if !errors.Is(err, ErrShortIndex) {
			t.Errorf("decodeSnapshotIndex() error = %v, want %v", err, ErrShortIndex)
		}
	})
}

func TestSearchTimeIndex(t *testing.T) {
	tests := []struct {
		name string
		t    int64
		want int
	}{
		{name: "before_every_entry", t: 99, want: 0},
		{name: "exactly_the_first_entry", t: 100, want: 0},
		{name: "between_entries", t: 101, want: 1},
		{name: "the_first_of_two_equal_entries", t: 200, want: 1},
		{name: "just_past_a_repeated_entry", t: 201, want: 3},
		{name: "exactly_the_last_entry", t: 400, want: 3},
		{name: "past_every_entry", t: 401, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := searchTimeIndex(goldenTimeIndex, tt.t)

			if got != tt.want {
				t.Errorf("searchTimeIndex(%d) = %d, want %d", tt.t, got, tt.want)
			}
		})
	}

	t.Run("an_empty_index_has_no_match", func(t *testing.T) {
		got := searchTimeIndex(nil, 100)

		if got != 0 {
			t.Errorf("searchTimeIndex(nil, 100) = %d, want 0", got)
		}
	})
}

func TestSearchSnapshotIndex(t *testing.T) {
	idx := []uint64{0, 5, 11}

	tests := []struct {
		name        string
		recordIndex uint64
		want        uint64
		wantOK      bool
	}{
		{name: "exactly_the_first_entry", recordIndex: 0, want: 0, wantOK: true},
		{name: "between_entries", recordIndex: 4, want: 0, wantOK: true},
		{name: "exactly_an_entry", recordIndex: 5, want: 5, wantOK: true},
		{name: "past_the_last_entry", recordIndex: 99, want: 11, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := searchSnapshotIndex(idx, tt.recordIndex)

			if ok != tt.wantOK {
				t.Fatalf("searchSnapshotIndex(%d) ok = %v, want %v", tt.recordIndex, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("searchSnapshotIndex(%d) = %d, want %d", tt.recordIndex, got, tt.want)
			}
		})
	}

	t.Run("no_snapshot_at_or_before_the_record", func(t *testing.T) {
		_, ok := searchSnapshotIndex([]uint64{5, 11}, 4)

		if ok {
			t.Error("searchSnapshotIndex() ok = true, want false")
		}
	})

	t.Run("an_empty_index_has_no_match", func(t *testing.T) {
		_, ok := searchSnapshotIndex(nil, 0)

		if ok {
			t.Error("searchSnapshotIndex(nil) ok = true, want false")
		}
	})
}
