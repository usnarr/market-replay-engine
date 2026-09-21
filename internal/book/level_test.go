package book

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

func TestSideUpsert(t *testing.T) {
	tests := []struct {
		name    string
		initial []store.Level
		price   int64
		size    int64
		want    []store.Level
		wantErr error
	}{
		{
			name:  "insert_into_an_empty_side",
			price: 100, size: 10,
			want: []store.Level{{Price: 100, Size: 10}},
		},
		{
			name:    "insert_below_the_lowest_existing_price",
			initial: []store.Level{{Price: 100, Size: 10}},
			price:   90, size: 5,
			want: []store.Level{{Price: 90, Size: 5}, {Price: 100, Size: 10}},
		},
		{
			name:    "insert_above_the_highest_existing_price",
			initial: []store.Level{{Price: 100, Size: 10}},
			price:   110, size: 5,
			want: []store.Level{{Price: 100, Size: 10}, {Price: 110, Size: 5}},
		},
		{
			name:    "insert_between_two_existing_prices",
			initial: []store.Level{{Price: 90, Size: 1}, {Price: 110, Size: 1}},
			price:   100, size: 5,
			want: []store.Level{{Price: 90, Size: 1}, {Price: 100, Size: 5}, {Price: 110, Size: 1}},
		},
		{
			name:    "update_an_existing_price_in_place",
			initial: []store.Level{{Price: 90, Size: 1}, {Price: 100, Size: 10}, {Price: 110, Size: 1}},
			price:   100, size: 99,
			want: []store.Level{{Price: 90, Size: 1}, {Price: 100, Size: 99}, {Price: 110, Size: 1}},
		},
		{
			name:    "zero_size_removes_an_existing_level",
			initial: []store.Level{{Price: 90, Size: 1}, {Price: 100, Size: 10}, {Price: 110, Size: 1}},
			price:   100, size: 0,
			want: []store.Level{{Price: 90, Size: 1}, {Price: 110, Size: 1}},
		},
		{
			name:    "zero_size_on_the_only_level_empties_the_side",
			initial: []store.Level{{Price: 100, Size: 10}},
			price:   100, size: 0,
			want: []store.Level{},
		},
		{
			name:    "zero_size_on_a_missing_price_is_an_error",
			initial: []store.Level{{Price: 90, Size: 1}},
			price:   100, size: 0,
			wantErr: ErrLevelNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &side{levels: append([]store.Level(nil), tt.initial...)}

			err := s.upsert(tt.price, tt.size)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Fatalf("upsert(%d, %d) error = %v, want %v", tt.price, tt.size, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("upsert(%d, %d) error = %v, want nil", tt.price, tt.size, err)
			}
			if diff := cmp.Diff(tt.want, s.snapshot()); diff != "" {
				t.Errorf("upsert(%d, %d) levels mismatch (-want +got):\n%s", tt.price, tt.size, diff)
			}
		})
	}
}

func TestSideReset(t *testing.T) {
	tests := []struct {
		name    string
		levels  []store.Level
		want    []store.Level
		wantErr error
	}{
		{
			name:   "sorts_unsorted_levels_ascending_by_price",
			levels: []store.Level{{Price: 110, Size: 1}, {Price: 90, Size: 2}, {Price: 100, Size: 3}},
			want:   []store.Level{{Price: 90, Size: 2}, {Price: 100, Size: 3}, {Price: 110, Size: 1}},
		},
		{
			name:   "an_empty_input_resets_to_an_empty_side",
			levels: nil,
			want:   []store.Level{},
		},
		{
			name:    "a_duplicate_price_is_an_error",
			levels:  []store.Level{{Price: 100, Size: 1}, {Price: 100, Size: 2}},
			wantErr: ErrDuplicateLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &side{levels: []store.Level{{Price: 1, Size: 1}}}

			err := s.reset(tt.levels)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Fatalf("reset(%v) error = %v, want %v", tt.levels, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("reset(%v) error = %v, want nil", tt.levels, err)
			}
			if diff := cmp.Diff(tt.want, s.snapshot()); diff != "" {
				t.Errorf("reset(%v) levels mismatch (-want +got):\n%s", tt.levels, diff)
			}
		})
	}
}
