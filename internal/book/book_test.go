package book

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

func TestBookApplySnapshot(t *testing.T) {
	tests := []struct {
		name     string
		levels   []store.Level
		bidCount int
		wantBids []store.Level
		wantAsks []store.Level
		wantErr  error
	}{
		{
			name:     "loads_bids_and_asks_from_a_decoded_snapshot",
			levels:   []store.Level{{Price: 99, Size: 5}, {Price: 100, Size: 10}, {Price: 101, Size: 8}, {Price: 102, Size: 3}},
			bidCount: 2,
			wantBids: []store.Level{{Price: 100, Size: 10}, {Price: 99, Size: 5}},
			wantAsks: []store.Level{{Price: 101, Size: 8}, {Price: 102, Size: 3}},
		},
		{
			name:     "sorts_unsorted_input_levels_by_price",
			levels:   []store.Level{{Price: 100, Size: 10}, {Price: 99, Size: 5}, {Price: 102, Size: 3}, {Price: 101, Size: 8}},
			bidCount: 2,
			wantBids: []store.Level{{Price: 100, Size: 10}, {Price: 99, Size: 5}},
			wantAsks: []store.Level{{Price: 101, Size: 8}, {Price: 102, Size: 3}},
		},
		{
			name:     "zero_bid_count_puts_every_level_on_the_ask_side",
			levels:   []store.Level{{Price: 101, Size: 8}},
			bidCount: 0,
			wantBids: []store.Level{},
			wantAsks: []store.Level{{Price: 101, Size: 8}},
		},
		{
			name:     "a_negative_bid_count_is_an_error",
			levels:   []store.Level{{Price: 100, Size: 10}},
			bidCount: -1,
			wantErr:  ErrBidCountOutOfRange,
		},
		{
			name:     "a_bid_count_larger_than_the_level_slice_is_an_error",
			levels:   []store.Level{{Price: 100, Size: 10}},
			bidCount: 2,
			wantErr:  ErrBidCountOutOfRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBook(1)

			err := b.ApplySnapshot(tt.levels, tt.bidCount)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("ApplySnapshot() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplySnapshot() error = %v, want nil", err)
			}
			if diff := cmp.Diff(tt.wantBids, b.Bids()); diff != "" {
				t.Errorf("Bids() mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantAsks, b.Asks()); diff != "" {
				t.Errorf("Asks() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func deltaAt(instrumentID uint32, sideFlag uint8, price, size int64) store.Record {
	return store.Record{
		ExchangeTs:   1,
		InstrumentID: instrumentID,
		VenueID:      1,
		RecordType:   store.RecordTypeDelta,
		SideFlags:    sideFlag,
		Price:        price,
		Size:         size,
	}
}

func TestBookApplyScriptedSequence(t *testing.T) {
	b := NewBook(1)
	snapLevels := []store.Level{{Price: 99, Size: 5}, {Price: 100, Size: 10}, {Price: 101, Size: 8}, {Price: 102, Size: 3}}
	if err := b.ApplySnapshot(snapLevels, 2); err != nil {
		t.Fatalf("ApplySnapshot() error = %v, want nil", err)
	}

	deltas := []store.Record{
		deltaAt(1, store.SideBid, 100, 20), // update in place
		deltaAt(1, store.SideBid, 98, 1),   // insert
		deltaAt(1, store.SideAsk, 101, 0),  // remove
		deltaAt(1, store.SideAsk, 103, 4),  // insert
	}
	for _, d := range deltas {
		if err := b.Apply(d); err != nil {
			t.Fatalf("Apply(%+v) error = %v, want nil", d, err)
		}
	}

	wantBids := []store.Level{{Price: 100, Size: 20}, {Price: 99, Size: 5}, {Price: 98, Size: 1}}
	wantAsks := []store.Level{{Price: 102, Size: 3}, {Price: 103, Size: 4}}
	if diff := cmp.Diff(wantBids, b.Bids()); diff != "" {
		t.Errorf("Bids() mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantAsks, b.Asks()); diff != "" {
		t.Errorf("Asks() mismatch (-want +got):\n%s", diff)
	}

	bestBid, ok := b.BestBid()
	if !ok || bestBid != (store.Level{Price: 100, Size: 20}) {
		t.Errorf("BestBid() = (%v, %v), want ({100 20}, true)", bestBid, ok)
	}
	bestAsk, ok := b.BestAsk()
	if !ok || bestAsk != (store.Level{Price: 102, Size: 3}) {
		t.Errorf("BestAsk() = (%v, %v), want ({102 3}, true)", bestAsk, ok)
	}
}

func TestBookApplyRejects(t *testing.T) {
	tests := []struct {
		name    string
		rec     store.Record
		wantErr error
	}{
		{
			name:    "a_non_delta_record_is_rejected",
			rec:     store.Record{RecordType: store.RecordTypeTrade, InstrumentID: 1},
			wantErr: ErrWrongRecordType,
		},
		{
			name:    "a_record_for_a_different_instrument_is_rejected",
			rec:     store.Record{RecordType: store.RecordTypeDelta, InstrumentID: 2},
			wantErr: ErrWrongInstrument,
		},
		{
			name:    "removing_a_price_not_in_the_book_is_rejected",
			rec:     deltaAt(1, store.SideBid, 500, 0),
			wantErr: ErrLevelNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBook(1)

			err := b.Apply(tt.rec)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Apply(%+v) error = %v, want %v", tt.rec, err, tt.wantErr)
			}
		})
	}
}

func TestBookBestOnEmptySide(t *testing.T) {
	b := NewBook(1)

	if _, ok := b.BestBid(); ok {
		t.Errorf("BestBid() ok = true on an empty book, want false")
	}
	if _, ok := b.BestAsk(); ok {
		t.Errorf("BestAsk() ok = true on an empty book, want false")
	}
}
