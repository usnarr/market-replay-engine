package book

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"

	"replay/internal/store"
)

const (
	warmupVenueID    uint16 = 7
	warmupPriceScale int64  = 100

	inst1 uint32 = 101
	inst2 uint32 = 202
	inst3 uint32 = 303
)

// warmupFixture builds a small multi-instrument, multi-epoch hot-tier
// file by hand, so every test in this file knows the exact expected book
// state at every point in the stream, rather than only cross-checking two
// reconstruction paths against each other.
//
// Layout (index: exchange_ts, instrument, kind):
//
//	 0: 1000 inst1 snapshot  bids=[99:5,100:10]  asks=[101:8,102:3]
//	 1: 1000 inst2 snapshot  bids=[50:20]        asks=[52:15,53:7]
//	 2: 1100 inst1 delta     bid  100 -> 12
//	 3: 1100 inst2 delta     ask  52  -> remove
//	 4: 1120 inst3 delta     bid  10  -> 100 (insert; inst3 has no snapshot yet)
//	 5: 1150 inst1 delta     ask  103 -> 4 (insert)
//	 6: 1150 inst2 delta     bid  48  -> 9 (insert)
//	 7: 1200 inst1 snapshot  bids=[200:1]        asks=[210:1]
//	 8: 1200 inst2 snapshot  bids=[300:2]        asks=[310:2]
//	 9: 1200 inst3 snapshot  bids=[10:100]       asks=[]
//	10: 1300 inst1 delta     bid  200 -> 5
//	11: 1300 inst2 delta     ask  320 -> insert
//	12: 1400 inst1 delta     ask  210 -> remove
//	13: 1400 inst2 delta     bid  300 -> 9
//
// Epoch 0 is records [0,1] and does not cover inst3: it is added only
// from epoch 1 onward, exercising the epoch-run-has-no-snapshot-for-this-
// instrument fallback.
func warmupFixture(t *testing.T) *store.Reader {
	t.Helper()

	path := filepath.Join(t.TempDir(), "warmup.hot")
	w, err := store.NewWriter(path, warmupVenueID, warmupPriceScale)
	if err != nil {
		t.Fatalf("NewWriter() error = %v, want nil", err)
	}

	seq := uint64(0)
	next := func() uint64 { seq++; return seq }

	mustSnapshot := func(ts int64, instrumentID uint32, bids, asks []store.Level) {
		t.Helper()
		rec := store.Record{ExchangeTs: ts, SequenceNumber: next(), InstrumentID: instrumentID, VenueID: warmupVenueID}
		if err := w.WriteSnapshot(rec, bids, asks); err != nil {
			t.Fatalf("WriteSnapshot(%+v) error = %v, want nil", rec, err)
		}
	}
	mustDelta := func(ts int64, instrumentID uint32, sideFlag uint8, price, size int64) {
		t.Helper()
		rec := store.Record{
			ExchangeTs: ts, SequenceNumber: next(), InstrumentID: instrumentID, VenueID: warmupVenueID,
			RecordType: store.RecordTypeDelta, SideFlags: sideFlag, Price: price, Size: size,
		}
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("WriteRecord(%+v) error = %v, want nil", rec, err)
		}
	}

	mustSnapshot(1000, inst1, []store.Level{{Price: 99, Size: 5}, {Price: 100, Size: 10}}, []store.Level{{Price: 101, Size: 8}, {Price: 102, Size: 3}})
	mustSnapshot(1000, inst2, []store.Level{{Price: 50, Size: 20}}, []store.Level{{Price: 52, Size: 15}, {Price: 53, Size: 7}})
	mustDelta(1100, inst1, store.SideBid, 100, 12)
	mustDelta(1100, inst2, store.SideAsk, 52, 0)
	mustDelta(1120, inst3, store.SideBid, 10, 100)
	mustDelta(1150, inst1, store.SideAsk, 103, 4)
	mustDelta(1150, inst2, store.SideBid, 48, 9)
	mustSnapshot(1200, inst1, []store.Level{{Price: 200, Size: 1}}, []store.Level{{Price: 210, Size: 1}})
	mustSnapshot(1200, inst2, []store.Level{{Price: 300, Size: 2}}, []store.Level{{Price: 310, Size: 2}})
	mustSnapshot(1200, inst3, []store.Level{{Price: 10, Size: 100}}, nil)
	mustDelta(1300, inst1, store.SideBid, 200, 5)
	mustDelta(1300, inst2, store.SideAsk, 320, 1)
	mustDelta(1400, inst1, store.SideAsk, 210, 0)
	mustDelta(1400, inst2, store.SideBid, 300, 9)

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	r, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

type bookState struct {
	Bids []store.Level
	Asks []store.Level
}

func stateOf(b *Book) bookState {
	return bookState{Bids: b.Bids(), Asks: b.Asks()}
}

func TestWarmUp(t *testing.T) {
	tests := []struct {
		name         string
		instrumentID uint32
		targetTs     int64
		wantResume   int
		want         bookState
	}{
		{
			name:         "before_the_first_epoch_reconstructs_an_empty_book",
			instrumentID: inst1,
			targetTs:     0,
			wantResume:   0,
			want:         bookState{Bids: []store.Level{}, Asks: []store.Level{}},
		},
		{
			name:         "strictly_between_two_epochs",
			instrumentID: inst1,
			targetTs:     1125,
			wantResume:   5,
			want: bookState{
				Bids: []store.Level{{Price: 100, Size: 12}, {Price: 99, Size: 5}},
				Asks: []store.Level{{Price: 101, Size: 8}, {Price: 102, Size: 3}},
			},
		},
		{
			name:         "exactly_on_an_epoch_boundary",
			instrumentID: inst1,
			targetTs:     1200,
			wantResume:   7,
			want: bookState{
				Bids: []store.Level{{Price: 100, Size: 12}, {Price: 99, Size: 5}},
				Asks: []store.Level{{Price: 101, Size: 8}, {Price: 102, Size: 3}, {Price: 103, Size: 4}},
			},
		},
		{
			name:         "after_the_last_epoch",
			instrumentID: inst1,
			targetTs:     9999,
			wantResume:   14,
			want: bookState{
				Bids: []store.Level{{Price: 200, Size: 5}},
				Asks: []store.Level{},
			},
		},
		{
			name:         "an_instrument_missing_from_the_epoch_falls_back_to_a_full_scan",
			instrumentID: inst3,
			targetTs:     1125,
			wantResume:   5,
			want: bookState{
				Bids: []store.Level{{Price: 10, Size: 100}},
				Asks: []store.Level{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := warmupFixture(t)

			b, resume, err := WarmUp(r, tt.instrumentID, tt.targetTs)
			if err != nil {
				t.Fatalf("WarmUp() error = %v, want nil", err)
			}

			if resume != tt.wantResume {
				t.Errorf("WarmUp() resume = %d, want %d", resume, tt.wantResume)
			}
			if diff := cmp.Diff(tt.want, stateOf(b)); diff != "" {
				t.Errorf("WarmUp() book mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// fullScanBook reconstructs instrumentID's book by scanning every record
// from index 0 up to (exclusive) upTo, the reference implementation
// WarmUp's bounded epoch rewind is checked against.
func fullScanBook(t *testing.T, r *store.Reader, instrumentID uint32, upTo int) *Book {
	t.Helper()

	b := NewBook(instrumentID)
	for i := 0; i < upTo; i++ {
		rec := r.RecordAt(i)
		switch {
		case rec.RecordType == store.RecordTypeSnapshotPointer && rec.InstrumentID == instrumentID:
			levels, bidCount, err := r.AppendLevels(nil, rec)
			if err != nil {
				t.Fatalf("AppendLevels(%d) error = %v, want nil", i, err)
			}
			if err := b.ApplySnapshot(levels, bidCount); err != nil {
				t.Fatalf("ApplySnapshot(%d) error = %v, want nil", i, err)
			}
		case rec.RecordType == store.RecordTypeDelta && rec.InstrumentID == instrumentID:
			if err := b.Apply(rec); err != nil {
				t.Fatalf("Apply(%d) error = %v, want nil", i, err)
			}
		}
	}
	return b
}

func TestWarmUpMatchesFullScan(t *testing.T) {
	tests := []struct {
		name         string
		instrumentID uint32
		targetTs     int64
	}{
		{"before_the_first_epoch", inst1, 0},
		{"strictly_between_two_epochs_inst1", inst1, 1125},
		{"exactly_on_an_epoch_boundary_inst1", inst1, 1200},
		{"after_the_last_epoch_inst1", inst1, 9999},
		{"strictly_between_two_epochs_inst2", inst2, 1125},
		{"exactly_on_an_epoch_boundary_inst2", inst2, 1200},
		{"sparse_instrument_between_epochs", inst3, 1125},
		{"sparse_instrument_after_its_own_epoch", inst3, 1300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := warmupFixture(t)

			warm, resume, err := WarmUp(r, tt.instrumentID, tt.targetTs)
			if err != nil {
				t.Fatalf("WarmUp() error = %v, want nil", err)
			}
			scanned := fullScanBook(t, r, tt.instrumentID, resume)

			if diff := cmp.Diff(stateOf(scanned), stateOf(warm)); diff != "" {
				t.Errorf("WarmUp() vs full scan mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
