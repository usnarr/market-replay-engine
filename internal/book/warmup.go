package book

import "replay/internal/store"

// WarmUp reconstructs instrumentID's book state at targetTs and returns
// the index canonical replay should continue from — the same index
// r.SeekTime(targetTs) would return. It rewinds to the snapshot epoch at
// or before targetTs and replays that instrument's own deltas forward,
// per docs/book.md's epoch-run contract, instead of rewinding to the
// start of the file on every seek.
//
// When no snapshot epoch precedes targetTs — including a target at or
// before the file's first record, or an instrument missing from every
// epoch that does precede it — WarmUp degrades to a full scan from
// record 0. That is still correct, only unbounded: the common case is
// bounded by one epoch's deltas, and this is the fallback for a seek
// before the first usable epoch.
func WarmUp(r *store.Reader, instrumentID uint32, targetTs int64) (*Book, int, error) {
	idx, err := r.SeekTime(targetTs)
	if err != nil {
		return nil, 0, err
	}

	b := NewBook(instrumentID)
	from := 0

	for before := idx - 1; before >= 0; {
		lastSnap, ok := r.SnapshotBefore(before)
		if !ok {
			break
		}
		epochStart := epochRunStart(r, lastSnap)

		if snapIdx, found := findInstrumentSnapshot(r, epochStart, lastSnap, instrumentID); found {
			levels, bidCount, err := r.AppendLevels(nil, r.RecordAt(snapIdx))
			if err != nil {
				return nil, 0, err
			}
			if err := b.ApplySnapshot(levels, bidCount); err != nil {
				return nil, 0, err
			}
			from = lastSnap + 1
			break
		}

		if epochStart == 0 {
			break
		}
		before = epochStart - 1
	}

	for i := from; i < idx; i++ {
		rec := r.RecordAt(i)
		if rec.RecordType != store.RecordTypeDelta || rec.InstrumentID != instrumentID {
			continue
		}
		if err := b.Apply(rec); err != nil {
			return nil, 0, err
		}
	}

	return b, idx, nil
}

// epochRunStart walks backward from lastIdx — a SnapshotPointer record —
// over the contiguous run of SnapshotPointer records that precede it, and
// returns the run's first index. A run of consecutive SnapshotPointer
// records is one epoch; see docs/book.md.
func epochRunStart(r *store.Reader, lastIdx int) int {
	i := lastIdx
	for i > 0 && r.RecordAt(i-1).RecordType == store.RecordTypeSnapshotPointer {
		i--
	}
	return i
}

// findInstrumentSnapshot scans the epoch run [start, end] for
// instrumentID's own snapshot pointer record. A sparse cadence, where an
// instrument has no snapshot in a given epoch, is possible even though
// the epoch design intends one for every instrument, so callers must
// handle a false return rather than assume every epoch is complete.
func findInstrumentSnapshot(r *store.Reader, start, end int, instrumentID uint32) (int, bool) {
	for i := start; i <= end; i++ {
		rec := r.RecordAt(i)
		if rec.RecordType == store.RecordTypeSnapshotPointer && rec.InstrumentID == instrumentID {
			return i, true
		}
	}
	return 0, false
}
