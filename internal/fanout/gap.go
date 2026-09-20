package fanout

import (
	"runtime"

	"replay/internal/store"
)

// Gap is a contiguous run of emit indexes a subscriber missed because the
// writer overwrote them before the subscriber could read them. The unit
// is the global emit index, never a venue sequence number: a gap in a
// merged multi-venue stream spans several venues and has no single
// sequence range that describes it. Count is always LastMissedIndex -
// FirstMissedIndex + 1. See docs/backpressure.md.
type Gap struct {
	FirstMissedIndex uint64
	LastMissedIndex  uint64
	Count            uint64
}

// next reads the record at emit index cursor, catching up over any gap
// the writer has already forced. It reports the emit index to resume at
// next — cursor+1 on a successful read, unchanged when nothing was
// ready — whether a gap preceded the record returned, and whether a
// record was read at all this call.
//
// "Not ready" (ready == false, hasGap == false) covers two cases the
// caller does not need to tell apart: the writer has not reached cursor
// yet, or the writer is mid-write on exactly cursor. Either way the
// cursor does not move, and the caller should back off and call next
// again.
func (r *Ring) next(cursor uint64, rec *store.Record, dstBlob []byte) (blob []byte, gap Gap, hasGap bool, newCursor uint64, ready bool) {
	target := cursor
	for {
		b, heldIdx, complete := r.read(target, rec, dstBlob)
		if complete {
			if target > cursor {
				return b, Gap{FirstMissedIndex: cursor, LastMissedIndex: target - 1, Count: target - cursor}, true, target + 1, true
			}
			return b, Gap{}, false, target + 1, true
		}
		if heldIdx <= target {
			return nil, Gap{}, false, cursor, false
		}
		// heldIdx > target: the slot has already moved past what this
		// call wanted. Catch up and retry; a fast writer may have moved
		// on again by the time the retry runs, which is fine — this
		// converges, because each retry costs the reader a handful of
		// atomic loads against the writer's own much larger per-record
		// cost. Gosched keeps a tight catch-up loop from monopolising a
		// core while it converges.
		runtime.Gosched()
		target = heldIdx
	}
}
