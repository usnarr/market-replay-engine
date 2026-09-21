package fanout

import "replay/internal/merge"

// Run drains m into r, one record at a time, calling r.StartEmitting
// itself so every Subscribe or Unsubscribe issued concurrently with Run
// is safely queued from the very first record — see lifecycle.go. It
// applies any pending control request before each record.
//
// Run returns once m reports a clean end of stream, in which case it
// calls r.SetEnd and returns nil, or once m.Next or r.Write returns an
// error, in which case it returns that error without calling SetEnd: an
// aborted run has no valid prefix to hand subscribers, exactly as
// merge.Merger.Next's own sticky-error contract already requires of its
// caller (see internal/merge/merge.go).
//
// Run is not safe for concurrent use, matching Merger.Next's own rule:
// one goroutine drives one Ring from one Merger.
func (r *Ring) Run(m *merge.Merger) error {
	r.StartEmitting()
	for {
		r.applyControl()

		ev, ok, err := m.Next()
		if err != nil {
			return err
		}
		if !ok {
			r.SetEnd(r.writeSeq.Load())
			return nil
		}
		if _, err := r.Write(ev.Record, ev.Blob); err != nil {
			return err
		}
	}
}
