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

// RunPaced is Run with pacing: before writing each record, it calls
// p.Wait(record.ExchangeTs), so records are released no faster than the
// schedule p computes. p.Start is called once, anchored to the first
// record's own exchange timestamp — the replay's own pace starts from
// wherever the data starts, never from an external clock reading.
//
// Wait returning false means p was stopped, not that anything failed:
// RunPaced ends the stream at whatever point it had reached, exactly as
// a clean end of input does, so subscribers waiting on r.End are
// released rather than left hanging.
//
// This never reorders, drops, or duplicates a record: the pacer only
// gates whether the loop proceeds to the write it already has in hand,
// never which record that is — Merger.Next has already chosen it, in
// canonical order, before RunPaced ever sees it. See docs/clock.md.
func (r *Ring) RunPaced(m *merge.Merger, p *Pacer) error {
	r.StartEmitting()
	started := false
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
		if !started {
			p.Start(ev.Record.ExchangeTs)
			started = true
		}
		if !p.Wait(ev.Record.ExchangeTs) {
			r.SetEnd(r.writeSeq.Load())
			return nil
		}
		if _, err := r.Write(ev.Record, ev.Blob); err != nil {
			return err
		}
	}
}
