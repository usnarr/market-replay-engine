package merge

import "slices"

// Merger merges one cursor per venue into the canonical replay stream.
// It holds one event per venue ahead of what it emits — the loser tree
// needs every cursor's next key to pick a winner — so Blob on an
// unemitted event already aliases its file image.
//
// This is the synchronous merge: one goroutine, no decode concurrency.
type Merger struct {
	cursors []*Cursor
	tree    *LoserTree
	pending []Event
}

// NewMerger returns a merger over one cursor per venue. It reads one
// event from each cursor to seed the tree, so it fails here rather than
// on the first Next if a partition is unreadable.
//
// Two cursors may not hold the same venue. The claim that two live
// cursors can never tie on the ordering key rests on their venue ids
// differing, and so does the argument that a duplicate key can only
// come from inside one partition.
func NewMerger(cursors []*Cursor) (*Merger, error) {
	venues := make([]uint16, len(cursors))
	for i, c := range cursors {
		venues[i] = c.VenueID()
	}
	slices.Sort(venues)
	for i := 1; i < len(venues); i++ {
		if venues[i] == venues[i-1] {
			return nil, ErrDuplicateVenue
		}
	}

	m := &Merger{
		cursors: cursors,
		tree:    NewLoserTree(len(cursors)),
		pending: make([]Event, len(cursors)),
	}
	for i, c := range cursors {
		ev, ok, err := c.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			// The cursor keeps its leaf and its sentinel key.
			continue
		}
		m.pending[i] = ev
		m.tree.SetKey(i, keyOf(ev.Record))
	}
	m.tree.Init()
	return m, nil
}

// Next returns the stream's next event, reporting false at the end.
// The stream ends when every venue is exhausted, never when the first
// one is: a venue that runs out early simply stops winning.
func (m *Merger) Next() (Event, bool, error) {
	i, key := m.tree.Winner()
	if key == SentinelKey {
		return Event{}, false, nil
	}
	ev := m.pending[i]

	next, ok, err := m.cursors[i].Next()
	nextKey := SentinelKey
	if err != nil {
		return Event{}, false, err
	}
	if ok {
		m.pending[i] = next
		nextKey = keyOf(next.Record)
	}
	if err := m.tree.Advance(nextKey); err != nil {
		return Event{}, false, err
	}
	return ev, true, nil
}

// Close releases every cursor's files. It is idempotent.
func (m *Merger) Close() error {
	var err error
	for _, c := range m.cursors {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
