package merge

import "replay/internal/store"

// Event is one record together with its snapshot payload. Blob is nil
// unless Record is a snapshot pointer, and aliases the file image:
// it stays valid until the cursor that produced it is closed, so a
// consumer that needs it longer copies it. The payload travels with the
// record because only the cursor knows which file holds it, and the
// merge holds one record per venue ahead of what it emits.
type Event struct {
	Record store.Record
	Blob   []byte
}

// Cursor presents one venue's records as a single ordered sequence,
// however many files hold them. It hides file boundaries from the merge
// and carries the last key seen across them: a capture window that
// overlaps its neighbour puts a repeated or backwards key exactly at a
// boundary, which is the one place a per-file check cannot see it.
//
// A Cursor is not safe for concurrent use. One goroutine owns it, which
// is also what makes closing its files safe once it is drained.
type Cursor struct {
	readers []*store.Reader
	venueID uint16

	file  int // index into readers
	index int // next record in readers[file]
	block int // block of readers[file] already verified, -1 when none is

	last    Key
	hasLast bool
}

// NewCursor returns a cursor over venueID's files, which must be given
// in ascending time order. It fails if any of them holds another venue.
// An empty list is a valid empty partition: it yields nothing, still
// knows its venue, and still occupies a leaf of the loser tree, which is
// what keeps the tree's shape independent of which venues have data.
func NewCursor(venueID uint16, readers []*store.Reader) (*Cursor, error) {
	for _, r := range readers {
		if r.VenueID() != venueID {
			return nil, ErrVenueMismatch
		}
	}
	return &Cursor{readers: readers, venueID: venueID, block: -1}, nil
}

// VenueID returns the venue every record in this partition belongs to.
func (c *Cursor) VenueID() uint16 { return c.venueID }

// Next returns the partition's next event, reporting false once it is
// exhausted. It returns ErrDuplicateKey or ErrOutOfOrder if the key does
// not strictly follow the one before it, and a store error if the block
// the record sits in fails its checksum.
func (c *Cursor) Next() (Event, bool, error) {
	for c.file < len(c.readers) {
		r := c.readers[c.file]
		if c.index >= r.Len() {
			c.file++
			c.index = 0
			c.block = -1
			continue
		}

		// A cursor is the only thing that walks a file in record order,
		// so it is where each block is checked, once, on first touch.
		if b := c.index / int(r.BlockSizeRecords()); b != c.block {
			if err := r.VerifyBlock(b); err != nil {
				return Event{}, false, err
			}
			c.block = b
		}

		rec := r.RecordAt(c.index)
		c.index++
		if rec.VenueID != c.venueID {
			return Event{}, false, ErrVenueMismatch
		}

		key := keyOf(rec)
		if c.hasLast {
			switch cmp := compareKey(c.last, key); {
			case cmp == 0:
				return Event{}, false, ErrDuplicateKey
			case cmp > 0:
				return Event{}, false, ErrOutOfOrder
			}
		}
		c.last = key
		c.hasLast = true

		ev := Event{Record: rec}
		if rec.RecordType == store.RecordTypeSnapshotPointer {
			blob, err := r.Blob(rec)
			if err != nil {
				return Event{}, false, err
			}
			ev.Blob = blob
		}
		return ev, true, nil
	}
	return Event{}, false, nil
}

// Close releases every file in the partition. It is idempotent, and the
// merge must call it only once the cursor is drained: a store.Reader
// must not be closed while a goroutine is still reading from it.
func (c *Cursor) Close() error {
	var err error
	for _, r := range c.readers {
		if cerr := r.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
