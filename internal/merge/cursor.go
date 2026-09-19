package merge

import "replay/internal/store"

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

// NewCursor returns a cursor over one venue's files, which must be given
// in ascending time order. It fails if they do not all hold the same
// venue. An empty list is a valid empty partition: it yields nothing and
// still occupies a leaf of the loser tree, which is what keeps the
// tree's shape independent of which venues happen to have data.
func NewCursor(readers []*store.Reader) (*Cursor, error) {
	c := &Cursor{readers: readers, block: -1}
	if len(readers) > 0 {
		c.venueID = readers[0].VenueID()
	}
	for _, r := range readers {
		if r.VenueID() != c.venueID {
			return nil, ErrVenueMismatch
		}
	}
	return c, nil
}

// VenueID returns the venue every record in this partition belongs to.
// An empty partition reports zero.
func (c *Cursor) VenueID() uint16 { return c.venueID }

// Next returns the partition's next record, reporting false once it is
// exhausted. It returns ErrDuplicateKey or ErrOutOfOrder if the key does
// not strictly follow the one before it, and a store error if the block
// the record sits in fails its checksum.
func (c *Cursor) Next() (store.Record, bool, error) {
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
				return store.Record{}, false, err
			}
			c.block = b
		}

		rec := r.RecordAt(c.index)
		c.index++
		if rec.VenueID != c.venueID {
			return store.Record{}, false, ErrVenueMismatch
		}

		key := keyOf(rec)
		if c.hasLast {
			switch cmp := compareKey(c.last, key); {
			case cmp == 0:
				return store.Record{}, false, ErrDuplicateKey
			case cmp > 0:
				return store.Record{}, false, ErrOutOfOrder
			}
		}
		c.last = key
		c.hasLast = true
		return rec, true, nil
	}
	return store.Record{}, false, nil
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
