// Package fanout serves the canonical replay stream to any number of
// subscribers through a single shared ring buffer, replacing a channel
// and a goroutine per subscriber. One emit goroutine writes; any number
// of subscriber goroutines read through their own atomic cursor. Block
// and Drop backpressure, gap reporting, and pacing all live here. See
// docs/backpressure.md.
package fanout

import (
	"encoding/binary"
	"sync/atomic"

	"replay/internal/store"
)

// slotWords is how many 8-byte words one slot's record occupies.
const slotWords = store.RecordSize / 8

// slot holds one record of the ring in a form both the writer and every
// reader touch only through sync/atomic. A Drop subscriber may copy a
// slot the writer is concurrently overwriting, so every byte shared
// between the two sides must be an atomic word — a plain struct field
// here would be a genuine data race under the Go memory model, not a
// false positive, and go test -race would be right to report it. Do not
// change this to a plain struct. See docs/backpressure.md.
//
// seq encodes both which emit index a slot currently holds and whether
// that index's record is fully published: seq == 2*n while emit index n
// is being written, and seq == 2*n+1 once every word of it has been
// written. A reader wanting index n that observes anything else knows
// immediately which case it is in: the writer is still publishing n (an
// even value with n's own index), or the writer has already moved past n
// (any other index) and n's data is gone.
type slot struct {
	seq   atomic.Uint64
	words [slotWords]atomic.Uint64
}

// Config configures a new Ring. Every field must be set explicitly; the
// zero value of Capacity is rejected rather than defaulting to something
// that happens to work.
type Config struct {
	// Capacity is the number of slots. It must be a power of two of at
	// least two, so a slot index is a mask, never a modulo.
	Capacity int

	// MaxBlobBytes is the longest snapshot blob payload (the bytes
	// store.Reader.Blob returns, without its own 4-byte checksum
	// prefix) the ring will ever carry. A blob larger than this is a
	// construction-time-sized error, not a runtime one: Write returns
	// ErrBlobTooLarge rather than truncating or reallocating the arena.
	MaxBlobBytes int
}

// Ring is the fan-out core: one shared, fixed-capacity buffer written by
// a single goroutine and read by any number of subscriber goroutines
// through independent atomic cursors. See docs/backpressure.md for why
// this replaces a channel and a goroutine per subscriber, and for the
// lapping protocol that makes an undetectable dropped event impossible
// by construction.
type Ring struct {
	mask      uint64
	blobWords int
	maxBlob   int
	slots     []slot
	blobs     []atomic.Uint64

	writeSeq atomic.Uint64
}

// NewRing returns a Ring with the given configuration. The arena is
// sized once, at construction, for cfg.MaxBlobBytes: a snapshot larger
// than that is rejected by Write, never silently truncated.
func NewRing(cfg Config) (*Ring, error) {
	if cfg.Capacity < 2 || cfg.Capacity&(cfg.Capacity-1) != 0 {
		return nil, ErrCapacity
	}
	if cfg.MaxBlobBytes < 0 {
		return nil, ErrMaxBlobBytes
	}

	blobWords := (cfg.MaxBlobBytes + 7) / 8
	return &Ring{
		mask:      uint64(cfg.Capacity - 1),
		blobWords: blobWords,
		maxBlob:   cfg.MaxBlobBytes,
		slots:     make([]slot, cfg.Capacity),
		blobs:     make([]atomic.Uint64, blobWords*cfg.Capacity),
	}, nil
}

// Capacity returns the ring's fixed slot count.
func (r *Ring) Capacity() int { return int(r.mask) + 1 }

// WriteIndex returns the next emit index Write will assign.
func (r *Ring) WriteIndex() uint64 { return r.writeSeq.Load() }

// Write publishes rec, and blob when rec is a snapshot pointer, at the
// next emit index, and returns that index. Write is not safe for
// concurrent use: this project's single emit goroutine is a Ring's only
// writer, by the same rule that makes merge.Merger single-goroutine (see
// internal/merge/merge.go).
func (r *Ring) Write(rec store.Record, blob []byte) (uint64, error) {
	if len(blob) > r.maxBlob {
		return 0, ErrBlobTooLarge
	}

	n := r.writeSeq.Load()
	i := n & r.mask
	sl := &r.slots[i]

	// Invalidate before touching any word. If the words were written
	// first, a reader could load this slot's still-published seq from
	// the record it previously held, copy this write's half-finished
	// words, reload that same now-stale seq, and accept torn data as a
	// complete read. Invalidating first closes that window: no reader
	// can ever observe an even seq and mistake it for a complete
	// publication.
	sl.seq.Store(2 * n)

	var buf [store.RecordSize]byte
	store.EncodeRecord(buf[:], rec)
	for w := 0; w < slotWords; w++ {
		sl.words[w].Store(binary.LittleEndian.Uint64(buf[w*8:]))
	}

	if len(blob) > 0 {
		r.writeBlob(i, blob)
	}

	sl.seq.Store(2*n + 1)
	r.writeSeq.Store(n + 1)
	return n, nil
}

// writeBlob copies blob into slotIndex's region of the blob arena, one
// atomic word at a time. blob's length was already checked against
// r.maxBlob by the caller.
func (r *Ring) writeBlob(slotIndex uint64, blob []byte) {
	base := int(slotIndex) * r.blobWords
	var word [8]byte
	nw := (len(blob) + 7) / 8
	for w := 0; w < nw; w++ {
		off := w * 8
		end := off + 8
		if end > len(blob) {
			clear(word[:])
			copy(word[:], blob[off:])
			r.blobs[base+w].Store(binary.LittleEndian.Uint64(word[:]))
		} else {
			r.blobs[base+w].Store(binary.LittleEndian.Uint64(blob[off:end]))
		}
	}
}

// read attempts to read emit index n into rec, using dstBlob as scratch
// space for a snapshot's blob payload (grown and returned when too
// small; reused otherwise, so a caller that keeps reusing its own
// buffer allocates nothing in steady state). It always reports the emit
// index the slot actually held when the read finished, in heldIdx.
//
// heldIdx may not equal n: a slot the writer has not reached yet still
// holds an earlier index — not a lap, the caller should wait — and a
// slot the writer has already overwritten holds a later one — a lap, the
// caller should catch up to heldIdx. complete is true only when heldIdx
// == n and every word of the read was untorn.
func (r *Ring) read(n uint64, rec *store.Record, dstBlob []byte) (blob []byte, heldIdx uint64, complete bool) {
	i := n & r.mask
	sl := &r.slots[i]

	before := sl.seq.Load()
	if before != 2*n+1 {
		return nil, before >> 1, false
	}

	var buf [store.RecordSize]byte
	for w := 0; w < slotWords; w++ {
		binary.LittleEndian.PutUint64(buf[w*8:], sl.words[w].Load())
	}
	*rec = store.DecodeRecordFields(buf[:])

	var blobOut []byte
	if rec.RecordType == store.RecordTypeSnapshotPointer {
		blobLen := int(rec.BlobLen) - 4
		if blobLen < 0 || blobLen > r.maxBlob {
			// RecordType and BlobLen were decoded from words that may
			// themselves be torn. The reload below would reject this
			// read regardless, but a garbage length must be bounded
			// before it can size or index the blob copy, or a torn read
			// panics instead of being discarded.
			after := sl.seq.Load()
			return nil, after >> 1, false
		}
		blobOut = r.readBlob(i, blobLen, dstBlob)
	}

	after := sl.seq.Load()
	if after != before {
		return nil, after >> 1, false
	}
	return blobOut, n, true
}

// readBlob copies n bytes from slotIndex's region of the blob arena into
// dst, growing dst only if its capacity is too small.
func (r *Ring) readBlob(slotIndex uint64, n int, dst []byte) []byte {
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]

	base := int(slotIndex) * r.blobWords
	nw := (n + 7) / 8
	var word [8]byte
	for w := 0; w < nw; w++ {
		binary.LittleEndian.PutUint64(word[:], r.blobs[base+w].Load())
		off := w * 8
		end := off + 8
		if end > n {
			copy(dst[off:], word[:n-off])
		} else {
			copy(dst[off:end], word[:])
		}
	}
	return dst
}
