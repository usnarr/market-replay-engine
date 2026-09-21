package store

import (
	"encoding/binary"
	"hash/crc32"
)

// Reader serves one finalized hot-tier file. It is a concrete type, not
// an interface: the platform difference between mmap backends is
// resolved once, at Open, so that reading a record never pays for a
// method dispatch.
//
// A Reader is immutable once Open returns, so any number of goroutines
// may read from it at once. Close is the exception, and is the caller's
// responsibility to order: it must not run while another goroutine is
// still reading. Synchronising that here would cost the hot path
// something the merge stage's own lifecycle already prevents, since it
// closes a cursor's reader only after draining it.
type Reader struct {
	data      []byte
	records   []byte
	blobs     []byte
	timeIndex []int64
	snapIndex []uint64
	footer    []footerEntry
	hdr       header
	closeFn   func() error
	closed    bool
}

// Open reads and validates the file at path. It rejects a file that is
// not finalized, and does not verify the record blocks: those are
// checked per block, on first touch, by VerifyBlock.
func Open(path string) (*Reader, error) {
	data, closeFn, err := mapFile(path)
	if err != nil {
		return nil, err
	}
	r, err := openBytes(data)
	if err != nil {
		if closeFn != nil {
			_ = closeFn()
		}
		return nil, err
	}
	r.closeFn = closeFn
	return r, nil
}

// openBytes validates a whole file image and indexes it. Every size it
// derives from a header field is bounds-checked by validateHeader first,
// so no allocation below is attacker-controlled.
func openBytes(data []byte) (*Reader, error) {
	h, err := decodeHeader(data)
	if err != nil {
		return nil, err
	}
	if err := validateHeader(h, uint64(len(data))); err != nil {
		return nil, err
	}

	// The trailer is small and read in full below, so checking it here
	// costs nothing extra and makes the integrity chain complete: the
	// header checksum covers this value, this value covers both indexes
	// and the footer, the footer covers the record array a block at a
	// time, and each blob covers itself.
	if crc32.Checksum(data[h.TimeIndexOffset:], castagnoli) != h.TrailerCRC {
		return nil, ErrTrailerCRC
	}

	recStart := uint64(h.HeaderSize)
	r := &Reader{
		data:    data,
		hdr:     h,
		records: data[recStart : recStart+h.RecordCount*RecordSize],
		blobs:   data[h.BlobRegionOffset : h.BlobRegionOffset+h.BlobRegionLen],
	}

	r.timeIndex = make([]int64, h.TimeIndexCount)
	if err := decodeTimeIndex(r.timeIndex, data[h.TimeIndexOffset:]); err != nil {
		return nil, err
	}
	r.snapIndex = make([]uint64, h.SnapshotIndexCount)
	if err := decodeSnapshotIndex(r.snapIndex, data[h.SnapshotIndexOffset:], h.RecordCount); err != nil {
		return nil, err
	}
	r.footer = make([]footerEntry, blockCountFor(h.RecordCount, h.BlockSizeRecords))
	if err := decodeFooter(r.footer, data[h.FooterOffset:], h.BlockSizeRecords); err != nil {
		return nil, err
	}

	if err := r.checkTimestampRange(); err != nil {
		return nil, err
	}
	return r, nil
}

// checkTimestampRange confirms the header's advertised range against the
// records it claims to describe. Two record decodes catch a whole class
// of malformed file for O(1) work.
func (r *Reader) checkTimestampRange() error {
	if r.hdr.RecordCount == 0 {
		if r.hdr.MinExchangeTs != 0 || r.hdr.MaxExchangeTs != 0 {
			return ErrHeaderRange
		}
		return nil
	}
	first, err := decodeRecord(r.records)
	if err != nil {
		return err
	}
	last, err := decodeRecord(r.records[(r.hdr.RecordCount-1)*RecordSize:])
	if err != nil {
		return err
	}
	if first.ExchangeTs != r.hdr.MinExchangeTs || last.ExchangeTs != r.hdr.MaxExchangeTs {
		return ErrHeaderRange
	}
	return nil
}

// Len returns the number of records in the file.
func (r *Reader) Len() int { return int(r.hdr.RecordCount) }

// VenueID returns the venue every record in this file belongs to.
func (r *Reader) VenueID() uint16 { return r.hdr.VenueID }

// PriceScale returns the power-of-ten divisor for every Price and Size
// in this file.
func (r *Reader) PriceScale() int64 { return r.hdr.PriceScale }

// BlockSizeRecords returns how many records one checksummed block
// covers.
func (r *Reader) BlockSizeRecords() uint32 { return r.hdr.BlockSizeRecords }

// BlockCount returns the number of checksummed blocks.
func (r *Reader) BlockCount() int { return len(r.footer) }

// RecordAt decodes record i. It panics if i is out of range, the way a
// slice index does, and it does not verify the record's block: call
// VerifyBlock once per block instead of paying for a branch per record.
func (r *Reader) RecordAt(i int) Record {
	if i < 0 || i >= r.Len() {
		panic("store: RecordAt index out of range")
	}
	return DecodeRecordFields(r.records[i*RecordSize:])
}

// Blob returns a snapshot's blob payload: the bid and ask counts
// followed by the levels, without the four-byte checksum prefix. This is
// exactly the byte range canonical_v1 hashes.
//
// The result aliases the file image. It stays valid until Close, and
// touching it afterwards faults rather than panics once the file is
// memory-mapped, so a caller that needs it longer must copy it.
func (r *Reader) Blob(rec Record) ([]byte, error) {
	if err := validateRecord(rec); err != nil {
		return nil, err
	}
	if rec.RecordType != RecordTypeSnapshotPointer {
		return nil, ErrNotSnapshot
	}

	regionLen := uint64(len(r.blobs))
	if rec.BlobOffset > regionLen || uint64(rec.BlobLen) > regionLen-rec.BlobOffset {
		return nil, ErrBlobOutOfRange
	}
	blob := r.blobs[rec.BlobOffset:][:rec.BlobLen]

	payload := blob[4:]
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(blob) {
		return nil, ErrBlobChecksum
	}
	bids := binary.LittleEndian.Uint16(payload[0:])
	asks := binary.LittleEndian.Uint16(payload[2:])
	if uint32(bids)+uint32(asks) != uint32(rec.LevelCount) {
		return nil, ErrBlobLevelCount
	}
	return payload, nil
}

// AppendLevels appends a snapshot's levels to dst and returns the
// extended slice together with how many of the appended levels are bids.
// Bids come first, then asks. It appends rather than allocating so a
// caller reading many snapshots can reuse one buffer.
func (r *Reader) AppendLevels(dst []Level, rec Record) ([]Level, int, error) {
	payload, err := r.Blob(rec)
	if err != nil {
		return dst, 0, err
	}
	return AppendLevelsFromPayload(dst, payload)
}

// AppendLevelsFromPayload is AppendLevels for a caller holding a copy of
// the payload with no Reader to read it from: internal/fanout's ring
// copies one into its own arena, so a subscriber has the bytes and not
// the file (see docs/backpressure.md). It returns ErrBlobLen if payload
// is not exactly a header plus a whole number of levels.
func AppendLevelsFromPayload(dst []Level, payload []byte) ([]Level, int, error) {
	const payloadHeaderSize = blobHeaderSize - 4 // the crc prefix is not part of the payload
	if len(payload) < payloadHeaderSize {
		return dst, 0, ErrBlobLen
	}
	bids := int(binary.LittleEndian.Uint16(payload[0:]))
	asks := int(binary.LittleEndian.Uint16(payload[2:]))
	if len(payload) != payloadHeaderSize+(bids+asks)*levelSize {
		return dst, 0, ErrBlobLen
	}

	for i := 0; i < bids+asks; i++ {
		off := payloadHeaderSize + i*levelSize
		dst = append(dst, Level{
			Price: int64(binary.LittleEndian.Uint64(payload[off:])),
			Size:  int64(binary.LittleEndian.Uint64(payload[off+8:])),
		})
	}
	return dst, bids, nil
}

// SeekTime returns the index of the first record whose exchange_ts is
// at or after t. It returns Len() when every record is earlier, which is
// a valid end cursor rather than an error, because the common caller is
// "replay from t to the end". An error means the file is structurally
// wrong, never that t simply matched nothing.
//
// The sparse index is non-decreasing, not strictly increasing: one venue
// repeats a timestamp, and a run of equal timestamps can straddle a
// block boundary or be longer than a whole block. So searching for the
// block whose first timestamp is at or before t and scanning inside it
// is wrong — the first matching record can sit in the block before. The
// search below finds the first index entry at or after t instead, which
// makes the previous entry strictly earlier than t and bounds the scan
// to that block plus one record.
//
// The answer means something only for a file whose blocks verify. The
// index is trusted, and a record array that contradicts it is
// corruption, which is what VerifyBlock is for.
func (r *Reader) SeekTime(t int64) (int, error) {
	n := r.Len()
	if n == 0 {
		return 0, nil
	}

	b := searchTimeIndex(r.timeIndex, t)
	if b == 0 {
		return 0, nil
	}

	blockSize := int(r.hdr.BlockSizeRecords)
	lo := (b - 1) * blockSize
	hi := lo + blockSize
	if b == len(r.timeIndex) || hi > n-1 {
		hi = n - 1
	}
	for i := lo; i <= hi; i++ {
		if r.RecordAt(i).ExchangeTs >= t {
			return i, nil
		}
	}
	return n, nil
}

// SnapshotBefore returns the index of the last snapshot pointer record
// at or before recordIndex, and reports whether one exists. A seek uses
// it to rewind to an epoch before replaying deltas forward, so that the
// book it reconstructs is valid.
func (r *Reader) SnapshotBefore(recordIndex int) (int, bool) {
	if recordIndex < 0 {
		return 0, false
	}
	i, ok := searchSnapshotIndex(r.snapIndex, uint64(recordIndex))
	if !ok {
		return 0, false
	}
	return int(i), true
}

// VerifyBlock checks one block's records against its stored checksum.
// Callers verify a block once, when they first reach it, so reading
// record 0 never costs a pass over the whole file.
func (r *Reader) VerifyBlock(blockIndex int) error {
	return verifyBlock(r.records, r.footer, blockIndex, r.hdr.BlockSizeRecords, r.hdr.RecordCount)
}

// VerifyAll checks every block and every snapshot blob. It is for tests
// and for offline artifact checking, never for the replay path.
func (r *Reader) VerifyAll() error {
	for i := range r.footer {
		if err := r.VerifyBlock(i); err != nil {
			return err
		}
	}
	for _, i := range r.snapIndex {
		if _, err := r.Blob(r.RecordAt(int(i))); err != nil {
			return err
		}
	}
	return nil
}

// Close releases the file image. It is idempotent. It must not be called
// while another goroutine is still reading from this Reader.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.data = nil
	r.records = nil
	r.blobs = nil
	if r.closeFn != nil {
		return r.closeFn()
	}
	return nil
}
