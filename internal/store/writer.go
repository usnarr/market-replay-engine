package store

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// defaultBlockSizeRecords is the number of records one checksummed block
// covers: 1024 records, or 64 KiB.
//
// Verification throughput is flat from 64 records upward, so it does not
// constrain the choice. What does is seek cost, which is linear in the
// block size because SeekTime scans inside one block, against the 20
// bytes of footer and time index each block costs. At 1024 that
// metadata is 0.03% of the file and a seek is under 5 microseconds. See
// BENCHMARKS.md.
const defaultBlockSizeRecords = 1024

// writerBufSize is the record-stream buffer. Records are written 64
// bytes at a time, so an unbuffered file would cost one syscall each.
const writerBufSize = 1 << 16

type writerState uint8

const (
	writerOpen writerState = iota
	writerClosed
	writerAborted
)

// writerOptions carries the two geometry knobs a test needs to set.
// Production code uses NewWriter, which fills them from the host.
type writerOptions struct {
	pageSize         int
	blockSizeRecords uint32
}

func defaultWriterOptions() writerOptions {
	return writerOptions{
		pageSize:         os.Getpagesize(),
		blockSizeRecords: defaultBlockSizeRecords,
	}
}

// Writer produces one hot-tier file for one venue. Records must arrive
// in strictly increasing key order; the writer rejects anything else
// rather than sorting, because a key collision is a data problem to fix
// upstream and is far cheaper to find here than during a six-hour
// replay. See docs/format.md.
type Writer struct {
	opts writerOptions
	path string
	f    *os.File
	bw   *bufio.Writer

	blobPath string
	blobFile *os.File
	blobBW   *bufio.Writer
	blobLen  uint64
	blobBuf  []byte

	hdr       header
	pos       uint64
	recCount  uint64
	lastKey   Record
	hasLast   bool
	timeIndex []int64
	snapIndex []uint64
	footer    []footerEntry
	blockCRC  uint32

	buf   [RecordSize]byte
	state writerState
}

// NewWriter creates path and prepares it for venueID. It fails if path
// already exists: a half-written file from a crashed run is evidence,
// not something to silently overwrite.
func NewWriter(path string, venueID uint16, priceScale int64) (*Writer, error) {
	return newWriter(path, venueID, priceScale, defaultWriterOptions())
}

func newWriter(path string, venueID uint16, priceScale int64, opts writerOptions) (*Writer, error) {
	if priceScale <= 0 {
		return nil, ErrPriceScale
	}
	if opts.blockSizeRecords == 0 || opts.blockSizeRecords > maxBlockSizeRecords {
		return nil, ErrBlockSize
	}
	headerSize, err := headerSizeFor(opts.pageSize)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		opts:     opts,
		path:     path,
		f:        f,
		bw:       bufio.NewWriterSize(f, writerBufSize),
		blobPath: path + ".blob.tmp",
		hdr: header{
			FormatVersion:    formatVersion,
			VenueID:          venueID,
			PriceScale:       priceScale,
			HeaderSize:       headerSize,
			BlockSizeRecords: opts.blockSizeRecords,
		},
	}

	// Reserve the header. Record 0 starts at header_size, so nothing can
	// be written until that space exists.
	if _, err := w.bw.Write(make([]byte, headerSize)); err != nil {
		w.discard()
		return nil, err
	}
	w.pos = uint64(headerSize)
	return w, nil
}

// WriteRecord appends one non-snapshot record. Use WriteSnapshot for a
// snapshot pointer, which needs its blob written too.
func (w *Writer) WriteRecord(rec Record) error {
	if err := w.checkOpen(); err != nil {
		return err
	}
	if rec.RecordType == RecordTypeSnapshotPointer {
		return ErrUseWriteSnapshot
	}
	return w.append(rec)
}

// WriteSnapshot appends a snapshot pointer record and writes its levels
// into the blob region. It fills in record_type and the three blob
// fields, so rec carries the ordering key and nothing else: a caller
// that sets price, size, side_flags or any blob field gets an error
// rather than having its value silently overwritten.
func (w *Writer) WriteSnapshot(rec Record, bids, asks []Level) error {
	if err := w.checkOpen(); err != nil {
		return err
	}
	if rec.Price != 0 || rec.Size != 0 || rec.SideFlags != 0 ||
		rec.BlobOffset != 0 || rec.BlobLen != 0 || rec.LevelCount != 0 {
		return ErrUnusedFieldSet
	}
	levels := len(bids) + len(asks)
	if levels > int(^uint16(0)) {
		return ErrTooManyLevels
	}

	blobLen := blobHeaderSize + levelSize*levels
	if cap(w.blobBuf) < blobLen {
		w.blobBuf = make([]byte, blobLen)
	}
	b := w.blobBuf[:blobLen]
	binary.LittleEndian.PutUint16(b[4:], uint16(len(bids)))
	binary.LittleEndian.PutUint16(b[6:], uint16(len(asks)))
	off := blobHeaderSize
	for _, side := range [2][]Level{bids, asks} {
		for _, l := range side {
			binary.LittleEndian.PutUint64(b[off:], uint64(l.Price))
			binary.LittleEndian.PutUint64(b[off+8:], uint64(l.Size))
			off += levelSize
		}
	}
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))

	rec.RecordType = RecordTypeSnapshotPointer
	rec.BlobOffset = w.blobLen
	rec.BlobLen = uint32(blobLen)
	rec.LevelCount = uint16(levels)

	if err := w.openBlobFile(); err != nil {
		return err
	}
	if err := w.append(rec); err != nil {
		return err
	}
	if _, err := w.blobBW.Write(b); err != nil {
		return err
	}
	w.blobLen += uint64(blobLen)
	return nil
}

// append validates rec against the last key written and adds it to the
// record array, the block checksum, and whichever indexes it belongs in.
func (w *Writer) append(rec Record) error {
	if rec.VenueID != w.hdr.VenueID {
		return ErrVenueMismatch
	}
	if err := validateRecord(rec); err != nil {
		return err
	}
	if w.hasLast {
		switch c := compareKey(w.lastKey, rec); {
		case c == 0:
			return ErrDuplicateKey
		case c > 0:
			return ErrOutOfOrder
		}
	}
	if w.recCount >= maxRecordCount {
		return ErrRecordCount
	}

	blockSize := uint64(w.opts.blockSizeRecords)
	if w.recCount%blockSize == 0 {
		if w.recCount > 0 {
			w.footer = append(w.footer, footerEntry{
				BlockStartIndex: w.recCount - blockSize,
				CRC32C:          w.blockCRC,
			})
		}
		w.blockCRC = 0
		w.timeIndex = append(w.timeIndex, rec.ExchangeTs)
	}

	encodeRecord(w.buf[:], rec)
	if _, err := w.bw.Write(w.buf[:]); err != nil {
		return err
	}
	w.blockCRC = crc32.Update(w.blockCRC, castagnoli, w.buf[:])

	if rec.RecordType == RecordTypeSnapshotPointer {
		w.snapIndex = append(w.snapIndex, w.recCount)
	}
	if w.recCount == 0 {
		w.hdr.MinExchangeTs = rec.ExchangeTs
	}
	w.hdr.MaxExchangeTs = rec.ExchangeTs
	w.pos += RecordSize
	w.recCount++
	w.lastKey = rec
	w.hasLast = true
	return nil
}

// Close finishes the file and makes it readable. It runs the
// finalization sequence from docs/format.md: blob region, both indexes,
// footer, header with the finalized byte clear, fsync, the finalized
// byte, fsync. Close is idempotent.
//
// There is no directory fsync. Windows has no equivalent, so the
// durability this gives is "the file's own bytes reached the disk", not
// "the directory entry did".
func (w *Writer) Close() error {
	switch w.state {
	case writerClosed:
		return nil
	case writerAborted:
		return ErrWriterAborted
	}

	if err := w.finish(); err != nil {
		w.discard()
		w.state = writerAborted
		return err
	}
	w.state = writerClosed
	return nil
}

func (w *Writer) finish() error {
	blockSize := uint64(w.opts.blockSizeRecords)
	if blocks := blockCountFor(w.recCount, w.opts.blockSizeRecords); uint64(len(w.footer)) < blocks {
		w.footer = append(w.footer, footerEntry{
			BlockStartIndex: uint64(len(w.footer)) * blockSize,
			CRC32C:          w.blockCRC,
		})
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}

	w.hdr.BlobRegionOffset = w.pos
	w.hdr.BlobRegionLen = w.blobLen
	if err := w.copyBlobRegion(); err != nil {
		return err
	}

	tail := make([]byte, len(w.timeIndex)*indexEntrySize+len(w.snapIndex)*indexEntrySize+len(w.footer)*footerEntrySize)
	n := len(w.timeIndex) * indexEntrySize
	encodeTimeIndex(tail, w.timeIndex)
	w.hdr.TimeIndexOffset = w.pos
	w.hdr.TimeIndexCount = uint64(len(w.timeIndex))

	encodeSnapshotIndex(tail[n:], w.snapIndex)
	w.hdr.SnapshotIndexOffset = w.pos + uint64(n)
	w.hdr.SnapshotIndexCount = uint64(len(w.snapIndex))
	n += len(w.snapIndex) * indexEntrySize

	encodeFooter(tail[n:], w.footer)
	w.hdr.FooterOffset = w.pos + uint64(n)

	// The block checksums cover the record array and each blob carries
	// its own, but nothing covered the two indexes or the footer itself.
	// One checksum over all three closes that gap: the header checksum
	// protects this value, and this value protects the trailer.
	w.hdr.TrailerCRC = crc32.Checksum(tail, castagnoli)

	if _, err := w.f.Write(tail); err != nil {
		return err
	}
	w.pos += uint64(len(tail))
	w.hdr.RecordCount = w.recCount

	hdrBuf := make([]byte, headerFieldsSize)
	encodeHeader(hdrBuf, w.hdr)
	if _, err := w.f.WriteAt(hdrBuf, 0); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}

	// Everything else is durable. One byte now says so.
	if _, err := w.f.WriteAt([]byte{1}, hdrOffFinalized); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.closeFiles()
}

// copyBlobRegion appends the sidecar temp file to the main file. The
// blob region cannot be written inline, because a record's blob_offset
// is assigned before the region's own position in the file is known.
func (w *Writer) copyBlobRegion() error {
	if w.blobFile == nil {
		return nil
	}
	if err := w.blobBW.Flush(); err != nil {
		return err
	}
	if _, err := w.blobFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(w.f, w.blobFile); err != nil {
		return err
	}
	w.pos += w.blobLen
	return nil
}

// Abort discards a partial file. It closes every handle before removing
// anything: Windows refuses to delete a file that still has one open,
// and getting the order wrong fails only there.
func (w *Writer) Abort() error {
	if w.state == writerClosed {
		return nil
	}
	w.state = writerAborted
	return w.discard()
}

func (w *Writer) discard() error {
	err := w.closeFiles()
	if rmErr := os.Remove(w.path); rmErr != nil && err == nil && !os.IsNotExist(rmErr) {
		err = rmErr
	}
	return err
}

// closeFiles closes both handles and removes the blob temp file. It is
// safe to call twice.
func (w *Writer) closeFiles() error {
	var err error
	if w.f != nil {
		err = w.f.Close()
		w.f = nil
	}
	if w.blobFile != nil {
		if cerr := w.blobFile.Close(); cerr != nil && err == nil {
			err = cerr
		}
		w.blobFile = nil
		if rmErr := os.Remove(w.blobPath); rmErr != nil && err == nil && !os.IsNotExist(rmErr) {
			err = rmErr
		}
	}
	return err
}

// openBlobFile creates the sidecar on the first snapshot, so a file with
// no snapshots never creates a temp file at all.
func (w *Writer) openBlobFile() error {
	if w.blobFile != nil {
		return nil
	}
	f, err := os.OpenFile(w.blobPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.blobFile = f
	w.blobBW = bufio.NewWriterSize(f, writerBufSize)
	return nil
}

func (w *Writer) checkOpen() error {
	switch w.state {
	case writerClosed:
		return ErrWriterClosed
	case writerAborted:
		return ErrWriterAborted
	}
	return nil
}
