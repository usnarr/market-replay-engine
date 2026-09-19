package store

import (
	"encoding/binary"
	"hash/crc32"
)

// magic identifies the format. The trailing \r\n\x1a\n run is the PNG
// trick: it turns a line-ending conversion, which git on Windows will
// happily do to a file it thinks is text, into a magic mismatch instead
// of a silently corrupt record array.
var magic = [8]byte{0x89, 'R', 'P', 'L', 0x0D, 0x0A, 0x1A, 0x0A}

// formatVersion is the only version this package reads or writes. A
// reader rejects any other value rather than guessing at compatibility.
const formatVersion uint32 = 1

// Byte offsets of each header field. finalized and header_crc32c sit at
// the end, in that order, so the checksum covers one contiguous range
// [0, hdrOffFinalized) that naturally excludes both of them: finalized is
// written after the checksum, and a checksum cannot cover itself.
const (
	hdrOffMagic               = 0
	hdrOffFormatVersion       = 8
	hdrOffVenueID             = 12
	hdrOffReserved1           = 14
	hdrOffPriceScale          = 16
	hdrOffRecordCount         = 24
	hdrOffMinExchangeTs       = 32
	hdrOffMaxExchangeTs       = 40
	hdrOffHeaderSize          = 48
	hdrOffBlockSizeRecords    = 52
	hdrOffBlobRegionOffset    = 56
	hdrOffBlobRegionLen       = 64
	hdrOffTimeIndexOffset     = 72
	hdrOffTimeIndexCount      = 80
	hdrOffSnapshotIndexOffset = 88
	hdrOffSnapshotIndexCount  = 96
	hdrOffFooterOffset        = 104
	hdrOffReserved2           = 112
	hdrOffFinalized           = 120
	hdrOffReserved3           = 121
	hdrOffCRC                 = 124

	// headerFieldsSize is the size of the defined header fields. The
	// header is then padded with zeroes out to header_size, which is
	// where record 0 starts.
	headerFieldsSize = 128

	// headerCRCLen is the prefix the header checksum covers.
	headerCRCLen = hdrOffFinalized
)

// header is the decoded file header. Every offset it carries is absolute
// within the file, except that a record's blob_offset is relative to
// BlobRegionOffset. See docs/format.md.
type header struct {
	FormatVersion       uint32
	VenueID             uint16
	PriceScale          int64
	RecordCount         uint64
	MinExchangeTs       int64
	MaxExchangeTs       int64
	HeaderSize          uint32
	BlockSizeRecords    uint32
	BlobRegionOffset    uint64
	BlobRegionLen       uint64
	TimeIndexOffset     uint64
	TimeIndexCount      uint64
	SnapshotIndexOffset uint64
	SnapshotIndexCount  uint64
	FooterOffset        uint64
	Finalized           bool
}

// headerSizeFor returns the offset of record 0 for a file written on a
// host with the given page size: the page size rounded up to cover the
// defined header fields. The writer passes os.Getpagesize() and stores
// the result in the header, so a reader on a host with a different page
// size still finds record 0. Never assume 4096 — macOS on Apple silicon
// uses 16384.
func headerSizeFor(pageSize int) (uint32, error) {
	if pageSize <= 0 {
		return 0, ErrPageSize
	}
	pages := (headerFieldsSize + pageSize - 1) / pageSize
	size := pages * pageSize
	if size > maxHeaderSize {
		return 0, ErrPageSize
	}
	return uint32(size), nil
}

// maxHeaderSize bounds header_size so a hostile header cannot claim a
// record array that starts past any plausible file.
const maxHeaderSize = 1 << 20

// Bounds on the header's own count fields. They exist so that every size
// this package derives from a header field stays inside uint64 and
// inside int, before any of it reaches a slice length. A ten-byte
// hostile header must not be able to ask for a hundred-gigabyte
// allocation.
const (
	maxRecordCount      = 1 << 40
	maxBlockSizeRecords = 1 << 24
	maxInt              = int(^uint(0) >> 1)
)

// Entry strides of the trailing regions.
const (
	// indexEntrySize is one int64 timestamp or one uint64 record index.
	indexEntrySize = 8
	// footerEntrySize is one block_start_index plus one crc32c.
	footerEntrySize = 12
)

// blockCountFor returns the number of checksummed blocks a file with
// recordCount records holds. The final block is short, never padded out
// to blockSize.
func blockCountFor(recordCount uint64, blockSize uint32) uint64 {
	if blockSize == 0 {
		return 0
	}
	return (recordCount + uint64(blockSize) - 1) / uint64(blockSize)
}

// setFinalized writes the finalized byte into an encoded header. It is
// the last write of the finalization sequence, which runs in exactly
// this order: the record array, the blob region, both indexes, the
// footer, then the header with the byte clear, fsync, then this byte,
// then fsync again. A reader rejects a file whose byte is clear, so a
// writer that dies at any earlier point leaves a file nothing will read
// rather than one whose header advertises an index that was never
// written. See docs/format.md.
func setFinalized(dst []byte) {
	dst[hdrOffFinalized] = 1
}

// validateHeader reports whether h describes a coherent file of fileLen
// bytes. It runs before any allocation sized from a header field.
func validateHeader(h header, fileLen uint64) error {
	if h.FormatVersion != formatVersion {
		return ErrFormatVersion
	}
	if !h.Finalized {
		return ErrNotFinalized
	}
	if h.HeaderSize < headerFieldsSize || h.HeaderSize > maxHeaderSize {
		return ErrHeaderSize
	}
	if h.BlockSizeRecords == 0 || h.BlockSizeRecords > maxBlockSizeRecords {
		return ErrBlockSize
	}
	if h.PriceScale <= 0 {
		return ErrPriceScale
	}
	if h.RecordCount > maxRecordCount || h.RecordCount > uint64(maxInt) {
		return ErrRecordCount
	}

	// Walk the region chain once, forwards. Each step checks that the
	// region starts at or after the previous one ended, then that its own
	// extent does not overflow. Addition is always written as a
	// subtraction against the remaining headroom, never as a + b.
	end := uint64(h.HeaderSize)
	if h.RecordCount > (^uint64(0)-end)/RecordSize {
		return ErrRecordCount
	}
	end += h.RecordCount * RecordSize

	if h.BlobRegionOffset < end || h.BlobRegionLen > ^uint64(0)-h.BlobRegionOffset {
		return ErrOffsetChain
	}
	end = h.BlobRegionOffset + h.BlobRegionLen

	if h.TimeIndexOffset < end || h.TimeIndexCount > (^uint64(0)-h.TimeIndexOffset)/indexEntrySize {
		return ErrOffsetChain
	}
	end = h.TimeIndexOffset + h.TimeIndexCount*indexEntrySize

	if h.SnapshotIndexOffset < end || h.SnapshotIndexCount > (^uint64(0)-h.SnapshotIndexOffset)/indexEntrySize {
		return ErrOffsetChain
	}
	end = h.SnapshotIndexOffset + h.SnapshotIndexCount*indexEntrySize

	blocks := blockCountFor(h.RecordCount, h.BlockSizeRecords)
	if h.FooterOffset < end || blocks > (^uint64(0)-h.FooterOffset)/footerEntrySize {
		return ErrOffsetChain
	}
	end = h.FooterOffset + blocks*footerEntrySize

	if end > fileLen {
		return ErrOffsetChain
	}
	return nil
}

// encodeHeader writes h into the first headerFieldsSize bytes of dst and
// panics if dst is shorter. It writes the checksum but leaves the
// finalized byte clear: Close sets that byte on its own, after every
// other byte of the file is durable. See docs/format.md.
func encodeHeader(dst []byte, h header) {
	dst = dst[:headerFieldsSize:headerFieldsSize]
	clear(dst)

	copy(dst[hdrOffMagic:], magic[:])
	binary.LittleEndian.PutUint32(dst[hdrOffFormatVersion:], h.FormatVersion)
	binary.LittleEndian.PutUint16(dst[hdrOffVenueID:], h.VenueID)
	binary.LittleEndian.PutUint64(dst[hdrOffPriceScale:], uint64(h.PriceScale))
	binary.LittleEndian.PutUint64(dst[hdrOffRecordCount:], h.RecordCount)
	binary.LittleEndian.PutUint64(dst[hdrOffMinExchangeTs:], uint64(h.MinExchangeTs))
	binary.LittleEndian.PutUint64(dst[hdrOffMaxExchangeTs:], uint64(h.MaxExchangeTs))
	binary.LittleEndian.PutUint32(dst[hdrOffHeaderSize:], h.HeaderSize)
	binary.LittleEndian.PutUint32(dst[hdrOffBlockSizeRecords:], h.BlockSizeRecords)
	binary.LittleEndian.PutUint64(dst[hdrOffBlobRegionOffset:], h.BlobRegionOffset)
	binary.LittleEndian.PutUint64(dst[hdrOffBlobRegionLen:], h.BlobRegionLen)
	binary.LittleEndian.PutUint64(dst[hdrOffTimeIndexOffset:], h.TimeIndexOffset)
	binary.LittleEndian.PutUint64(dst[hdrOffTimeIndexCount:], h.TimeIndexCount)
	binary.LittleEndian.PutUint64(dst[hdrOffSnapshotIndexOffset:], h.SnapshotIndexOffset)
	binary.LittleEndian.PutUint64(dst[hdrOffSnapshotIndexCount:], h.SnapshotIndexCount)
	binary.LittleEndian.PutUint64(dst[hdrOffFooterOffset:], h.FooterOffset)

	binary.LittleEndian.PutUint32(dst[hdrOffCRC:], crc32.Checksum(dst[:headerCRCLen], castagnoli))
}

// decodeHeader reads the header fields from the start of src. It checks
// the magic and the header checksum only; validateHeader checks that the
// values describe a coherent file.
func decodeHeader(src []byte) (header, error) {
	if len(src) < headerFieldsSize {
		return header{}, ErrShortHeader
	}
	src = src[:headerFieldsSize:headerFieldsSize]

	if [8]byte(src[hdrOffMagic:hdrOffMagic+8]) != magic {
		return header{}, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(src[hdrOffCRC:]) != crc32.Checksum(src[:headerCRCLen], castagnoli) {
		return header{}, ErrHeaderCRC
	}

	return header{
		FormatVersion:       binary.LittleEndian.Uint32(src[hdrOffFormatVersion:]),
		VenueID:             binary.LittleEndian.Uint16(src[hdrOffVenueID:]),
		PriceScale:          int64(binary.LittleEndian.Uint64(src[hdrOffPriceScale:])),
		RecordCount:         binary.LittleEndian.Uint64(src[hdrOffRecordCount:]),
		MinExchangeTs:       int64(binary.LittleEndian.Uint64(src[hdrOffMinExchangeTs:])),
		MaxExchangeTs:       int64(binary.LittleEndian.Uint64(src[hdrOffMaxExchangeTs:])),
		HeaderSize:          binary.LittleEndian.Uint32(src[hdrOffHeaderSize:]),
		BlockSizeRecords:    binary.LittleEndian.Uint32(src[hdrOffBlockSizeRecords:]),
		BlobRegionOffset:    binary.LittleEndian.Uint64(src[hdrOffBlobRegionOffset:]),
		BlobRegionLen:       binary.LittleEndian.Uint64(src[hdrOffBlobRegionLen:]),
		TimeIndexOffset:     binary.LittleEndian.Uint64(src[hdrOffTimeIndexOffset:]),
		TimeIndexCount:      binary.LittleEndian.Uint64(src[hdrOffTimeIndexCount:]),
		SnapshotIndexOffset: binary.LittleEndian.Uint64(src[hdrOffSnapshotIndexOffset:]),
		SnapshotIndexCount:  binary.LittleEndian.Uint64(src[hdrOffSnapshotIndexCount:]),
		FooterOffset:        binary.LittleEndian.Uint64(src[hdrOffFooterOffset:]),
		Finalized:           src[hdrOffFinalized] == 1,
	}, nil
}
