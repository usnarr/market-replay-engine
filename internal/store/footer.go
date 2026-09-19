package store

import (
	"encoding/binary"
	"hash/crc32"
)

// The footer holds one checksum per fixed-size block of records, not one
// checksum for the whole file. A reader verifies a block on first touch,
// so reading record 0 never has to page in the last byte of a
// multi-gigabyte file. The checksums cover the record array only: a
// blob carries its own checksum, because a seek reads a blob without
// touching the block its pointer record lives in. See docs/format.md.

// footerEntry is one block's checksum. BlockStartIndex is redundant with
// the entry's position, and is stored anyway so the geometry can be
// checked rather than assumed.
type footerEntry struct {
	BlockStartIndex uint64
	CRC32C          uint32
}

// encodeFooter writes entries into the start of dst, and panics if dst
// is shorter than the entries need.
func encodeFooter(dst []byte, entries []footerEntry) {
	dst = dst[:len(entries)*footerEntrySize]
	for i, e := range entries {
		b := dst[i*footerEntrySize:]
		binary.LittleEndian.PutUint64(b, e.BlockStartIndex)
		binary.LittleEndian.PutUint32(b[8:], e.CRC32C)
	}
}

// decodeFooter fills dst from the start of src. It rejects a footer
// whose entry positions disagree with the block size, so a hostile file
// cannot describe overlapping or out-of-order blocks that the verify
// path would then have to handle.
func decodeFooter(dst []footerEntry, src []byte, blockSize uint32) error {
	if len(src) < len(dst)*footerEntrySize {
		return ErrShortFooter
	}
	for i := range dst {
		b := src[i*footerEntrySize:]
		start := binary.LittleEndian.Uint64(b)
		if start != uint64(i)*uint64(blockSize) {
			return ErrFooterGeometry
		}
		dst[i] = footerEntry{BlockStartIndex: start, CRC32C: binary.LittleEndian.Uint32(b[8:])}
	}
	return nil
}

// blockRecordBytes returns the record-array bytes that block blockIndex
// covers, and reports whether the block exists. The last block is short
// whenever record_count is not a multiple of blockSize; it is never
// padded out to a full block, so its checksum covers exactly the records
// it holds.
func blockRecordBytes(records []byte, blockIndex int, blockSize uint32, recordCount uint64) ([]byte, bool) {
	if blockIndex < 0 || uint64(blockIndex) >= blockCountFor(recordCount, blockSize) {
		return nil, false
	}
	start := uint64(blockIndex) * uint64(blockSize)
	end := start + uint64(blockSize)
	if end > recordCount {
		end = recordCount
	}
	if end*RecordSize > uint64(len(records)) {
		return nil, false
	}
	return records[start*RecordSize : end*RecordSize], true
}

// blockChecksum returns the checksum of one block's record bytes.
func blockChecksum(blockBytes []byte) uint32 {
	return crc32.Checksum(blockBytes, castagnoli)
}

// verifyBlock checks one block's records against its footer entry.
func verifyBlock(records []byte, entries []footerEntry, blockIndex int, blockSize uint32, recordCount uint64) error {
	if blockIndex < 0 || blockIndex >= len(entries) {
		return ErrBlockRange
	}
	b, ok := blockRecordBytes(records, blockIndex, blockSize, recordCount)
	if !ok {
		return ErrBlockRange
	}
	if blockChecksum(b) != entries[blockIndex].CRC32C {
		return ErrBlockChecksum
	}
	return nil
}
