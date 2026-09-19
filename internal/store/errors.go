package store

import "errors"

// Errors this package returns. They are pre-declared sentinels, not
// fmt.Errorf calls, because several of them are returned from per-record
// paths where building an error string would allocate once per record.
// Compare them with errors.Is.
var (
	ErrShortRecord    = errors.New("store: buffer shorter than one record")
	ErrRecordType     = errors.New("store: unknown record type")
	ErrSideFlags      = errors.New("store: side_flags sets a reserved bit")
	ErrReserved       = errors.New("store: a byte undefined by this format version is set")
	ErrUnusedFieldSet = errors.New("store: a field unused by this record type is not zero")
	ErrBlobLen        = errors.New("store: blob_len and level_count disagree")

	ErrShortHeader = errors.New("store: buffer shorter than the header fields")
	ErrBadMagic    = errors.New("store: not a replay hot-tier file")
	ErrHeaderCRC   = errors.New("store: header checksum mismatch")
	ErrTrailerCRC  = errors.New("store: index and footer checksum mismatch")
	ErrPageSize    = errors.New("store: unusable page size")

	ErrFormatVersion = errors.New("store: unsupported format version")
	ErrNotFinalized  = errors.New("store: file is not finalized")
	ErrHeaderSize    = errors.New("store: header_size out of range")
	ErrBlockSize     = errors.New("store: block_size_records out of range")
	ErrPriceScale    = errors.New("store: price_scale must be positive")
	ErrRecordCount   = errors.New("store: record_count out of range")
	ErrOffsetChain   = errors.New("store: file regions do not form a valid chain")

	ErrShortIndex = errors.New("store: index region shorter than its entry count")
	ErrIndexOrder = errors.New("store: index entries are not in order")
	ErrIndexRange = errors.New("store: index entry names a record that does not exist")
	ErrIndexCount = errors.New("store: index entry count does not match the record array")

	ErrShortFooter    = errors.New("store: footer region shorter than its entry count")
	ErrFooterGeometry = errors.New("store: footer entry does not start where its block does")
	ErrBlockRange     = errors.New("store: block index out of range")
	ErrBlockChecksum  = errors.New("store: block checksum mismatch")

	ErrWriterClosed     = errors.New("store: writer is closed")
	ErrWriterAborted    = errors.New("store: writer was aborted")
	ErrVenueMismatch    = errors.New("store: record venue_id does not match the file")
	ErrOutOfOrder       = errors.New("store: record key does not increase")
	ErrDuplicateKey     = errors.New("store: duplicate record key")
	ErrUseWriteSnapshot = errors.New("store: write a snapshot pointer with WriteSnapshot")
	ErrTooManyLevels    = errors.New("store: snapshot has more levels than level_count can hold")

	ErrHeaderRange    = errors.New("store: header exchange_ts range does not match the records")
	ErrNotSnapshot    = errors.New("store: record is not a snapshot pointer")
	ErrBlobOutOfRange = errors.New("store: blob lies outside the blob region")
	ErrBlobChecksum   = errors.New("store: blob checksum mismatch")
	ErrBlobLevelCount = errors.New("store: blob level counts disagree with level_count")
)
