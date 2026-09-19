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
	ErrReserved       = errors.New("store: reserved bytes are not zero")
	ErrUnusedFieldSet = errors.New("store: a field unused by this record type is not zero")
	ErrBlobLen        = errors.New("store: blob_len and level_count disagree")
)
