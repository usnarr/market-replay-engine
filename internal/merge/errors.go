package merge

import "errors"

// Errors this package returns. They are pre-declared sentinels rather
// than fmt.Errorf calls, because they are returned from per-record
// paths where building an error string would allocate once per record —
// and because fmt.Errorf is banned here for that reason. Compare them
// with errors.Is.
var (
	ErrVenueMismatch  = errors.New("merge: one partition holds more than one venue")
	ErrDuplicateVenue = errors.New("merge: two partitions hold the same venue")
	ErrDuplicateKey   = errors.New("merge: two records share an ordering key")
	ErrOutOfOrder     = errors.New("merge: record key does not increase")
)
