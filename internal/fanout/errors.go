package fanout

import "errors"

// Errors this package returns. They are pre-declared sentinels rather
// than fmt.Errorf calls, because they are returned from per-record paths
// where building an error string would allocate once per record — and
// because fmt.Errorf is banned here for that reason. Compare them with
// errors.Is.
var (
	ErrCapacity     = errors.New("fanout: ring capacity must be a power of two of at least two")
	ErrMaxBlobBytes = errors.New("fanout: max blob bytes must not be negative")
	ErrBlobTooLarge = errors.New("fanout: snapshot blob is larger than the ring's configured max blob bytes")
	ErrModeUnset    = errors.New("fanout: backpressure mode must be Block or Drop")
	ErrStartUnset   = errors.New("fanout: start position must be set")
	ErrStartLapped  = errors.New("fanout: a Block subscriber cannot start before the oldest record still in the ring")
	ErrClosed       = errors.New("fanout: ring is closed")
	ErrEvicted      = errors.New("fanout: subscriber was evicted for making no progress at the Block barrier")
	ErrInvalidSpeed = errors.New("fanout: speed must be a positive rational num/den")
)
