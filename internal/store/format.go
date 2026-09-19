// Package store implements the hot-tier binary format: the fixed-stride
// record array, the page-aligned header, the sparse time index, the
// snapshot position index, the per-block checksums, the sidecar blob
// region, and the canonical hash projection the determinism test reads.
// See docs/format.md for the layout and the reasoning behind it.
//
// Every field is encoded and decoded explicitly, one at a time, with
// encoding/binary's LittleEndian functions. Never unsafe-cast a Go struct
// onto the file bytes, and never use binary.Read or binary.Write: the
// first lets compiler-inserted struct padding into the file, and the
// second uses reflection and allocates. Either one would put bytes into a
// file that no part of this package defines, and the canonical hash over
// that file is the product's core claim.
package store

// RecordSize is the fixed stride of the record array in bytes. Record i
// starts at header_size + RecordSize*i, so a record index converts to a
// file offset by arithmetic alone. Every index in this format depends on
// that, so the stride is a constant and never varies by record type.
const RecordSize = 64

// Byte offsets of each field within a record. The first four fields are
// the total ordering key, in key order, so a key comparison touches only
// the leading 22 bytes.
const (
	offExchangeTs     = 0
	offSequenceNumber = 8
	offInstrumentID   = 16
	offVenueID        = 20
	offRecordType     = 22
	offSideFlags      = 23
	offPrice          = 24
	offSize           = 32
	offBlobOffset     = 40
	offBlobLen        = 48
	offLevelCount     = 52
	offReserved       = 54

	lenReserved = 10
)

// Record types. A flat set of explicit types in one layout, never a union
// with type-dependent field offsets: decode stays a single straight-line
// function that never branches on layout.
const (
	RecordTypeDelta           uint8 = 0
	RecordTypeTrade           uint8 = 1
	RecordTypeSnapshotPointer uint8 = 2

	recordTypeCount = 3
)

// Side values carried in bit 0 of side_flags.
const (
	SideBid uint8 = 0
	SideAsk uint8 = 1

	// sideFlagsMask is the set of defined bits. Bits 1-7 are reserved and
	// a decoder rejects them when set, because they are inside the
	// canonical hash projection: an undefined bit that reached a file
	// would change the hash without changing the record's meaning.
	sideFlagsMask uint8 = 0x01
)

// Record is one decoded event. It is a value type with no pointer fields,
// so it can be returned from the reader's hot path without allocating.
// Blob fields are meaningful only when RecordType is
// RecordTypeSnapshotPointer, and must be zero otherwise.
type Record struct {
	ExchangeTs     int64
	SequenceNumber uint64
	InstrumentID   uint32
	VenueID        uint16
	RecordType     uint8
	SideFlags      uint8
	Price          int64
	Size           int64
	BlobOffset     uint64
	BlobLen        uint32
	LevelCount     uint16
}

// compareKey orders a and b by the total ordering key
// (exchange_ts, venue_id, sequence_number, instrument_id). It returns a
// negative value, zero, or a positive value as a sorts before, equal to,
// or after b. Two records that compare equal have the same key, which is
// a hard error everywhere this package uses it. See docs/format.md.
func compareKey(a, b Record) int {
	if a.ExchangeTs != b.ExchangeTs {
		if a.ExchangeTs < b.ExchangeTs {
			return -1
		}
		return 1
	}
	if a.VenueID != b.VenueID {
		if a.VenueID < b.VenueID {
			return -1
		}
		return 1
	}
	if a.SequenceNumber != b.SequenceNumber {
		if a.SequenceNumber < b.SequenceNumber {
			return -1
		}
		return 1
	}
	if a.InstrumentID != b.InstrumentID {
		if a.InstrumentID < b.InstrumentID {
			return -1
		}
		return 1
	}
	return 0
}
