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

import "encoding/binary"

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

// Blob region geometry. A snapshot's level data lives in the sidecar
// blob region, never inline, because a 50-level book cannot fit a
// 64-byte record. See docs/format.md.
const (
	// blobHeaderSize covers the blob's own crc32c, bid_count and ask_count.
	blobHeaderSize = 8
	// levelSize is the stride of one price level: price then size.
	levelSize = 16
)

// Level is one price level of a snapshot. It carries no side of its own:
// a blob stores its bids and its asks as two counted runs, which is
// denser than a side byte per level and leaves no undefined padding.
// Price and Size are scaled integers, never floats — a float64 can be
// NaN, which is not equal to itself and breaks both the comparator and
// the hash.
type Level struct {
	Price int64
	Size  int64
}

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

// validateRecord reports whether rec's type-dependent fields are
// consistent. Both encode and decode go through it, which is what makes
// decode-then-encode reproduce the original bytes exactly: a field this
// function rejects can never reach a file, so no file can hold a value
// that survives decode but changes on re-encode.
func validateRecord(rec Record) error {
	if rec.RecordType >= recordTypeCount {
		return ErrRecordType
	}
	if rec.SideFlags&^sideFlagsMask != 0 {
		return ErrSideFlags
	}
	if rec.RecordType != RecordTypeSnapshotPointer {
		if rec.BlobOffset != 0 || rec.BlobLen != 0 || rec.LevelCount != 0 {
			return ErrUnusedFieldSet
		}
		return nil
	}

	// A snapshot pointer carries no price, size or side of its own; those
	// live in the blob's levels. They are hashed by canonical_v1 for every
	// record type, so leaving them unconstrained would let two files with
	// the same meaning hash differently.
	if rec.Price != 0 || rec.Size != 0 || rec.SideFlags != 0 {
		return ErrUnusedFieldSet
	}
	if rec.BlobLen < blobHeaderSize || (rec.BlobLen-blobHeaderSize)%levelSize != 0 {
		return ErrBlobLen
	}
	if uint32(rec.LevelCount) != (rec.BlobLen-blobHeaderSize)/levelSize {
		return ErrBlobLen
	}
	return nil
}

// encodeRecord writes rec into the first RecordSize bytes of dst, and
// panics if dst is shorter. It writes every byte, including explicit
// zeroes for the reserved range, so the result never depends on what dst
// held before. Callers validate rec first; this function does not.
func encodeRecord(dst []byte, rec Record) {
	dst = dst[:RecordSize:RecordSize]

	binary.LittleEndian.PutUint64(dst[offExchangeTs:], uint64(rec.ExchangeTs))
	binary.LittleEndian.PutUint64(dst[offSequenceNumber:], rec.SequenceNumber)
	binary.LittleEndian.PutUint32(dst[offInstrumentID:], rec.InstrumentID)
	binary.LittleEndian.PutUint16(dst[offVenueID:], rec.VenueID)
	dst[offRecordType] = rec.RecordType
	dst[offSideFlags] = rec.SideFlags
	binary.LittleEndian.PutUint64(dst[offPrice:], uint64(rec.Price))
	binary.LittleEndian.PutUint64(dst[offSize:], uint64(rec.Size))
	binary.LittleEndian.PutUint64(dst[offBlobOffset:], rec.BlobOffset)
	binary.LittleEndian.PutUint32(dst[offBlobLen:], rec.BlobLen)
	binary.LittleEndian.PutUint16(dst[offLevelCount:], rec.LevelCount)
	clear(dst[offReserved : offReserved+lenReserved])
}

// decodeRecord reads one record from the first RecordSize bytes of src.
// It returns ErrShortRecord if src is too small, and a typed error if any
// byte of the record is undefined by this format version.
func decodeRecord(src []byte) (Record, error) {
	if len(src) < RecordSize {
		return Record{}, ErrShortRecord
	}
	src = src[:RecordSize:RecordSize]

	rec := Record{
		ExchangeTs:     int64(binary.LittleEndian.Uint64(src[offExchangeTs:])),
		SequenceNumber: binary.LittleEndian.Uint64(src[offSequenceNumber:]),
		InstrumentID:   binary.LittleEndian.Uint32(src[offInstrumentID:]),
		VenueID:        binary.LittleEndian.Uint16(src[offVenueID:]),
		RecordType:     src[offRecordType],
		SideFlags:      src[offSideFlags],
		Price:          int64(binary.LittleEndian.Uint64(src[offPrice:])),
		Size:           int64(binary.LittleEndian.Uint64(src[offSize:])),
		BlobOffset:     binary.LittleEndian.Uint64(src[offBlobOffset:]),
		BlobLen:        binary.LittleEndian.Uint32(src[offBlobLen:]),
		LevelCount:     binary.LittleEndian.Uint16(src[offLevelCount:]),
	}

	for _, b := range src[offReserved : offReserved+lenReserved] {
		if b != 0 {
			return Record{}, ErrReserved
		}
	}
	if err := validateRecord(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
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
