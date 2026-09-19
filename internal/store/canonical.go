package store

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
)

// The determinism test hashes an explicit field projection, never raw
// record bytes. Raw bytes would tie every historical baseline to the
// reserved bytes and to fields like blob_offset, which describe where a
// snapshot happens to sit in one file rather than what it contains — two
// byte-different files with identical content would then never match.
// The projection is versioned so a deliberate format change can add
// canonical_v2 without silently changing what an old baseline meant. See
// docs/format.md.

// CanonicalAlgo selects the hash a CanonicalHasher runs.
type CanonicalAlgo uint8

const (
	// CanonicalCRC32C is the default. It is hardware accelerated, so it
	// does not become the throughput bottleneck the way an always-on
	// SHA-256 would.
	CanonicalCRC32C CanonicalAlgo = iota
	// CanonicalSHA256 is for artifact attestation, where a collision has
	// to be infeasible rather than merely unlikely. It runs at roughly
	// 1-2 GB/s per core, so it is never the default.
	CanonicalSHA256
)

// canonicalPrefixMax is the longest fixed part of a projection: the
// common 40 bytes plus a snapshot pointer's level_count.
const canonicalPrefixMax = 42

// canonicalTerminator closes a run's hash input. With the per-record
// length prefix and the record count, it is what stops two different
// record sequences that concatenate to the same bytes from producing the
// same digest.
var canonicalTerminator = [8]byte{'C', 'A', 'N', 'O', 'N', 'v', '1', 0}

// canonicalPrefix writes rec's projected fields into dst and returns how
// many bytes it wrote. blob_offset and blob_len are deliberately absent:
// they say where a snapshot sits in one particular file, not what it
// holds.
func canonicalPrefix(dst []byte, rec Record) int {
	binary.LittleEndian.PutUint64(dst[0:], uint64(rec.ExchangeTs))
	binary.LittleEndian.PutUint64(dst[8:], rec.SequenceNumber)
	binary.LittleEndian.PutUint32(dst[16:], rec.InstrumentID)
	binary.LittleEndian.PutUint16(dst[20:], rec.VenueID)
	dst[22] = rec.RecordType
	dst[23] = rec.SideFlags
	binary.LittleEndian.PutUint64(dst[24:], uint64(rec.Price))
	binary.LittleEndian.PutUint64(dst[32:], uint64(rec.Size))
	if rec.RecordType != RecordTypeSnapshotPointer {
		return 40
	}
	binary.LittleEndian.PutUint16(dst[40:], rec.LevelCount)
	return canonicalPrefixMax
}

// CanonicalHasher accumulates the canonical_v1 digest of a record
// stream. Write allocates nothing: its scratch buffers are fields, and a
// snapshot's blob payload goes to the hash directly from the caller's
// slice rather than being concatenated onto anything.
type CanonicalHasher struct {
	sha     hash.Hash // nil unless the algorithm is CanonicalSHA256
	crc     uint32
	count   uint64
	scratch [canonicalPrefixMax]byte
	lenBuf  [4]byte
	digest  [sha256.Size]byte
	digestN int
	done    bool
}

// NewCanonicalHasher returns a hasher for algo.
func NewCanonicalHasher(algo CanonicalAlgo) *CanonicalHasher {
	h := &CanonicalHasher{}
	if algo == CanonicalSHA256 {
		h.sha = sha256.New()
	}
	return h
}

// Write adds one record to the digest. blobPayload is the snapshot's
// blob without its four-byte checksum prefix, and is ignored for every
// other record type. It panics if called after Sum.
func (h *CanonicalHasher) Write(rec Record, blobPayload []byte) {
	if h.done {
		panic("store: CanonicalHasher.Write after Sum")
	}
	n := canonicalPrefix(h.scratch[:], rec)
	if rec.RecordType != RecordTypeSnapshotPointer {
		blobPayload = nil
	}

	binary.LittleEndian.PutUint32(h.lenBuf[:], uint32(n)+uint32(len(blobPayload)))
	h.update(h.lenBuf[:])
	h.update(h.scratch[:n])
	if len(blobPayload) > 0 {
		h.update(blobPayload)
	}
	h.count++
}

// Sum finishes the run and appends the digest to dst. It writes the
// record count and the terminator into the hash first, so it is
// idempotent and Write after it panics.
func (h *CanonicalHasher) Sum(dst []byte) []byte {
	if !h.done {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], h.count)
		h.update(tail[:])
		h.update(canonicalTerminator[:])

		if h.sha != nil {
			h.digestN = copy(h.digest[:], h.sha.Sum(nil))
		} else {
			binary.LittleEndian.PutUint32(h.digest[:], h.crc)
			h.digestN = 4
		}
		h.done = true
	}
	return append(dst, h.digest[:h.digestN]...)
}

func (h *CanonicalHasher) update(b []byte) {
	if h.sha != nil {
		h.sha.Write(b)
		return
	}
	h.crc = crc32.Update(h.crc, castagnoli, b)
}
