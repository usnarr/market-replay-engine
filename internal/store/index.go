package store

import (
	"encoding/binary"
	"sort"
)

// The sparse time index holds one int64 per block: the exchange_ts of
// that block's first record. It holds timestamps only, never
// (timestamp, offset) pairs — with a fixed record stride an offset is
// arithmetic once the record index is known, so storing it again would
// be redundant. The snapshot position index holds one uint64 per
// snapshot pointer record: that record's index. See docs/format.md.

// encodeTimeIndex writes ts into the start of dst, and panics if dst is
// shorter than the entries need.
func encodeTimeIndex(dst []byte, ts []int64) {
	dst = dst[:len(ts)*indexEntrySize]
	for i, v := range ts {
		binary.LittleEndian.PutUint64(dst[i*indexEntrySize:], uint64(v))
	}
}

// encodeSnapshotIndex writes idx into the start of dst, and panics if
// dst is shorter than the entries need.
func encodeSnapshotIndex(dst []byte, idx []uint64) {
	dst = dst[:len(idx)*indexEntrySize]
	for i, v := range idx {
		binary.LittleEndian.PutUint64(dst[i*indexEntrySize:], v)
	}
}

// decodeTimeIndex fills dst from the start of src. It rejects an index
// that is not non-decreasing: a binary search over an unsorted index
// returns an arbitrary answer rather than failing, so the ordering is
// checked once here instead of being assumed on every seek. Equal
// adjacent entries are normal — one venue repeats a timestamp, and a
// run of them can be longer than a block.
func decodeTimeIndex(dst []int64, src []byte) error {
	if len(src) < len(dst)*indexEntrySize {
		return ErrShortIndex
	}
	for i := range dst {
		v := int64(binary.LittleEndian.Uint64(src[i*indexEntrySize:]))
		if i > 0 && v < dst[i-1] {
			return ErrIndexOrder
		}
		dst[i] = v
	}
	return nil
}

// decodeSnapshotIndex fills dst from the start of src. Entries must
// strictly increase and must name a record that exists: two index
// entries for one record, or an entry past the end, is a corrupt file.
func decodeSnapshotIndex(dst []uint64, src []byte, recordCount uint64) error {
	if len(src) < len(dst)*indexEntrySize {
		return ErrShortIndex
	}
	for i := range dst {
		v := binary.LittleEndian.Uint64(src[i*indexEntrySize:])
		if i > 0 && v <= dst[i-1] {
			return ErrIndexOrder
		}
		if v >= recordCount {
			return ErrIndexRange
		}
		dst[i] = v
	}
	return nil
}

// searchTimeIndex returns the position of the first entry greater than
// or equal to t, or len(idx) if there is none. Callers rely on it
// returning the *first* such position even when entries repeat: that is
// what makes idx[n-1] < t hold strictly for the returned n, which in
// turn bounds the forward scan a seek has to do. See Reader.SeekTime.
func searchTimeIndex(idx []int64, t int64) int {
	return sort.Search(len(idx), func(j int) bool { return idx[j] >= t })
}

// searchSnapshotIndex returns the largest entry less than or equal to
// recordIndex, and reports whether one exists.
func searchSnapshotIndex(idx []uint64, recordIndex uint64) (uint64, bool) {
	n := sort.Search(len(idx), func(j int) bool { return idx[j] > recordIndex })
	if n == 0 {
		return 0, false
	}
	return idx[n-1], true
}
