package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"

	"replay/internal/store"
)

// readBatch is how many rows one Read call decodes. Batching keeps the
// row loop out of parquet-go's per-call overhead without holding a whole
// row group in memory.
const readBatch = 256

// Row-level rejection classes. A source file that trips any of them is
// rejected whole: the converter never repairs a row, because a silent
// repair hides a data problem that has to be fixed upstream. See
// docs/convert.md.
var (
	ErrVenueIDRange     = errors.New("venue_id does not fit uint16")
	ErrRecordTypeRange  = errors.New("record_type is not a defined record type")
	ErrSideFlagsRange   = errors.New("side_flags sets a bit this format reserves")
	ErrLevelsOnNonSnap  = errors.New("a row that is not a snapshot carries levels")
	ErrSnapshotFieldSet = errors.New("a snapshot row sets price, size or side_flags")
	ErrLevelSide        = errors.New("a level's side is neither bid nor ask")
)

// RowError identifies the one source row a conversion rejected. It
// carries the row's ordinal in the source file and the row's own
// ordering key, so the fault can be found in the input without replaying
// the conversion to work out which record the converter was looking at.
type RowError struct {
	// Index is the row's ordinal in the source file, counting from zero.
	Index int64

	// Row is the offending row.
	Row SourceRow

	// Err is the rejection class this error wraps.
	Err error
}

func (e *RowError) Error() string {
	return fmt.Sprintf("convert: source row %d (exchange_ts=%d, venue_id=%d, sequence_number=%d, instrument_id=%d): %s",
		e.Index, e.Row.ExchangeTs, e.Row.VenueID, e.Row.SequenceNumber, e.Row.InstrumentID, e.Err)
}

func (e *RowError) Unwrap() error { return e.Err }

// Convert reads src and writes one hot-tier file per venue and UTC day
// into outDir, returning the paths it wrote ordered by venue then day.
// It validates the whole source schema before it creates any file, and
// removes every file it has started if a later row is rejected: a
// half-converted directory is worse than none, because nothing
// downstream can tell one from a complete conversion.
func Convert(src, outDir string, priceScale int64) ([]string, error) {
	if _, err := CheckSourceFile(src); err != nil {
		return nil, err
	}
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	c := &converter{outDir: outDir, priceScale: priceScale}
	r := parquet.NewGenericReader[SourceRow](f)
	defer r.Close()

	rows := make([]SourceRow, readBatch)
	index := int64(0)
	for {
		n, readErr := r.Read(rows)
		for i := 0; i < n; i++ {
			if err := c.writeRow(index, rows[i]); err != nil {
				c.abort()
				return nil, err
			}
			index++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			c.abort()
			return nil, readErr
		}
	}
	return c.close()
}

// partition is one open output file.
type partition struct {
	key  PartitionKey
	path string
	w    *store.Writer
}

// converter holds the output files a conversion has opened. They live in
// a slice sorted by partition key, searched by binary search, never in a
// map: cmd/convert is held to the same ordered-path rule as the replay
// path, and this order is the order files are closed and reported in.
// See the root CLAUDE.md.
type converter struct {
	outDir     string
	priceScale int64
	parts      []*partition
}

// writeRow converts one source row and appends it to its partition.
func (c *converter) writeRow(index int64, row SourceRow) error {
	rec, bids, asks, err := recordOf(row)
	if err != nil {
		return &RowError{Index: index, Row: row, Err: err}
	}
	p, err := c.partition(PartitionOf(rec.VenueID, rec.ExchangeTs))
	if err != nil {
		return err
	}
	if rec.RecordType == store.RecordTypeSnapshotPointer {
		return p.w.WriteSnapshot(rec, bids, asks)
	}
	return p.w.WriteRecord(rec)
}

// partition returns the open file for key, creating it on first use.
func (c *converter) partition(key PartitionKey) (*partition, error) {
	i, found := slices.BinarySearchFunc(c.parts, key, func(p *partition, k PartitionKey) int {
		return p.key.Compare(k)
	})
	if found {
		return c.parts[i], nil
	}

	path := filepath.Join(c.outDir, key.FileName())
	w, err := store.NewWriter(path, key.VenueID, c.priceScale)
	if err != nil {
		return nil, err
	}
	p := &partition{key: key, path: path, w: w}
	c.parts = slices.Insert(c.parts, i, p)
	return p, nil
}

// close finalizes every open file and returns their paths in partition
// order.
func (c *converter) close() ([]string, error) {
	paths := make([]string, 0, len(c.parts))
	for _, p := range c.parts {
		if err := p.w.Close(); err != nil {
			c.abort()
			return nil, err
		}
		paths = append(paths, p.path)
	}
	return paths, nil
}

// abort discards every file this conversion created, finished or not.
func (c *converter) abort() {
	for _, p := range c.parts {
		_ = p.w.Abort()
		_ = os.Remove(p.path)
	}
	c.parts = nil
}

// recordOf converts one source row into a store.Record, plus the bid and
// ask runs of a snapshot row's levels. It narrows the source's 32-bit
// columns into store.Record's own widths and rejects a value that does
// not fit, rather than truncating it. See docs/convert.md.
func recordOf(row SourceRow) (store.Record, []store.Level, []store.Level, error) {
	if row.VenueID > math.MaxUint16 {
		return store.Record{}, nil, nil, ErrVenueIDRange
	}
	if row.RecordType > uint32(store.RecordTypeSnapshotPointer) {
		return store.Record{}, nil, nil, ErrRecordTypeRange
	}
	if row.SideFlags > uint32(store.SideAsk) {
		return store.Record{}, nil, nil, ErrSideFlagsRange
	}

	rec := store.Record{
		ExchangeTs:     row.ExchangeTs,
		SequenceNumber: row.SequenceNumber,
		InstrumentID:   row.InstrumentID,
		VenueID:        uint16(row.VenueID),
		RecordType:     uint8(row.RecordType),
	}
	if rec.RecordType != store.RecordTypeSnapshotPointer {
		if len(row.Levels) != 0 {
			return store.Record{}, nil, nil, ErrLevelsOnNonSnap
		}
		rec.SideFlags = uint8(row.SideFlags)
		rec.Price = row.Price
		rec.Size = row.Size
		return rec, nil, nil, nil
	}

	// A snapshot pointer carries no price, size or side of its own; those
	// live in its levels. store.Writer rejects them too, but rejecting
	// here names the source row that set them.
	if row.Price != 0 || row.Size != 0 || row.SideFlags != 0 {
		return store.Record{}, nil, nil, ErrSnapshotFieldSet
	}
	bids, asks, err := splitLevels(row.Levels)
	if err != nil {
		return store.Record{}, nil, nil, err
	}
	return rec, bids, asks, nil
}

// splitLevels sorts a snapshot row's levels into a bid run and an ask
// run, each in the best-first order internal/book uses: bids
// highest-price first, asks lowest-price first. Normalizing here is what
// makes the blob's bytes a function of the level set alone, not of the
// order the source happened to list them in.
func splitLevels(levels []SourceLevel) ([]store.Level, []store.Level, error) {
	var bids, asks []store.Level
	for _, l := range levels {
		switch uint8(l.Side) {
		case store.SideBid:
			bids = append(bids, store.Level{Price: l.Price, Size: l.Size})
		case store.SideAsk:
			asks = append(asks, store.Level{Price: l.Price, Size: l.Size})
		default:
			return nil, nil, ErrLevelSide
		}
	}
	sort.SliceStable(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.SliceStable(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return bids, asks, nil
}

// CheckSourceFile validates path against the canonical source schema and
// returns how many rows it holds. It reads the file's footer only, so a
// schema fault costs nothing close to a whole conversion.
func CheckSourceFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return 0, err
	}
	if err := CheckSourceSchema(pf.Schema()); err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}
