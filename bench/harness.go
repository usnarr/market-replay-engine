// Package bench assembles the whole in-process replay stack over a
// dataset directory and reports what it delivered: one store.Reader per
// file, one merge.Cursor per venue, a merge.Merger, and a fanout.Ring
// with a configured subscriber mix. It is the load harness behind
// BENCHMARKS.md's end-to-end replay rate.
//
// Nothing here imports testing. The harness is the thing being measured,
// so it stays a plain library that a benchmark, a test, or a later load
// tool can each drive; bench/harness_test.go is what builds datasets with
// internal/synth and calls it.
//
// The harness is on the ordered path (see cmd/lint-determinism/scope.go):
// it picks the order venues enter the merge tree and the order
// subscribers register, and both are output-affecting for the stream it
// measures. Every order it picks comes from file content, never from a
// directory listing, a file name, or goroutine start order.
package bench

import (
	"cmp"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"replay/internal/clock"
	"replay/internal/fanout"
	"replay/internal/merge"
	"replay/internal/store"
)

// ErrNoVenues means the dataset directory held no venue file at all.
var ErrNoVenues = errors.New("bench: dataset directory holds no venue files")

// Config is one replay run. Every field is named explicitly; nothing here
// falls back to a value that merely happens to work.
type Config struct {
	// Dir is the dataset directory. Every .bin file directly inside it is
	// opened and grouped into a venue partition by the venue id in its own
	// header, never by its name.
	Dir string

	// Workers is the merge's decode worker count. Zero decodes inline on
	// the calling goroutine and starts no reader goroutine at all. The
	// count changes decode concurrency only: it never changes the merged
	// stream.
	Workers int

	// Capacity is the ring's slot count: a power of two, at least two.
	Capacity int

	// MaxBlobBytes is the longest snapshot payload the ring carries.
	MaxBlobBytes int

	// BlockSubscribers and DropSubscribers are how many subscribers of
	// each backpressure mode join before the first record is emitted.
	// Both counts are stated separately because the mode itself is
	// explicit at subscribe time; this harness has no mixed default to
	// pick from.
	BlockSubscribers int
	DropSubscribers  int

	// Speed paces the replay through a fanout.Pacer. The zero Speed is
	// the unpaced case: the stack runs as fast as it can, which is what
	// a throughput measurement wants.
	Speed fanout.Speed

	// Clock drives the pacer and the ring's own barrier accounting. It
	// defaults to clock.RealClock{} when nil, matching fanout.Config's
	// rule for the same field.
	Clock clock.Clock

	// Hash makes every subscriber hash what it receives with the
	// canonical projection, so a caller can assert a Block subscriber
	// received the merged stream exactly. It costs one hash per record
	// per subscriber, so a throughput run leaves it off.
	Hash bool
}

// SubscriberResult is one subscriber's share of a replay.
type SubscriberResult struct {
	// Mode is the backpressure mode this subscriber registered with.
	Mode fanout.BackpressureMode

	// Received is how many deliveries it took.
	Received uint64

	// Gaps is how many gap reports it was handed, and Missed is how many
	// records those gaps account for. Both are zero for a Block
	// subscriber, which is the promise Block makes.
	Gaps   uint64
	Missed uint64

	// Hash is the canonical digest of every record it received, in hex,
	// or empty when Config.Hash was off.
	Hash string
}

// Result is what one replay delivered.
type Result struct {
	// Venues is how many venue partitions the dataset held.
	Venues int

	// Records is how many records those partitions hold in total, read
	// from the file headers rather than counted during the replay, so a
	// caller can check the replay against it.
	Records int

	// Emitted is how many records the emit loop wrote into the ring.
	Emitted uint64

	// Block and Drop hold one result per subscriber, in registration
	// order.
	Block []SubscriberResult
	Drop  []SubscriberResult

	// PacingSlipNanos is how long the writer spent parked at the Block
	// barrier. See fanout.Ring.PacingSlipNanos.
	PacingSlipNanos int64
}

// Run replays cfg.Dir through the whole in-process stack once. It drives
// the emit loop on the calling goroutine and gives every subscriber its
// own, exactly as a real replay does: a Block subscriber only makes
// progress while something reads it, so the emit loop and the subscribers
// have to run at the same time.
//
// Run closes every file it opened before returning, on the error paths as
// well.
func Run(cfg Config) (Result, error) {
	parts, err := openPartitions(cfg.Dir)
	if err != nil {
		return Result{}, err
	}

	cursors := make([]*merge.Cursor, len(parts))
	records := 0
	for i, p := range parts {
		cursors[i] = p.cursor
		records += p.records
	}

	clk := cfg.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}

	ring, err := fanout.NewRing(fanout.Config{
		Capacity:     cfg.Capacity,
		MaxBlobBytes: cfg.MaxBlobBytes,
		Clock:        clk,
	})
	if err != nil {
		return Result{}, closeCursors(cursors, err)
	}

	// Block subscribers register first, then Drop, each group in index
	// order. The registration order fixes the Block barrier's own slice
	// order, so a run is reproducible rather than depending on which
	// goroutine got to Subscribe first.
	blockSubs := make([]*fanout.Subscriber, cfg.BlockSubscribers)
	for i := range blockSubs {
		s, err := ring.Subscribe(fanout.ModeBlock, fanout.Beginning())
		if err != nil {
			return Result{}, closeCursors(cursors, err)
		}
		blockSubs[i] = s
	}
	dropSubs := make([]*fanout.Subscriber, cfg.DropSubscribers)
	for i := range dropSubs {
		s, err := ring.Subscribe(fanout.ModeDrop, fanout.Beginning())
		if err != nil {
			return Result{}, closeCursors(cursors, err)
		}
		dropSubs[i] = s
	}

	var merger *merge.Merger
	if cfg.Workers == 0 {
		merger, err = merge.NewMerger(cursors)
	} else {
		merger, err = merge.NewConcurrentMerger(cursors, cfg.Workers)
	}
	if err != nil {
		return Result{}, closeCursors(cursors, err)
	}

	res := Result{
		Venues:  len(parts),
		Records: records,
		Block:   make([]SubscriberResult, len(blockSubs)),
		Drop:    make([]SubscriberResult, len(dropSubs)),
	}
	blockErrs := make([]error, len(blockSubs))
	dropErrs := make([]error, len(dropSubs))

	var wg sync.WaitGroup
	for i, s := range blockSubs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res.Block[i], blockErrs[i] = drain(s, cfg.Hash)
		}()
	}
	for i, s := range dropSubs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res.Drop[i], dropErrs[i] = drain(s, cfg.Hash)
		}()
	}

	var runErr error
	if cfg.Speed.Num() == 0 {
		runErr = ring.Run(merger)
	} else {
		runErr = ring.RunPaced(merger, fanout.NewPacer(clk, cfg.Speed))
	}
	if runErr != nil {
		// An aborted run never calls SetEnd, so every subscriber would
		// otherwise wait for a record that is never coming. Cancel
		// releases them. The errors they then report are consequences of
		// runErr, which is why runErr is the one Run returns.
		for _, s := range blockSubs {
			s.Cancel()
		}
		for _, s := range dropSubs {
			s.Cancel()
		}
	}
	wg.Wait()

	res.Emitted = ring.WriteIndex()
	res.PacingSlipNanos = ring.PacingSlipNanos()

	if err := merger.Close(); err != nil && runErr == nil {
		runErr = err
	}
	if runErr != nil {
		return Result{}, runErr
	}
	for _, err := range slices.Concat(blockErrs, dropErrs) {
		if err != nil {
			return Result{}, err
		}
	}
	return res, nil
}

// drain reads s to the end of its stream, counting what it received and
// what it was told it missed.
func drain(s *fanout.Subscriber, hash bool) (SubscriberResult, error) {
	res := SubscriberResult{Mode: s.Mode()}

	var hasher *store.CanonicalHasher
	if hash {
		hasher = store.NewCanonicalHasher(store.CanonicalCRC32C)
	}
	for {
		d, ok, err := s.Next()
		if err != nil {
			return res, err
		}
		if !ok {
			break
		}
		if d.HasGap {
			res.Gaps++
			res.Missed += d.Gap.Count
		}
		if hasher != nil {
			hasher.Write(d.Record, d.Blob)
		}
		res.Received++
	}
	if hasher != nil {
		res.Hash = hex.EncodeToString(hasher.Sum(nil))
	}
	return res, nil
}

// partition is one venue's cursor and the record count of the files
// behind it.
type partition struct {
	cursor  *merge.Cursor
	records int
}

// datasetFile is one opened file and the fields that place it in its
// venue's read order.
type datasetFile struct {
	reader   *store.Reader
	path     string
	venueID  uint16
	empty    bool
	firstTs  int64
	firstSeq uint64
}

// openPartitions opens every .bin file directly inside dir and groups
// them into one cursor per venue.
//
// Both orders come from the files' own content: partitions go in venue-id
// order, and a venue's files in first-record-key order. A name-ordered
// walk would put "day10" before "day2" and hand merge.NewCursor a
// partition that is not in ascending time order, which is what that
// constructor requires of its caller.
func openPartitions(dir string) ([]partition, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]datasetFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".bin" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		r, err := store.Open(path)
		if err != nil {
			return nil, closeFiles(files, err)
		}

		f := datasetFile{reader: r, path: path, venueID: r.VenueID(), empty: r.Len() == 0}
		if !f.empty {
			rec := r.RecordAt(0)
			f.firstTs, f.firstSeq = rec.ExchangeTs, rec.SequenceNumber
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		return nil, ErrNoVenues
	}
	slices.SortFunc(files, compareDatasetFile)

	var parts []partition
	for i := 0; i < len(files); {
		venue := files[i].venueID
		j := i
		for j < len(files) && files[j].venueID == venue {
			j++
		}

		readers := make([]*store.Reader, 0, j-i)
		records := 0
		for _, f := range files[i:j] {
			readers = append(readers, f.reader)
			records += f.reader.Len()
		}
		c, err := merge.NewCursor(venue, readers)
		if err != nil {
			return nil, closeFiles(files, err)
		}
		parts = append(parts, partition{cursor: c, records: records})
		i = j
	}
	return parts, nil
}

// compareDatasetFile orders two files by venue, then by their first
// record's key, then by path. An empty file carries no key to sort by, so
// it goes last within its venue, where it is inert: a cursor walks
// straight past it. The path is the final term only to separate two files
// this comparison cannot otherwise tell apart.
func compareDatasetFile(a, b datasetFile) int {
	if c := cmp.Compare(a.venueID, b.venueID); c != 0 {
		return c
	}
	if a.empty != b.empty {
		if a.empty {
			return 1
		}
		return -1
	}
	if c := cmp.Compare(a.firstTs, b.firstTs); c != 0 {
		return c
	}
	if c := cmp.Compare(a.firstSeq, b.firstSeq); c != 0 {
		return c
	}
	return strings.Compare(a.path, b.path)
}

// closeFiles closes every reader and returns cause, or the first close
// error when cause is nil.
func closeFiles(files []datasetFile, cause error) error {
	for _, f := range files {
		if err := f.reader.Close(); err != nil && cause == nil {
			cause = err
		}
	}
	return cause
}

// closeCursors closes every cursor and returns cause, or the first close
// error when cause is nil. A cursor closes the files behind it, and both
// it and store.Reader are idempotent, so this is safe on a path where a
// failed constructor has already closed some of them.
func closeCursors(cursors []*merge.Cursor, cause error) error {
	for _, c := range cursors {
		if err := c.Close(); err != nil && cause == nil {
			cause = err
		}
	}
	return cause
}
