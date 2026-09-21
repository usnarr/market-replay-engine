package main

import (
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
)

// The run manifest is how a finished run reaches cmd/catalogue. replayd
// writes a plain JSON file and stops there: it never imports
// cmd/catalogue and never links a SQLite driver, which is what keeps
// "SQLite is not touched during a replay" true by construction rather
// than by intent. See docs/no-database.md.

// Manifest is one finished run, as written to disk.
type Manifest struct {
	Dataset     []DatasetFile `json:"dataset"`
	Config      RunConfig     `json:"config"`
	Subscribers SubscriberMix `json:"subscribers"`
	Result      RunResult     `json:"result"`
	Timing      Timing        `json:"timing"`
}

// DatasetFile identifies one input file.
//
// Path and size, because that is the identity available today: there is
// no cmd/convert yet to stamp an artifact content hash into the file or
// to hand one over. 11-convert-and-catalogue.md replaces these two
// fields with that hash, which is the only identity that survives a
// file being moved or rebuilt.
type DatasetFile struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

// RunConfig is what the run was asked to do.
type RunConfig struct {
	// SpeedNum and SpeedDen are the exact rational the run was paced
	// at, reduced to lowest terms, never a float: a manifest holding a
	// float is not reproducible across architectures the way a num/den
	// pair is.
	SpeedNum int64 `json:"speed_num"`
	SpeedDen int64 `json:"speed_den"`

	Workers      int `json:"workers"`
	RingCapacity int `json:"ring_capacity"`
	MaxBlobBytes int `json:"max_blob_bytes"`

	// SeekExchangeTs is null when the run replayed every record. A
	// seek to timestamp zero is a real request, so the distinction
	// cannot be carried by the value alone.
	SeekExchangeTs       *int64 `json:"seek_exchange_ts"`
	WatchdogTimeoutNanos int64  `json:"watchdog_timeout_nanos"`
}

// SubscriberMix is how many subscribers of each kind attached over the
// run's life. Counts, never a list and never a map: a list's order is
// the order clients happened to connect in, and a map's serialization
// order is not defined at all. Either would make two runs of the same
// workload produce different manifest bytes.
type SubscriberMix struct {
	Block int `json:"block"`
	Drop  int `json:"drop"`

	StartBeginning  int `json:"start_beginning"`
	StartExchangeTs int `json:"start_exchange_ts"`
	StartEmitIndex  int `json:"start_emit_index"`
	StartLive       int `json:"start_live"`
}

// RunResult is what the run produced.
type RunResult struct {
	// EmitIndexAtEnd is the number of records the run emitted.
	EmitIndexAtEnd uint64 `json:"emit_index_at_end"`

	// PacingSlipNanos is the cumulative time the writer spent parked at
	// the Block barrier.
	PacingSlipNanos int64 `json:"pacing_slip_nanos"`

	// CanonicalHash is empty in this milestone, and the field is here
	// as the hook for it rather than as a promise. Nothing in the emit
	// loop hands the merged stream out: RunPaced drains the merger into
	// the ring itself, and the only way to see every record from
	// outside is a Block subscriber, which would throttle the whole run
	// to the speed of a hash nobody asked for. Filling this in needs
	// internal/fanout to offer the digest itself, as a run-level
	// option, so a run that does not want it pays nothing. The gRPC
	// integration test computes the same digest client-side today, so
	// the projection is already pinned; what is missing is only a way
	// for a real run to produce it for free.
	CanonicalHash string `json:"canonical_hash"`

	// Error is the run's own failure, if it had one, as text. A
	// manifest is written for a failed run too: "this run aborted
	// here" is exactly what a catalogue needs to record.
	Error string `json:"error,omitempty"`
}

// Timing is the run's wall-clock summary, read through
// clock.RealClock, the repository's one time.Now call site.
type Timing struct {
	StartedUnixNano int64 `json:"started_unix_nano"`
	EndedUnixNano   int64 `json:"ended_unix_nano"`
	ElapsedNanos    int64 `json:"elapsed_nanos"`
}

// datasetIdentity describes every configured file, sorted by absolute
// path so the same set of files always produces the same manifest
// bytes however the command line ordered them.
func datasetIdentity(files []string) ([]DatasetFile, error) {
	out := make([]DatasetFile, 0, len(files))
	for _, path := range files {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		out = append(out, DatasetFile{Path: filepath.ToSlash(abs), Bytes: info.Size()})
	}
	slices.SortFunc(out, func(a, b DatasetFile) int {
		return cmp.Compare(a.Path, b.Path)
	})
	return out, nil
}

// writeManifest writes m to path, indented, with a trailing newline.
func writeManifest(path string, m Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
