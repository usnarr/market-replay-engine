package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Manifest mirrors the JSON cmd/replayd writes. It is a copy of that
// struct's on-disk shape, not an import of it: cmd/catalogue must not
// depend on the replay module, because that dependency is what would
// make "no code path from replayd's process to SQLite" a convention
// rather than a fact. The duplication is the mechanism. See
// docs/no-database.md.
//
// Only the fields the catalogue stores are declared. A manifest field
// this struct omits is ignored, which is what lets replayd add one
// without breaking ingestion.
type Manifest struct {
	Dataset []ManifestDatasetFile `json:"dataset"`
	Config  json.RawMessage       `json:"config"`
	Result  ManifestResult        `json:"result"`
	Timing  ManifestTiming        `json:"timing"`
}

// ManifestDatasetFile is one input file's identity.
type ManifestDatasetFile struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

// ManifestResult is what the run produced.
type ManifestResult struct {
	CanonicalHash string `json:"canonical_hash"`
}

// ManifestTiming is the run's wall-clock summary.
type ManifestTiming struct {
	StartedUnixNano int64 `json:"started_unix_nano"`
	ElapsedNanos    int64 `json:"elapsed_nanos"`
}

// Run is one row of the runs table.
type Run struct {
	RunID           string
	DatasetHash     string
	Config          string
	CanonicalHash   string
	StartedUnixNano int64
	ElapsedNanos    int64
}

// IngestManifest reads one run manifest and inserts it, returning the
// row it wrote. The run ID is the manifest file's name without its
// extension: a manifest carries no ID of its own, and the operator
// already chose that name.
func IngestManifest(db *sql.DB, path string) (Run, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Run{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Run{}, fmt.Errorf("catalogue: %s is not a run manifest: %w", filepath.Base(path), err)
	}

	config := string(m.Config)
	if config == "" {
		config = "{}"
	}
	run := Run{
		RunID:           strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		DatasetHash:     DatasetHash(m.Dataset),
		Config:          config,
		CanonicalHash:   m.Result.CanonicalHash,
		StartedUnixNano: m.Timing.StartedUnixNano,
		ElapsedNanos:    m.Timing.ElapsedNanos,
	}
	_, err = db.Exec(
		`INSERT INTO runs (run_id, dataset_hash, config, canonical_hash, started_unix_nano, elapsed_nanos)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		run.RunID, run.DatasetHash, run.Config, run.CanonicalHash, run.StartedUnixNano, run.ElapsedNanos)
	if err != nil {
		return Run{}, err
	}
	return run, nil
}

// IngestManifests ingests every .json file in dir, in sorted name order
// so that two ingestions of the same directory do the same work in the
// same order.
func IngestManifests(db *sql.DB, dir string) ([]Run, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)

	runs := make([]Run, 0, len(names))
	for _, name := range names {
		run, err := IngestManifest(db, filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// DatasetHash reduces a manifest's dataset to one value: the SHA-256
// over each file's path and content hash, in manifest order. The
// manifest is already sorted by path, so the digest is a function of the
// dataset alone. The path is included as well as the hash, so a dataset
// whose files carry no converter-stamped hash still has an identity.
func DatasetHash(files []ManifestDatasetFile) string {
	h := sha256.New()
	for _, f := range files {
		fmt.Fprintf(h, "%s\t%s\n", f.Path, f.Hash)
	}
	return hex.EncodeToString(h.Sum(nil))
}
