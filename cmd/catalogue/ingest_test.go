package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// manifestJSON is one run manifest exactly as cmd/replayd writes it,
// including the fields this module deliberately does not declare. Those
// have to survive being ignored.
const manifestJSON = `{
  "dataset": [
    {
      "path": "/data/venue-7-2024-01-01.bin",
      "hash": "1111111111111111111111111111111111111111111111111111111111111111"
    },
    {
      "path": "/data/venue-9-2024-01-01.bin",
      "hash": ""
    }
  ],
  "config": {
    "speed_num": 2,
    "speed_den": 1,
    "workers": 4,
    "ring_capacity": 8192,
    "max_blob_bytes": 4096,
    "seek_exchange_ts": null,
    "watchdog_timeout_nanos": 0
  },
  "subscribers": {
    "block": 1,
    "drop": 0,
    "start_beginning": 1,
    "start_exchange_ts": 0,
    "start_emit_index": 0,
    "start_live": 0
  },
  "result": {
    "emit_index_at_end": 7590,
    "pacing_slip_nanos": 0,
    "canonical_hash": "deadbeef"
  },
  "timing": {
    "started_unix_nano": 1700000000000000000,
    "ended_unix_nano": 1700000001500000000,
    "elapsed_nanos": 1500000000
  }
}
`

// benchOutput is `go test -bench -benchmem` output, with the header and
// trailer lines a real file carries and one benchmark measured twice.
const benchOutput = `goos: windows
goarch: amd64
pkg: replay/internal/merge
cpu: 12th Gen Intel(R) Core(TM) i7-12700H
BenchmarkMergeNext/4_venues-16      	 5000000	       234.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkMergeNext/4_venues-16      	 5100000	       231.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkRingPublish-16             	 9000000	       118.2 ns/op	      16 B/op	       1 allocs/op
PASS
ok  	replay/internal/merge	4.321s
`

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v, want nil", name, err)
	}
	return path
}

// readRuns reads the runs table back, ordered by run ID.
func readRuns(t *testing.T, db *sql.DB) []Run {
	t.Helper()
	rows, err := db.Query(`SELECT run_id, dataset_hash, config, canonical_hash, started_unix_nano, elapsed_nanos
		FROM runs ORDER BY run_id`)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}
	defer rows.Close()

	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.RunID, &r.DatasetHash, &r.Config, &r.CanonicalHash, &r.StartedUnixNano, &r.ElapsedNanos); err != nil {
			t.Fatalf("Scan() error = %v, want nil", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v, want nil", err)
	}
	return out
}

// readBenchmarks reads the benchmarks table back, in insertion order.
func readBenchmarks(t *testing.T, db *sql.DB) []Benchmark {
	t.Helper()
	rows, err := db.Query(`SELECT name, git_commit, sample, iterations, ns_per_op, bytes_per_op, allocs_per_op, recorded_unix_nano
		FROM benchmarks ORDER BY sample`)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}
	defer rows.Close()

	var out []Benchmark
	for rows.Next() {
		var b Benchmark
		if err := rows.Scan(&b.Name, &b.GitCommit, &b.Sample, &b.Iterations, &b.NsPerOp, &b.BytesPerOp, &b.AllocsPerOp, &b.RecordedUnixNano); err != nil {
			t.Fatalf("Scan() error = %v, want nil", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v, want nil", err)
	}
	return out
}

func TestIngestManifests(t *testing.T) {
	t.Run("a_manifest_round_trips_through_the_catalogue", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "run-2024-01-01.json", manifestJSON)
		writeFile(t, dir, "notes.txt", "not a manifest")
		db := openTestDB(t)

		ingested, err := IngestManifests(db, dir)

		if err != nil {
			t.Fatalf("IngestManifests() error = %v, want nil", err)
		}
		want := []Run{{
			RunID: "run-2024-01-01",
			DatasetHash: DatasetHash([]ManifestDatasetFile{
				{Path: "/data/venue-7-2024-01-01.bin", Hash: "1111111111111111111111111111111111111111111111111111111111111111"},
				{Path: "/data/venue-9-2024-01-01.bin"},
			}),
			CanonicalHash:   "deadbeef",
			StartedUnixNano: 1700000000000000000,
			ElapsedNanos:    1500000000,
		}}
		got := readRuns(t, db)
		if len(got) != 1 {
			t.Fatalf("readRuns() returned %d rows, want 1", len(got))
		}
		// The config column holds the manifest's own bytes, whitespace and
		// all, so it is compared separately from the derived fields.
		if !strings.Contains(got[0].Config, `"ring_capacity": 8192`) {
			t.Errorf("runs.config = %q, want the manifest's config object", got[0].Config)
		}
		want[0].Config = got[0].Config
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("readRuns() mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(want, ingested); diff != "" {
			t.Errorf("IngestManifests() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("the_dataset_hash_distinguishes_two_datasets", func(t *testing.T) {
		a := DatasetHash([]ManifestDatasetFile{{Path: "/data/a.bin", Hash: "aa"}})
		b := DatasetHash([]ManifestDatasetFile{{Path: "/data/a.bin", Hash: "bb"}})
		c := DatasetHash([]ManifestDatasetFile{{Path: "/data/b.bin"}, {Path: "/data/c.bin"}})
		d := DatasetHash([]ManifestDatasetFile{{Path: "/data/b.bin"}})

		if a == b {
			t.Error("two datasets whose content hashes differ hash the same")
		}
		if c == d {
			t.Error("two datasets whose file lists differ hash the same")
		}
	})

	t.Run("a_file_that_is_not_a_manifest", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "broken.json", "{not json")
		db := openTestDB(t)

		_, err := IngestManifests(db, dir)

		if err == nil {
			t.Fatal("IngestManifests() error = nil, want a decode failure")
		}
		if !strings.Contains(err.Error(), "broken.json") {
			t.Errorf("IngestManifests() error = %v, want it to name broken.json", err)
		}
	})

	t.Run("ingesting_the_same_manifest_twice", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "run-1.json", manifestJSON)
		db := openTestDB(t)
		if _, err := IngestManifests(db, dir); err != nil {
			t.Fatalf("IngestManifests() error = %v, want nil", err)
		}

		_, err := IngestManifests(db, dir)

		if err == nil {
			t.Error("IngestManifests() error = nil on a second ingestion, want a constraint failure")
		}
	})
}

func TestParseBenchmarks(t *testing.T) {
	t.Run("a_benchmem_result_file", func(t *testing.T) {
		got, err := ParseBenchmarks(strings.NewReader(benchOutput), "abc123", 1700000000000000000)

		if err != nil {
			t.Fatalf("ParseBenchmarks() error = %v, want nil", err)
		}
		want := []Benchmark{
			{Name: "BenchmarkMergeNext/4_venues", GitCommit: "abc123", Sample: 0, Iterations: 5000000, NsPerOp: 234.5, BytesPerOp: 0, AllocsPerOp: 0, RecordedUnixNano: 1700000000000000000},
			{Name: "BenchmarkMergeNext/4_venues", GitCommit: "abc123", Sample: 1, Iterations: 5100000, NsPerOp: 231.0, BytesPerOp: 0, AllocsPerOp: 0, RecordedUnixNano: 1700000000000000000},
			{Name: "BenchmarkRingPublish", GitCommit: "abc123", Sample: 2, Iterations: 9000000, NsPerOp: 118.2, BytesPerOp: 16, AllocsPerOp: 1, RecordedUnixNano: 1700000000000000000},
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("ParseBenchmarks() mismatch (-want +got):\n%s", diff)
		}
	})

	tests := []struct {
		name  string
		input string
		want  error
	}{
		{
			name:  "a_measurement_taken_without_benchmem",
			input: "BenchmarkMergeNext-16    \t 5000000\t       234.5 ns/op\n",
			want:  ErrMissingMetric,
		},
		{
			name:  "a_measurement_with_no_ns_per_op",
			input: "BenchmarkMergeNext-16    \t 5000000\t       0 B/op\t       0 allocs/op\n",
			want:  ErrMissingMetric,
		},
		{
			name:  "a_measurement_whose_value_is_not_a_number",
			input: "BenchmarkMergeNext-16    \t 5000000\t       many ns/op\t       0 B/op\t       0 allocs/op\n",
			want:  ErrBadValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseBenchmarks(strings.NewReader(tt.input), "abc123", 1)

			if !errors.Is(err, tt.want) {
				t.Fatalf("ParseBenchmarks() error = %v, want %v", err, tt.want)
			}
			var le *LineError
			if !errors.As(err, &le) || le.Line != 1 {
				t.Errorf("ParseBenchmarks() error = %v, want a *LineError naming line 1", err)
			}
		})
	}
}

func TestIngestBenchmarks(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "latest.txt", benchOutput)
	db := openTestDB(t)

	ingested, err := IngestBenchmarks(db, path, "abc123")

	if err != nil {
		t.Fatalf("IngestBenchmarks() error = %v, want nil", err)
	}
	got := readBenchmarks(t, db)
	if diff := cmp.Diff(ingested, got); diff != "" {
		t.Errorf("the rows read back differ from the rows ingested (-want +got):\n%s", diff)
	}
	if len(got) != 3 {
		t.Fatalf("benchmarks holds %d rows, want 3", len(got))
	}
	// The timestamp comes from the file's own modification time; this
	// tool never reads the wall clock.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v, want nil", err)
	}
	if got[0].RecordedUnixNano != info.ModTime().UnixNano() {
		t.Errorf("recorded_unix_nano = %d, want the file's mtime %d", got[0].RecordedUnixNano, info.ModTime().UnixNano())
	}
}
