package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Benchmark measurement failures. A result file that trips one is
// rejected whole rather than stored with a zero standing in for a number
// nothing measured.
var (
	ErrMissingMetric = errors.New("a measurement without ns/op, B/op and allocs/op")
	ErrBadValue      = errors.New("a measurement whose value is not a number")
)

// LineError names the line of a benchmark result file that could not be
// parsed, and carries the line itself.
type LineError struct {
	// Line is the line's position in the file, counting from one.
	Line int
	Text string
	Err  error
}

func (e *LineError) Error() string {
	return fmt.Sprintf("catalogue: line %d: %s: %q", e.Line, e.Err, e.Text)
}

func (e *LineError) Unwrap() error { return e.Err }

// Benchmark is one row of the benchmarks table.
type Benchmark struct {
	Name       string
	GitCommit  string
	Sample     int
	Iterations int64

	NsPerOp     float64
	BytesPerOp  int64
	AllocsPerOp int64

	RecordedUnixNano int64
}

// gomaxprocsSuffix is the -N a benchmark name carries. It is stripped,
// the way benchstat strips it: the parallelism a measurement ran at is
// not something this schema records, and leaving it in the name would
// split one benchmark's history in two the first time a runner's core
// count changed.
var gomaxprocsSuffix = regexp.MustCompile(`-\d+$`)

// ParseBenchmarks reads `go test -bench -benchmem` output and returns
// every measurement it holds, in file order. Sample counts each
// measurement's position in the file, so a -count=6 run produces six
// distinguishable rows for one benchmark.
//
// A line that is not a measurement is skipped: the output carries goos,
// goarch, pkg, PASS and ok lines that say nothing about a benchmark. A
// line that is a measurement but is missing a metric is an error, not a
// skip, because a result file produced without -benchmem cannot answer
// the one question this history exists for.
func ParseBenchmarks(r io.Reader, gitCommit string, recordedUnixNano int64) ([]Benchmark, error) {
	var out []Benchmark
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text := sc.Text()
		fields := strings.Fields(text)
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		iterations, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}

		b := Benchmark{
			Name:             gomaxprocsSuffix.ReplaceAllString(fields[0], ""),
			GitCommit:        gitCommit,
			Sample:           len(out),
			Iterations:       iterations,
			RecordedUnixNano: recordedUnixNano,
			NsPerOp:          -1,
			BytesPerOp:       -1,
			AllocsPerOp:      -1,
		}
		for i := 2; i+1 < len(fields); i += 2 {
			value, unit := fields[i], fields[i+1]
			switch unit {
			case "ns/op":
				f, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return nil, &LineError{Line: line, Text: text, Err: ErrBadValue}
				}
				b.NsPerOp = f
			case "B/op":
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, &LineError{Line: line, Text: text, Err: ErrBadValue}
				}
				b.BytesPerOp = n
			case "allocs/op":
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, &LineError{Line: line, Text: text, Err: ErrBadValue}
				}
				b.AllocsPerOp = n
			}
		}
		if b.NsPerOp < 0 || b.BytesPerOp < 0 || b.AllocsPerOp < 0 {
			return nil, &LineError{Line: line, Text: text, Err: ErrMissingMetric}
		}
		out = append(out, b)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// IngestBenchmarks reads a benchmark result file and inserts every
// measurement it holds. The recorded timestamp is the file's own
// modification time, which is when `make bench` wrote it — this tool
// never reads the wall clock, for the reason the root CLAUDE.md gives.
func IngestBenchmarks(db *sql.DB, path, gitCommit string) ([]Benchmark, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	benchmarks, err := ParseBenchmarks(f, gitCommit, info.ModTime().UnixNano())
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, b := range benchmarks {
		_, err := tx.Exec(
			`INSERT INTO benchmarks (name, git_commit, sample, iterations, ns_per_op, bytes_per_op, allocs_per_op, recorded_unix_nano)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			b.Name, b.GitCommit, b.Sample, b.Iterations, b.NsPerOp, b.BytesPerOp, b.AllocsPerOp, b.RecordedUnixNano)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return benchmarks, nil
}
