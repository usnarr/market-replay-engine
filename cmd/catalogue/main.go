// Command catalogue maintains the run catalogue and benchmark history
// in SQLite. It runs offline, after a replay has already finished, and
// is the only program in this repository that links a SQLite driver. It
// reads run manifests as files and never talks to replayd. See
// docs/no-database.md.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	dbPath := flag.String("db", "", "SQLite catalogue file, created if it does not exist")
	manifests := flag.String("manifests", "", "directory of replayd run-manifest JSON files to ingest")
	bench := flag.String("bench", "", "go test -bench -benchmem result file to ingest")
	commit := flag.String("commit", "", "git commit the -bench results were measured at")
	flag.Parse()

	if err := run(*dbPath, *manifests, *bench, *commit); err != nil {
		fmt.Fprintln(os.Stderr, "catalogue:", err)
		os.Exit(1)
	}
}

func run(dbPath, manifests, bench, commit string) error {
	if dbPath == "" {
		return fmt.Errorf("-db is required")
	}
	if bench != "" && commit == "" {
		return fmt.Errorf("-commit is required with -bench: a measurement with no commit cannot be compared against another")
	}
	db, err := Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if manifests != "" {
		runs, err := IngestManifests(db, manifests)
		if err != nil {
			return err
		}
		fmt.Printf("ingested %d run manifests\n", len(runs))
	}
	if bench != "" {
		measurements, err := IngestBenchmarks(db, bench, commit)
		if err != nil {
			return err
		}
		fmt.Printf("ingested %d benchmark measurements\n", len(measurements))
	}
	return nil
}
